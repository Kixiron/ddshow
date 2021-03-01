// mod channel_stats;
mod filter_map;
mod inspect;
mod min_max;
mod operator_stats;
mod replay_with_shutdown;
mod subgraphs;
mod util;

pub use filter_map::FilterMap;
pub use inspect::InspectExt;
pub use min_max::{DiffDuration, Max, Min};
pub use operator_stats::OperatorStats;
pub(crate) use replay_with_shutdown::make_streams;
pub use util::{CrossbeamExtractor, CrossbeamPusher, OperatorExt};

use crate::args::Args;
use abomonation_derive::Abomonation;
use anyhow::Result;
use crossbeam_channel::Sender;
use differential_dataflow::{
    collection::AsCollection,
    difference::{Abelian, Monoid, Semigroup},
    lattice::Lattice,
    operators::{Consolidate, Join, Threshold},
    Collection, ExchangeData,
};
use operator_stats::operator_stats;
use replay_with_shutdown::ReplayWithShutdown;
use std::{
    fmt::{self, Debug},
    net::TcpStream,
    ops::{Deref, Mul},
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};
use subgraphs::rewire_channels;
use timely::{
    dataflow::{
        operators::{
            capture::{Capture, Event, EventReader},
            Filter, Map,
        },
        Scope, Stream,
    },
    logging::{ChannelsEvent, OperatesEvent, TimelyEvent, WorkerIdentifier},
};

type TimelyLogBundle = (Duration, WorkerIdentifier, TimelyEvent);
type Diff = isize;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct Address {
    pub addr: Vec<usize>,
}

impl Address {
    pub const fn new(addr: Vec<usize>) -> Self {
        Self { addr }
    }
}

impl Deref for Address {
    type Target = Vec<usize>;

    fn deref(&self) -> &Self::Target {
        &self.addr
    }
}

impl Debug for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.addr.iter()).finish()
    }
}

pub type NodeBundle = ((Address, OperatesEvent), Duration, Diff);
pub type EdgeBundle = (
    (
        OperatesEvent,
        Channel,
        OperatesEvent,
        // Option<ChannelMessageStats>,
    ),
    Duration,
    Diff,
);
pub type SubgraphBundle = ((Address, OperatesEvent), Duration, Diff);
pub type StatsBundle = ((usize, OperatorStats), Duration, Diff);

#[derive(Debug, Clone)]
pub struct DataflowSenders {
    pub node_sender: Sender<Event<Duration, NodeBundle>>,
    pub edge_sender: Sender<Event<Duration, EdgeBundle>>,
    pub subgraph_sender: Sender<Event<Duration, SubgraphBundle>>,
    pub stats_sender: Sender<Event<Duration, StatsBundle>>,
}

pub fn dataflow<S>(
    scope: &mut S,
    _args: &Args,
    timely_traces: Vec<EventReader<Duration, TimelyLogBundle, TcpStream>>,
    replay_shutdown: Arc<AtomicBool>,
    senders: DataflowSenders,
) -> Result<()>
where
    S: Scope<Timestamp = Duration>,
{
    // The timely log stream filtered down to worker 0's events
    let timely_stream = timely_traces
        .replay_with_shutdown_into_named("Timely Replay", scope, replay_shutdown)
        // This is a bit of a cop-out that should work for *most*
        // dataflow programs. In most programs, every worker has the
        // exact same dataflows created, which means that ignoring
        // every worker other than worker 0 works perfectly fine.
        // However, if a program that has unique dataflows per-worker,
        // we'll need to rework how some things are done to distinguish
        // between workers on an end-to-end basis. This also isn't
        // accurate on a profiling basis, as it only shows the stats
        // of worker 0 and ignores all others. Ideally, we'll be able
        // to show total and per-worker times on each operator, since
        // they should vary from worker to worker and this workaround
        // only shows 1/nth of the story
        .filter(|&(_, worker_id, _)| worker_id == 0);

    let operators = operator_creations(&timely_stream);
    let (leaves, subgraphs) = sift_leaves_and_scopes(scope, &operators);

    operators
        .map(|operator| (Address::new(operator.addr.clone()), operator))
        .semijoin(&leaves)
        .consolidate()
        .inner
        .capture_into(CrossbeamPusher::new(senders.node_sender));

    operators
        .map(|operator| (Address::new(operator.addr.clone()), operator))
        .semijoin(&subgraphs)
        .consolidate()
        .inner
        .capture_into(CrossbeamPusher::new(senders.subgraph_sender));

    let channel_events = channel_creations(&timely_stream);
    let channels = rewire_channels(scope, &channel_events, &subgraphs);

    let edges = attach_operators(scope, &operators, &channels, &leaves);

    // .map(|(source, channel, target)| {
    //     (
    //         (source.id, channel.channel_id(), target.id),
    //         (source, channel, target),
    //     )
    // })
    //
    // let messages = coagulate_channel_messages(scope, &timely_stream);
    //
    // let edge_bundles = edges
    //     .join_map(&messages, |_, (source, channel, target), channel_stats| {
    //         (
    //             source.clone(),
    //             channel.clone(),
    //             target.clone(),
    //             Some(channel_stats.clone()),
    //         )
    //     })
    //     .join_map(edges.antijoin(&messages.map(|(key, _)| key)), |_, );

    edges
        .consolidate()
        .inner
        .capture_into(CrossbeamPusher::new(senders.edge_sender));

    let operator_stats = operator_stats(scope, &timely_stream);
    operator_stats
        .consolidate()
        .inner
        .capture_into(CrossbeamPusher::new(senders.stats_sender));

    // TODO: Fix this
    // // If saving logs is enabled, write all log messages to the `save_logs` file
    // if let Some(save_logs) = args.save_logs.as_ref() {
    //     if scope.index() == 0 {
    //         let file = File::create(save_logs).context("failed to open `--save-logs` file")?;
    //         log_stream.capture_into(EventWriter::new(file));
    //     }
    // }

    Ok(())
}

fn operator_creations<S>(
    log_stream: &Stream<S, TimelyLogBundle>,
) -> Collection<S, OperatesEvent, Diff>
where
    S: Scope<Timestamp = Duration>,
{
    log_stream
        .flat_map(|(time, _worker, event)| {
            if let TimelyEvent::Operates(event) = event {
                Some((event, time, 1))
            } else {
                None
            }
        })
        .as_collection()
}

fn channel_creations<S>(
    log_stream: &Stream<S, TimelyLogBundle>,
) -> Collection<S, ChannelsEvent, Diff>
where
    S: Scope<Timestamp = Duration>,
{
    log_stream
        .flat_map(|(time, _worker, event)| {
            if let TimelyEvent::Channels(event) = event {
                Some((event, time, 1))
            } else {
                None
            }
        })
        .as_collection()
}

fn sift_leaves_and_scopes<S, D>(
    scope: &mut S,
    operators: &Collection<S, OperatesEvent, D>,
) -> (Collection<S, Address, D>, Collection<S, Address, D>)
where
    S: Scope,
    S::Timestamp: Lattice,
    D: Semigroup + Monoid + Abelian + ExchangeData + Mul<isize, Output = D>,
{
    scope.region_named("Sift Leaves and Scopes", |region| {
        let operators = operators
            .enter_region(region)
            .map(|operator| (Address::new(operator.addr), ()));

        // The addresses of potential scopes, excluding leaf operators
        let potential_scopes = operators
            .map(|(mut addr, ())| {
                addr.addr.pop();
                addr
            })
            .distinct();

        // Leaf operators
        let leaf_operators = operators
            .antijoin(&potential_scopes)
            .map(|(addr, _)| addr)
            .leave_region();

        // Only retain subgraphs that are observed within the logs
        let observed_subgraphs = operators
            .semijoin(&potential_scopes)
            .map(|(addr, ())| addr)
            .leave_region();

        (leaf_operators, observed_subgraphs)
    })
}

fn attach_operators<S, D>(
    scope: &mut S,
    operators: &Collection<S, OperatesEvent, D>,
    channels: &Collection<S, Channel, D>,
    leaves: &Collection<S, Address, D>,
) -> Collection<S, (OperatesEvent, Channel, OperatesEvent), D>
where
    S: Scope,
    S::Timestamp: Lattice,
    D: Semigroup + ExchangeData + Mul<Output = D>,
{
    // TODO: Make `Graph` nested so that subgraphs contain a `Vec<Graph>` of all children
    scope.region_named("Attach Operators to Channels", |region| {
        let (operators, channels, leaves) = (
            operators.enter_region(region),
            channels.enter_region(region),
            leaves.enter_region(region),
        );

        let operators_by_address =
            operators.map(|operator| (Address::new(operator.addr.clone()), operator));

        operators_by_address
            .semijoin(&leaves)
            .join_map(
                &channels.map(|channel| (channel.source_addr(), channel)),
                |_src_addr, src_operator, channel| {
                    (
                        channel.target_addr(),
                        (src_operator.clone(), channel.clone()),
                    )
                },
            )
            .join_map(
                &operators_by_address,
                |_target_addr, (src_operator, channel), target_operator| {
                    (
                        src_operator.clone(),
                        channel.clone(),
                        target_operator.clone(),
                    )
                },
            )
            .leave_region()
    })
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub enum Channel {
    ScopeCrossing {
        channel_id: usize,
        source_addr: Address,
        target_addr: Address,
    },

    Normal {
        channel_id: usize,
        source_addr: Address,
        target_addr: Address,
    },
}

impl Channel {
    pub const fn channel_id(&self) -> usize {
        match *self {
            Self::ScopeCrossing { channel_id, .. } | Self::Normal { channel_id, .. } => channel_id,
        }
    }

    pub fn source_addr(&self) -> Address {
        match self {
            Self::ScopeCrossing { source_addr, .. } | Self::Normal { source_addr, .. } => {
                source_addr.to_owned()
            }
        }
    }

    pub fn target_addr(&self) -> Address {
        match self {
            Self::ScopeCrossing { target_addr, .. } | Self::Normal { target_addr, .. } => {
                target_addr.to_owned()
            }
        }
    }
}
