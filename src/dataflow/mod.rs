// mod channel_stats;
mod differential;
mod operator_stats;
pub mod operators;
mod reachability;
mod subgraphs;
mod summation;
mod worker_timeline;

pub use operator_stats::OperatorStats;
pub use worker_timeline::{TimelineEvent, WorkerTimelineEvent};

use crate::args::Args;
use abomonation_derive::Abomonation;
use anyhow::Result;
use crossbeam_channel::Sender;
use differential::arrangement_stats;
use differential_dataflow::{
    collection::AsCollection,
    difference::{Abelian, Monoid, Semigroup},
    lattice::Lattice,
    logging::DifferentialEvent,
    operators::{arrange::ArrangeByKey, Consolidate, Join, ThresholdTotal},
    Collection, ExchangeData,
};
use operator_stats::operator_stats;
use operators::{CrossbeamPusher, FilterMap, InspectExt, Multiply, ReplayWithShutdown};
use std::{
    fmt::{self, Debug},
    net::TcpStream,
    ops::Deref,
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
    order::TotalOrder,
};
use worker_timeline::worker_timeline;

type TimelyLogBundle = (Duration, WorkerIdentifier, TimelyEvent);
type DifferentialLogBundle = (Duration, WorkerIdentifier, DifferentialEvent);
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
pub type TimelineBundle = (WorkerTimelineEvent, Duration, Diff);

#[derive(Debug, Clone)]
pub struct DataflowSenders {
    pub node_sender: Sender<Event<Duration, NodeBundle>>,
    pub edge_sender: Sender<Event<Duration, EdgeBundle>>,
    pub subgraph_sender: Sender<Event<Duration, SubgraphBundle>>,
    pub stats_sender: Sender<Event<Duration, StatsBundle>>,
    pub timeline_sender: Sender<Event<Duration, TimelineBundle>>,
}

pub fn dataflow<S>(
    scope: &mut S,
    _args: &Args,
    timely_traces: Vec<EventReader<Duration, TimelyLogBundle, TcpStream>>,
    differential_traces: Option<Vec<EventReader<Duration, DifferentialLogBundle, TcpStream>>>,
    replay_shutdown: Arc<AtomicBool>,
    senders: DataflowSenders,
) -> Result<()>
where
    S: Scope<Timestamp = Duration>,
{
    // The timely log stream filtered down to worker 0's events
    let raw_timely_stream = timely_traces
        .replay_with_shutdown_into_named("Timely Replay", scope, replay_shutdown.clone())
        .debug_inspect(|x| tracing::trace!("timely event: {:?}", x));

    let timely_stream = raw_timely_stream
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

    let raw_differential_stream = differential_traces.map(|traces| {
        traces
            .replay_with_shutdown_into_named("Differential Replay", scope, replay_shutdown)
            .debug_inspect(|x| tracing::trace!("differential dataflow event: {:?}", x))
    });

    let operators = operator_creations(&timely_stream);
    let operator_stats = operator_stats(scope, &timely_stream);
    let operator_names = operators
        .map(|operates| (operates.id, operates.name))
        .arrange_by_key();

    let operator_stats = raw_differential_stream
        .as_ref()
        .map(|stream| {
            let arrangement_stats = arrangement_stats(scope, stream)
                .filter_map(|((worker, operator), stats)| {
                    if worker == 0 {
                        Some((operator, stats))
                    } else {
                        None
                    }
                })
                .consolidate();

            let modified_stats = operator_stats.join_map(
                &arrangement_stats,
                |&operator, stats, arrangement_stats| {
                    let mut stats = stats.clone();
                    stats.arrangement_size = Some(arrangement_stats.clone());

                    (operator, stats)
                },
            );

            operator_stats
                .antijoin(&modified_stats.map(|(operator, _)| operator))
                .concat(&modified_stats)
        })
        .unwrap_or(operator_stats);

    let (leaves, subgraphs) = sift_leaves_and_scopes(scope, &operators);

    let channel_events = channel_creations(&timely_stream);
    let channels = rewire_channels(scope, &channel_events, &subgraphs);

    let edges = attach_operators(scope, &operators, &channels, &leaves);

    let worker_timeline = worker_timeline(
        scope,
        &raw_timely_stream,
        raw_differential_stream.as_ref(),
        &operator_names,
    );

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

    edges
        .consolidate()
        .inner
        .capture_into(CrossbeamPusher::new(senders.edge_sender));

    operator_stats
        .consolidate()
        .inner
        .capture_into(CrossbeamPusher::new(senders.stats_sender));

    worker_timeline
        .consolidate()
        .inner
        .capture_into(CrossbeamPusher::new(senders.timeline_sender));

    // TODO: Fix this
    // // If saving logs is enabled, write all log messages to the `save_logs` file
    // if let Some(save_logs) = args.save_logs.as_ref() {
    //     if scope.index() == 0 {
    //         let file = File::create(save_logs).context("failed to open `--save-logs` file")?;
    //         raw_timely_stream.capture_into(EventWriter::new(file));
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
    S::Timestamp: Lattice + TotalOrder,
    D: Semigroup + Monoid + Abelian + ExchangeData + Multiply<isize, Output = D>,
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
            .distinct_total();

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
    D: Semigroup + ExchangeData + Multiply<Output = D>,
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
