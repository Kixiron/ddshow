// mod channel_stats;
mod differential;
mod operator_stats;
pub mod operators;
#[cfg(feature = "timely-next")]
mod reachability;
mod subgraphs;
mod summation;
mod tests;
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
#[cfg(feature = "timely-next")]
use reachability::TrackerEvent;
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
            probe::Handle as ProbeHandle,
            Map, Probe,
        },
        Scope, Stream,
    },
    logging::{ChannelsEvent, OperatesEvent, TimelyEvent, WorkerIdentifier},
    order::TotalOrder,
};
use worker_timeline::worker_timeline;

// TODO: Newtype channel ids, operators ids, channel addrs and operator addrs and worker ids

type TimelyLogBundle = (Duration, WorkerIdentifier, TimelyEvent);
type DifferentialLogBundle = (Duration, WorkerIdentifier, DifferentialEvent);
#[cfg(feature = "timely-next")]
type ReachabilityLogBundle = (Duration, WorkerIdentifier, TrackerEvent);
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

pub type NodeBundle = (((WorkerIdentifier, Address), OperatesEvent), Duration, Diff);
pub type EdgeBundle = (
    (
        WorkerIdentifier,
        OperatesEvent,
        Channel,
        OperatesEvent,
        // Option<ChannelMessageStats>,
    ),
    Duration,
    Diff,
);
pub type SubgraphBundle = (((WorkerIdentifier, Address), OperatesEvent), Duration, Diff);
pub type StatsBundle = (((WorkerIdentifier, usize), OperatorStats), Duration, Diff);
pub type TimelineBundle = (WorkerTimelineEvent, Duration, Diff);

#[derive(Clone)]
pub struct DataflowSenders {
    pub node_sender: Sender<Event<Duration, NodeBundle>>,
    pub edge_sender: Sender<Event<Duration, EdgeBundle>>,
    pub subgraph_sender: Sender<Event<Duration, SubgraphBundle>>,
    pub stats_sender: Sender<Event<Duration, StatsBundle>>,
    pub timeline_sender: Sender<Event<Duration, TimelineBundle>>,
}

impl DataflowSenders {
    pub fn new(
        node_sender: Sender<Event<Duration, NodeBundle>>,
        edge_sender: Sender<Event<Duration, EdgeBundle>>,
        subgraph_sender: Sender<Event<Duration, SubgraphBundle>>,
        stats_sender: Sender<Event<Duration, StatsBundle>>,
        timeline_sender: Sender<Event<Duration, TimelineBundle>>,
    ) -> Self {
        Self {
            node_sender,
            edge_sender,
            subgraph_sender,
            stats_sender,
            timeline_sender,
        }
    }
}

pub fn dataflow<S>(
    scope: &mut S,
    _args: &Args,
    timely_traces: Vec<EventReader<Duration, TimelyLogBundle, TcpStream>>,
    differential_traces: Option<Vec<EventReader<Duration, DifferentialLogBundle, TcpStream>>>,
    replay_shutdown: Arc<AtomicBool>,
    senders: DataflowSenders,
) -> Result<ProbeHandle<Duration>>
where
    S: Scope<Timestamp = Duration>,
{
    let mut probe = ProbeHandle::new();

    // The timely log stream filtered down to worker 0's events
    let timely_stream = timely_traces
        .replay_with_shutdown_into_named("Timely Replay", scope, replay_shutdown.clone())
        .debug_inspect(|x| tracing::trace!("timely event: {:?}", x));

    let raw_differential_stream = differential_traces.map(|traces| {
        traces
            .replay_with_shutdown_into_named("Differential Replay", scope, replay_shutdown)
            .debug_inspect(|x| tracing::trace!("differential dataflow event: {:?}", x))
    });

    let operators = operator_creations(&timely_stream);

    let operator_stats = operator_stats(scope, &timely_stream);
    let operator_names = operators
        .map(|(worker, operates)| ((worker, operates.id), operates.name))
        .arrange_by_key();

    let operator_stats = raw_differential_stream
        .as_ref()
        .map(|stream| {
            let arrangement_stats = arrangement_stats(scope, stream).consolidate();

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
        &timely_stream,
        raw_differential_stream.as_ref(),
        &operator_names,
    );

    let addressed_operators = operators
        .map(|(worker, operator)| ((worker, Address::new(operator.addr.clone())), operator));

    addressed_operators
        .semijoin(&leaves)
        .consolidate()
        .inner
        .probe_with(&mut probe)
        .capture_into(CrossbeamPusher::new(senders.node_sender));

    addressed_operators
        .semijoin(&subgraphs)
        .consolidate()
        .inner
        .probe_with(&mut probe)
        .capture_into(CrossbeamPusher::new(senders.subgraph_sender));

    edges
        .consolidate()
        .inner
        .probe_with(&mut probe)
        .capture_into(CrossbeamPusher::new(senders.edge_sender));

    operator_stats
        .consolidate()
        .inner
        .probe_with(&mut probe)
        .capture_into(CrossbeamPusher::new(senders.stats_sender));

    worker_timeline
        .consolidate()
        .inner
        .probe_with(&mut probe)
        .capture_into(CrossbeamPusher::new(senders.timeline_sender));

    // TODO: Fix this
    // // If saving logs is enabled, write all log messages to the `save_logs` file
    // if let Some(save_logs) = args.save_logs.as_ref() {
    //     if scope.index() == 0 {
    //         let file = File::create(save_logs).context("failed to open `--save-logs` file")?;
    //         raw_timely_stream.capture_into(EventWriter::new(file));
    //     }
    // }

    Ok(probe)
}

fn operator_creations<S>(
    log_stream: &Stream<S, TimelyLogBundle>,
) -> Collection<S, (WorkerIdentifier, OperatesEvent), Diff>
where
    S: Scope<Timestamp = Duration>,
{
    log_stream
        .flat_map(|(time, worker, event)| {
            if let TimelyEvent::Operates(event) = event {
                Some(((worker, event), time, 1))
            } else {
                None
            }
        })
        .as_collection()
}

fn channel_creations<S>(
    log_stream: &Stream<S, TimelyLogBundle>,
) -> Collection<S, (WorkerIdentifier, ChannelsEvent), Diff>
where
    S: Scope<Timestamp = Duration>,
{
    log_stream
        .flat_map(|(time, worker, event)| {
            if let TimelyEvent::Channels(event) = event {
                Some(((worker, event), time, 1))
            } else {
                None
            }
        })
        .as_collection()
}

type LeavesAndScopes<S, D> = (
    Collection<S, (WorkerIdentifier, Address), D>,
    Collection<S, (WorkerIdentifier, Address), D>,
);

fn sift_leaves_and_scopes<S, D>(
    scope: &mut S,
    operators: &Collection<S, (WorkerIdentifier, OperatesEvent), D>,
) -> LeavesAndScopes<S, D>
where
    S: Scope,
    S::Timestamp: Lattice + TotalOrder,
    D: Semigroup + Monoid + Abelian + ExchangeData + Multiply<isize, Output = D>,
{
    scope.region_named("Sift Leaves and Scopes", |region| {
        let operators = operators
            .enter_region(region)
            .map(|(worker, operator)| ((worker, Address::new(operator.addr)), ()));

        // The addresses of potential scopes, excluding leaf operators
        let potential_scopes = operators
            .map(|((worker, mut addr), ())| {
                addr.addr.pop();
                (worker, addr)
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
    operators: &Collection<S, (WorkerIdentifier, OperatesEvent), D>,
    channels: &Collection<S, (WorkerIdentifier, Channel), D>,
    leaves: &Collection<S, (WorkerIdentifier, Address), D>,
) -> Collection<S, (WorkerIdentifier, OperatesEvent, Channel, OperatesEvent), D>
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

        let operators_by_address = operators
            .map(|(worker, operator)| ((worker, Address::new(operator.addr.clone())), operator));

        operators_by_address
            .semijoin(&leaves)
            .join_map(
                &channels.map(|(worker, channel)| ((worker, channel.source_addr()), channel)),
                |&(worker, ref _src_addr), src_operator, channel| {
                    (
                        (worker, channel.target_addr()),
                        (src_operator.clone(), channel.clone()),
                    )
                },
            )
            .join_map(
                &operators_by_address,
                |&(worker, ref _target_addr), (src_operator, channel), target_operator| {
                    (
                        worker,
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
