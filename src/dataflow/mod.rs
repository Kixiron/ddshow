// mod channel_stats;
mod differential;
mod operator_stats;
pub mod operators;
#[cfg(feature = "timely-next")]
mod reachability;
mod subgraphs;
mod summation;
mod tests;
mod types;
mod worker_timeline;

pub use operator_stats::OperatorStats;
pub use types::{ChannelId, OperatesEvent, OperatorAddr, OperatorId, PortId, WorkerId};
pub use worker_timeline::{TimelineEvent, WorkerTimelineEvent};

use crate::{
    args::Args,
    dataflow::operators::{DiffDuration, Max, SortBy},
};
use abomonation_derive::Abomonation;
use anyhow::Result;
use crossbeam_channel::Sender;
use differential::arrangement_stats;
use differential_dataflow::{
    collection::AsCollection,
    difference::{Abelian, DiffPair, Monoid, Semigroup},
    lattice::Lattice,
    logging::DifferentialEvent,
    operators::{arrange::ArrangeByKey, Consolidate, CountTotal, Join, ThresholdTotal},
    Collection, ExchangeData, Hashable,
};
use operator_stats::operator_stats;
use operators::{CrossbeamPusher, FilterMap, InspectExt, Multiply, ReplayWithShutdown};
#[cfg(feature = "timely-next")]
use reachability::TrackerEvent;
use std::{
    cmp::Reverse,
    fmt::Debug,
    net::TcpStream,
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};
use subgraphs::rewire_channels;
use timely::{
    dataflow::{
        channels::pact::Pipeline,
        operators::{
            capture::{Capture, Event, EventReader},
            probe::Handle as ProbeHandle,
            Concat, Exchange, Map, Operator, Probe,
        },
        Scope, Stream,
    },
    logging::{ChannelsEvent, TimelyEvent, WorkerIdentifier},
    order::TotalOrder,
};
use worker_timeline::worker_timeline;

// TODO: Newtype channel ids, operators ids, channel addrs and operator addrs and worker ids

type TimelyLogBundle<Id = WorkerId> = (Duration, Id, TimelyEvent);
type DifferentialLogBundle<Id = WorkerId> = (Duration, Id, DifferentialEvent);

#[cfg(feature = "timely-next")]
type ReachabilityLogBundle = (Duration, WorkerId, TrackerEvent);
type Diff = isize;

pub type NodeBundle = (((WorkerId, OperatorAddr), OperatesEvent), Duration, Diff);
pub type EdgeBundle = (
    (
        WorkerId,
        OperatesEvent,
        Channel,
        OperatesEvent,
        // Option<ChannelMessageStats>,
    ),
    Duration,
    Diff,
);
pub type SubgraphBundle = (((WorkerId, OperatorAddr), OperatesEvent), Duration, Diff);
pub type StatsBundle = (((WorkerId, usize), OperatorStats), Duration, Diff);
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
    timely_traces: Vec<EventReader<Duration, TimelyLogBundle<WorkerIdentifier>, TcpStream>>,
    differential_traces: Option<
        Vec<EventReader<Duration, DifferentialLogBundle<WorkerIdentifier>, TcpStream>>,
    >,
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
        .map(|(time, worker, event)| (time, WorkerId::new(worker), event))
        .debug_inspect(|x| tracing::trace!("timely event: {:?}", x));

    let raw_differential_stream = differential_traces.map(|traces| {
        traces
            .replay_with_shutdown_into_named("Differential Replay", scope, replay_shutdown)
            .map(|(time, worker, event)| (time, WorkerId::new(worker), event))
            .debug_inspect(|x| tracing::trace!("differential dataflow event: {:?}", x))
    });

    let (program_event_overview, worker_event_overview) =
        aggregate_overview_stats(&timely_stream, raw_differential_stream.as_ref());

    let operators = operator_creations(&timely_stream);

    let operator_stats = operator_stats(scope, &timely_stream);
    let operator_names = operators
        .map(|(worker, operates)| ((worker, operates.id), operates.name))
        .arrange_by_key();

    let cross_worker_sorted_operators = operator_stats
        .map(|(_, stats)| ((), stats))
        .sort_by(|stats| Reverse(stats.total))
        .map(|((), stats)| stats)
        .inspect(|x| println!("{:?}", x));

    let per_worker_sorted_operators = operator_stats
        .map(|((worker, _), stats)| (worker, stats))
        .sort_by(|stats| Reverse(stats.total))
        .inspect(|x| println!("{:?}", x));

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

    let addressed_operators =
        operators.map(|(worker, operator)| ((worker, operator.addr.clone()), operator));

    channel_sink(
        &addressed_operators.semijoin(&leaves),
        &mut probe,
        senders.node_sender,
    );
    channel_sink(
        &addressed_operators.semijoin(&subgraphs),
        &mut probe,
        senders.subgraph_sender,
    );
    channel_sink(&edges, &mut probe, senders.edge_sender);
    channel_sink(&operator_stats, &mut probe, senders.stats_sender);
    channel_sink(&worker_timeline, &mut probe, senders.timeline_sender);

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

/// Returns the total events & highest timestamp (program duration) for the entire program
/// in the first collection and the per-worker total events & highest timestamp for all
/// workers of the program
fn aggregate_overview_stats<S>(
    timely: &Stream<S, TimelyLogBundle>,
    differential: Option<&Stream<S, DifferentialLogBundle>>,
) -> (
    Collection<S, (Diff, Duration), Diff>,
    Collection<S, (WorkerId, (Diff, Duration)), Diff>,
)
where
    S: Scope<Timestamp = Duration>,
{
    let mut events = timely.map(|(time, worker, _event)| {
        (
            worker,
            time,
            DiffPair::new(1, Max::new(DiffDuration::new(time))),
        )
    });

    if let Some(differential) = differential {
        events = events.concat(&differential.map(|(time, worker, _event)| {
            (
                worker,
                time,
                DiffPair::new(1, Max::new(DiffDuration::new(time))),
            )
        }));
    }

    let total_events = events
        .map(|(_worker, time, diff)| ((), time, diff))
        .as_collection()
        .count_total()
        .map(
            |(
                (),
                DiffPair {
                    element1: total_events,
                    element2: max_timestamp,
                },
            )| { (total_events, max_timestamp.into_inner().0) },
        );

    let worker_events = events.as_collection().count_total().map(
        |(
            worker,
            DiffPair {
                element1: total_events,
                element2: max_timestamp,
            },
        )| { (worker, (total_events, max_timestamp.into_inner().0)) },
    );

    (total_events, worker_events)
}

fn operator_creations<S>(
    log_stream: &Stream<S, TimelyLogBundle>,
) -> Collection<S, (WorkerId, OperatesEvent), Diff>
where
    S: Scope<Timestamp = Duration>,
{
    log_stream
        .flat_map(|(time, worker, event)| {
            if let TimelyEvent::Operates(event) = event {
                Some(((worker, OperatesEvent::from(event)), time, 1))
            } else {
                None
            }
        })
        .as_collection()
}

fn channel_creations<S>(
    log_stream: &Stream<S, TimelyLogBundle>,
) -> Collection<S, (WorkerId, ChannelsEvent), Diff>
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
    Collection<S, (WorkerId, OperatorAddr), D>,
    Collection<S, (WorkerId, OperatorAddr), D>,
);

fn sift_leaves_and_scopes<S, D>(
    scope: &mut S,
    operators: &Collection<S, (WorkerId, OperatesEvent), D>,
) -> LeavesAndScopes<S, D>
where
    S: Scope,
    S::Timestamp: Lattice + TotalOrder,
    D: Semigroup + Monoid + Abelian + ExchangeData + Multiply<isize, Output = D>,
{
    scope.region_named("Sift Leaves and Scopes", |region| {
        let operators = operators
            .enter_region(region)
            .map(|(worker, operator)| ((worker, operator.addr), ()));

        // The addresses of potential scopes, excluding leaf operators
        let potential_scopes = operators
            .map(|((worker, mut addr), ())| {
                addr.pop();

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
    operators: &Collection<S, (WorkerId, OperatesEvent), D>,
    channels: &Collection<S, (WorkerId, Channel), D>,
    leaves: &Collection<S, (WorkerId, OperatorAddr), D>,
) -> Collection<S, (WorkerId, OperatesEvent, Channel, OperatesEvent), D>
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
            operators.map(|(worker, operator)| ((worker, operator.addr.clone()), operator));

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

#[allow(clippy::type_complexity)]
fn channel_sink<S, D, R>(
    collection: &Collection<S, D, R>,
    probe: &mut ProbeHandle<S::Timestamp>,
    channel: Sender<Event<S::Timestamp, (D, S::Timestamp, R)>>,
) where
    S: Scope,
    S::Timestamp: Lattice,
    D: ExchangeData + Hashable,
    R: Semigroup + ExchangeData,
{
    let consolidated = collection
        .inner
        .exchange(|_| 0)
        .as_collection()
        .consolidate();

    let worker_id = collection.scope().index();
    if worker_id == 0 {
        tracing::debug!("installed channel sink on worker {}", worker_id);

        consolidated
            .inner
            .probe_with(probe)
            .capture_into(CrossbeamPusher::new(channel));
    } else {
        tracing::debug!("installed channel sink assertion on worker {}", worker_id);

        consolidated
            .inner
            .probe_with(probe)
            .sink(Pipeline, "AssertEmpty", move |input| {
                input.for_each(move |_time, data| {
                    if !data.is_empty() {
                        tracing::error!(
                            "worker {} received unexpected post-exchange data",
                            worker_id,
                        );
                    }
                });
            });
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub enum Channel {
    ScopeCrossing {
        channel_id: usize,
        source_addr: OperatorAddr,
        target_addr: OperatorAddr,
    },

    Normal {
        channel_id: usize,
        source_addr: OperatorAddr,
        target_addr: OperatorAddr,
    },
}

impl Channel {
    pub const fn channel_id(&self) -> usize {
        match *self {
            Self::ScopeCrossing { channel_id, .. } | Self::Normal { channel_id, .. } => channel_id,
        }
    }

    pub fn source_addr(&self) -> OperatorAddr {
        match self {
            Self::ScopeCrossing { source_addr, .. } | Self::Normal { source_addr, .. } => {
                source_addr.to_owned()
            }
        }
    }

    pub fn target_addr(&self) -> OperatorAddr {
        match self {
            Self::ScopeCrossing { target_addr, .. } | Self::Normal { target_addr, .. } => {
                target_addr.to_owned()
            }
        }
    }
}
