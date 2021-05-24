// mod channel_stats;
mod differential;
mod operator_stats;
pub mod operators;
mod program_stats;
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
    dataflow::{
        differential::{arrangement_stats, ArrangementStats},
        operator_stats::operator_stats,
        operators::{
            rkyv_capture::{RkyvOperatesEvent, RkyvTimelyEvent},
            CrossbeamPusher, FilterMap, JoinArranged, Multiply, RkyvChannelsEvent, RkyvEventWriter,
            SortBy,
        },
        subgraphs::rewire_channels,
        worker_timeline::worker_timeline,
    },
    ui::{ProgramStats, WorkerStats},
};
use abomonation_derive::Abomonation;
use anyhow::{Context, Result};
use crossbeam_channel::Sender;
use differential_dataflow::{
    collection::AsCollection,
    difference::{Abelian, Monoid, Semigroup},
    lattice::Lattice,
    logging::DifferentialEvent,
    operators::{
        arrange::{ArrangeByKey, ArrangeBySelf, Arranged, TraceAgent},
        Consolidate, Join, Reduce, ThresholdTotal,
    },
    trace::implementations::ord::OrdKeySpine,
    Collection, ExchangeData, Hashable,
};
#[cfg(feature = "timely-next")]
use reachability::TrackerEvent;
use std::{
    convert::TryFrom,
    fmt::Debug,
    fs::{self, File},
    io::BufWriter,
    time::Duration,
};
use timely::{
    dataflow::{
        operators::{
            capture::{Capture, Event},
            probe::Handle as ProbeHandle,
            Exchange, Map, Probe,
        },
        Scope, ScopeParent, Stream,
    },
    order::TotalOrder,
};

// TODO: Dataflow lints
//       - Inconsistent dataflows across workers
//       - Not arranging before a loop feedback
//       - you aren't supposed to be able to forge capabilities,
//         but you can take an incoming CapabilityRef and turn
//         it in to a Capability for any output, even those that
//         the input should not be connected to via the summary.
//       - Packing `(data, time, diff)` updates in DD where time
//         is not greater or equal to the message capability.

/// Only cause the program stats to update every N milliseconds to
/// prevent this from absolutely thrashing the scheduler
// TODO: Make this configurable by the user
pub const PROGRAM_NS_GRANULARITY: u128 = 5_000_000_000;

fn granulate(&time: &Duration) -> Duration {
    let timestamp = time.as_nanos();
    let window_idx = (timestamp / PROGRAM_NS_GRANULARITY) + 1;

    let minted = Duration::from_nanos((window_idx * PROGRAM_NS_GRANULARITY) as u64);
    debug_assert_eq!(
        u64::try_from(window_idx * PROGRAM_NS_GRANULARITY).map(|res| res as u128),
        Ok(window_idx * PROGRAM_NS_GRANULARITY),
    );
    debug_assert!(time <= minted);

    minted
}

pub type TimelyLogBundle<Id = WorkerId> = (Duration, Id, RkyvTimelyEvent);
pub type DifferentialLogBundle<Id = WorkerId> = (Duration, Id, DifferentialEvent);

#[cfg(feature = "timely-next")]
type ReachabilityLogBundle = (Duration, WorkerId, TrackerEvent);
type Diff = isize;

pub type NodeBundle = (
    ((WorkerId, OperatorAddr), RkyvOperatesEvent),
    Duration,
    Diff,
);
pub type EdgeBundle = (
    (
        WorkerId,
        RkyvOperatesEvent,
        Channel,
        RkyvOperatesEvent,
        // Option<ChannelMessageStats>,
    ),
    Duration,
    Diff,
);
pub type SubgraphBundle = (
    ((WorkerId, OperatorAddr), RkyvOperatesEvent),
    Duration,
    Diff,
);
pub type StatsBundle = (((WorkerId, OperatorId), OperatorStats), Duration, Diff);
pub type AggStatsBundle = ((OperatorId, OperatorStats), Duration, Diff);
pub type TimelineBundle = (WorkerTimelineEvent, Duration, Diff);
pub type ProgramStatsBundle = (ProgramStats, Duration, Diff);
pub type WorkerStatsBundle = (Vec<(WorkerId, WorkerStats)>, Duration, Diff);
pub type NameLookupBundle = (((WorkerId, OperatorId), String), Duration, Diff);
pub type AddrLookupBundle = (((WorkerId, OperatorId), OperatorAddr), Duration, Diff);

type ChannelAddrs<S, D> = Arranged<
    S,
    TraceAgent<OrdKeySpine<(WorkerId, OperatorAddr), <S as ScopeParent>::Timestamp, D>>,
>;

#[derive(Clone)]
pub struct DataflowSenders {
    pub node_sender: Sender<Event<Duration, NodeBundle>>,
    pub edge_sender: Sender<Event<Duration, EdgeBundle>>,
    pub subgraph_sender: Sender<Event<Duration, SubgraphBundle>>,
    pub stats_sender: Sender<Event<Duration, StatsBundle>>,
    pub aggregated_stats_sender: Sender<Event<Duration, AggStatsBundle>>,
    pub timeline_sender: Sender<Event<Duration, TimelineBundle>>,
    pub program_stats: Sender<Event<Duration, ProgramStatsBundle>>,
    pub worker_stats: Sender<Event<Duration, WorkerStatsBundle>>,
    pub name_lookup: Sender<Event<Duration, NameLookupBundle>>,
    pub addr_lookup: Sender<Event<Duration, AddrLookupBundle>>,
}

impl DataflowSenders {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_sender: Sender<Event<Duration, NodeBundle>>,
        edge_sender: Sender<Event<Duration, EdgeBundle>>,
        subgraph_sender: Sender<Event<Duration, SubgraphBundle>>,
        stats_sender: Sender<Event<Duration, StatsBundle>>,
        aggregated_stats_sender: Sender<Event<Duration, AggStatsBundle>>,
        timeline_sender: Sender<Event<Duration, TimelineBundle>>,
        program_stats: Sender<Event<Duration, ProgramStatsBundle>>,
        worker_stats: Sender<Event<Duration, WorkerStatsBundle>>,
        name_lookup: Sender<Event<Duration, NameLookupBundle>>,
        addr_lookup: Sender<Event<Duration, AddrLookupBundle>>,
    ) -> Self {
        Self {
            node_sender,
            edge_sender,
            subgraph_sender,
            stats_sender,
            aggregated_stats_sender,
            timeline_sender,
            program_stats,
            worker_stats,
            name_lookup,
            addr_lookup,
        }
    }
}

pub fn dataflow<S>(
    scope: &mut S,
    args: &Args,
    timely_stream: &Stream<S, TimelyLogBundle>,
    differential_stream: Option<&Stream<S, DifferentialLogBundle>>,
    senders: DataflowSenders,
) -> Result<ProbeHandle<Duration>>
where
    S: Scope<Timestamp = Duration>,
{
    let mut probe = ProbeHandle::new();

    // let (program_event_overview, worker_event_overview) =
    //     aggregate_overview_stats(&timely_stream, differential_stream.as_ref());

    let operators = operator_creations(&timely_stream);

    let operator_stats = operator_stats(scope, &timely_stream);
    let operator_names = operators
        .map(|(worker, operates)| ((worker, operates.id), operates.name))
        .arrange_by_key();

    // let cross_worker_sorted_operators = operator_stats
    //     .map(|(_, stats)| ((), stats))
    //     .hierarchical_sort_by(|stats| Reverse(stats.total))
    //     .map(|((), stats)| stats);

    // let per_worker_sorted_operators = operator_stats
    //     .map(|((worker, _), stats)| (worker, stats))
    //     .hierarchical_sort_by(|stats| Reverse(stats.total));

    let operator_stats = differential_stream
        .as_ref()
        .map(|stream| {
            let arrangement_stats = arrangement_stats(scope, stream).consolidate();

            let modified_stats = operator_stats.join_map(
                &arrangement_stats,
                |&operator, stats, &arrangement_stats| {
                    let mut stats = stats.clone();
                    stats.arrangement_size = Some(arrangement_stats);

                    (operator, stats)
                },
            );

            operator_stats
                .antijoin(&modified_stats.map(|(operator, _)| operator))
                .concat(&modified_stats)
        })
        .unwrap_or(operator_stats);

    // FIXME: This is pretty much a guess since there's no way to actually associate
    //        operators/arrangements/channels across workers
    // TODO: This should use a specialized struct to hold relevant things like "total size across workers"
    //       in addition to per-worker stats
    let cross_aggregated_operator_stats = operator_stats
        .map(|((_worker, operator), stats)| (operator, stats))
        .reduce(|&operator, worker_stats, aggregated| {
            let activations = worker_stats.len();
            let mut activation_durations = Vec::new();
            let mut arrangements = Vec::new();
            let mut totals = Vec::new();

            for (stats, _diff) in worker_stats {
                totals.push(stats.total);
                activation_durations.extend(stats.activation_durations.iter().copied());

                if let Some(size) = stats.arrangement_size {
                    arrangements.push(size);
                }
            }

            let max = activation_durations
                .iter()
                .map(|&(duration, _)| duration)
                .max()
                .expect("reduce is always called with non-empty input");

            let min = activation_durations
                .iter()
                .map(|&(duration, _)| duration)
                .min()
                .expect("reduce is always called with non-empty input");

            let average = activation_durations
                .iter()
                .map(|&(duration, _)| duration)
                .sum::<Duration>()
                / activation_durations.len() as u32;

            let total = totals.iter().sum::<Duration>() / totals.len() as u32;

            let arrangement_size = if arrangements.is_empty() {
                None
            } else {
                let max_size = arrangements
                    .iter()
                    .map(|arr| arr.max_size)
                    .max()
                    .expect("arrangements is non-empty");

                let min_size = arrangements
                    .iter()
                    .map(|arr| arr.max_size)
                    .min()
                    .expect("arrangements is non-empty");

                let batches =
                    arrangements.iter().map(|arr| arr.batches).sum::<usize>() / arrangements.len();

                Some(ArrangementStats {
                    max_size,
                    min_size,
                    batches,
                })
            };

            let aggregate = OperatorStats {
                id: operator,
                worker: worker_stats[0].0.worker,
                max,
                min,
                average,
                total,
                activations,
                activation_durations,
                arrangement_size,
            };
            aggregated.push((aggregate, 1))
        });

    // TODO: Turn these into collections of `(WorkerId, OperatorId)` and arrange them
    let (leaves, subgraphs) = sift_leaves_and_scopes(scope, &operators);
    let (leaves_arranged, subgraphs_arranged) =
        (leaves.arrange_by_self(), subgraphs.arrange_by_self());

    let channel_events = channel_creations(&timely_stream);
    let channels = rewire_channels(scope, &channel_events, &subgraphs_arranged);

    let edges = attach_operators(scope, &operators, &channels, &leaves_arranged);

    let worker_timeline =
        worker_timeline(scope, &timely_stream, differential_stream, &operator_names);

    // TODO: Arrange this
    let addressed_operators =
        operators.map(|(worker, operator)| ((worker, operator.addr.clone()), operator));

    let (program_stats, worker_stats) = program_stats::aggregate_worker_stats(
        &timely_stream,
        differential_stream,
        &operators,
        &channels,
        &subgraphs_arranged,
    );

    channel_sink(
        &operator_names
            .as_collection(|&(worker, operator), name| ((worker, operator), name.to_owned())),
        &mut probe,
        senders.name_lookup,
    );
    channel_sink(
        &operators.map(|(worker, event)| ((worker, event.id), event.addr)),
        &mut probe,
        senders.addr_lookup,
    );
    channel_sink(&program_stats, &mut probe, senders.program_stats);
    channel_sink(
        &worker_stats
            .map(|(worker, stats)| ((), (worker, stats)))
            .sort_by(|&(worker, _)| worker)
            .map(|((), sorted_stats)| sorted_stats),
        &mut probe,
        senders.worker_stats,
    );
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
    channel_sink(
        &cross_aggregated_operator_stats,
        &mut probe,
        senders.aggregated_stats_sender,
    );
    channel_sink(&worker_timeline, &mut probe, senders.timeline_sender);

    // TODO: Save ddflow logs
    // If saving logs is enabled, write all log messages to the `save_logs` directory
    if let Some(save_logs) = args.save_logs.as_ref() {
        fs::create_dir_all(&save_logs).context("failed to create `--save-logs` directory")?;

        let file_name = if scope.index() == 0 {
            "timely.ddshow"

        // Make other workers pipe to a null output
        } else if cfg!(windows) {
            "nul"
        } else {
            "/dev/null"
        };

        let timely_file = BufWriter::new(
            File::create(save_logs.join(file_name))
                .context("failed to create `--save-logs` timely file")?,
        );

        timely_stream
            .exchange(|_| 0)
            .probe_with(&mut probe)
            .capture_into(RkyvEventWriter::new(timely_file));
    }

    Ok(probe)
}

fn operator_creations<S>(
    log_stream: &Stream<S, TimelyLogBundle>,
) -> Collection<S, (WorkerId, RkyvOperatesEvent), Diff>
where
    S: Scope<Timestamp = Duration>,
{
    log_stream
        .flat_map(|(time, worker, event)| {
            if let RkyvTimelyEvent::Operates(event) = event {
                Some(((worker, event), time, 1))
            } else {
                None
            }
        })
        .as_collection()
}

fn channel_creations<S>(
    log_stream: &Stream<S, TimelyLogBundle>,
) -> Collection<S, (WorkerId, RkyvChannelsEvent), Diff>
where
    S: Scope<Timestamp = Duration>,
{
    log_stream
        .flat_map(|(time, worker, event)| {
            if let RkyvTimelyEvent::Channels(event) = event {
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
    operators: &Collection<S, (WorkerId, RkyvOperatesEvent), D>,
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
    operators: &Collection<S, (WorkerId, RkyvOperatesEvent), D>,
    channels: &Collection<S, (WorkerId, Channel), D>,
    leaves: &ChannelAddrs<S, D>,
) -> Collection<S, (WorkerId, RkyvOperatesEvent, Channel, RkyvOperatesEvent), D>
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
            .semijoin_arranged(&leaves)
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
    collection
        .consolidate()
        .inner
        .probe_with(probe)
        .capture_into(CrossbeamPusher::new(channel));

    tracing::debug!(
        "installed channel sink on worker {}",
        collection.scope().index(),
    );
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub enum Channel {
    ScopeCrossing {
        channel_id: OperatorId,
        source_addr: OperatorAddr,
        target_addr: OperatorAddr,
    },

    Normal {
        channel_id: OperatorId,
        source_addr: OperatorAddr,
        target_addr: OperatorAddr,
    },
}

impl Channel {
    pub const fn channel_id(&self) -> OperatorId {
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
