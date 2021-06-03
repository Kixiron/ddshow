// mod channel_stats;
pub(crate) mod constants;
mod differential;
mod operator_stats;
pub mod operators;
mod program_stats;
#[cfg(feature = "timely-next")]
mod reachability;
mod send_recv;
mod subgraphs;
mod summation;
mod tests;
mod worker_timeline;

pub use constants::{DIFFERENTIAL_DISK_LOG_FILE, PROGRAM_NS_GRANULARITY, TIMELY_DISK_LOG_FILE};
pub use operator_stats::OperatorStats;
pub use send_recv::{DataflowData, DataflowExtractor, DataflowReceivers, DataflowSenders};
pub use worker_timeline::{TimelineEvent, WorkerTimelineEvent};

use crate::{
    args::Args,
    dataflow::{
        differential::{arrangement_stats, ArrangementStats},
        operator_stats::{extract_timely_info, operator_stats},
        operators::{CrossbeamPusher, FilterMap, JoinArranged, Multiply, SortBy},
        send_recv::ChannelAddrs,
        subgraphs::rewire_channels,
        worker_timeline::worker_timeline,
    },
    ui::{DataflowStats, Lifespan, ProgramStats, WorkerStats},
};
use abomonation_derive::Abomonation;
use anyhow::{Context, Result};
use crossbeam_channel::Sender;
use ddshow_sink::EventWriter;
use ddshow_types::{
    differential_logging::DifferentialEvent,
    timely_logging::{OperatesEvent, TimelyEvent},
    ChannelId, OperatorAddr, OperatorId, WorkerId,
};
use differential_dataflow::{
    difference::Semigroup,
    lattice::Lattice,
    operators::{
        arrange::{ArrangeByKey, ArrangeBySelf, Arranged, TraceAgent},
        Consolidate, CountTotal, Join, JoinCore, Reduce, ThresholdTotal,
    },
    trace::{
        implementations::ord::{OrdKeySpine, OrdValSpine},
        TraceReader,
    },
    Collection, ExchangeData, Hashable,
};
#[cfg(feature = "timely-next")]
use reachability::TrackerEvent;
use std::{
    convert::TryFrom,
    fmt::Debug,
    fs::{self, File},
    io::BufWriter,
    iter,
    path::{Path, PathBuf},
    time::Duration,
};
use timely::{
    dataflow::{
        operators::{
            capture::{Capture, Event},
            probe::Handle as ProbeHandle,
            Probe,
        },
        Scope, ScopeParent, Stream,
    },
    order::TotalOrder,
};

type ArrangedVal<S, K, V, D = Diff> =
    Arranged<S, TraceAgent<OrdValSpine<K, V, <S as ScopeParent>::Timestamp, D>>>;

type ArrangedKey<S, K, D = Diff> =
    Arranged<S, TraceAgent<OrdKeySpine<K, <S as ScopeParent>::Timestamp, D>>>;

// TODO: Dataflow lints
//       - Inconsistent dataflows across workers
//       - Not arranging before a loop feedback
//       - you aren't supposed to be able to forge capabilities,
//         but you can take an incoming CapabilityRef and turn
//         it in to a Capability for any output, even those that
//         the input should not be connected to via the summary.
//       - Packing `(data, time, diff)` updates in DD where time
//         is not greater or equal to the message capability.

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

pub type TimelyLogBundle<Id = WorkerId> = (Time, Id, TimelyEvent);
pub type DifferentialLogBundle<Id = WorkerId> = (Time, Id, DifferentialEvent);

#[cfg(feature = "timely-next")]
type ReachabilityLogBundle = (Time, WorkerId, TrackerEvent);

type Diff = isize;
type Time = Duration;

pub fn dataflow<S>(
    scope: &mut S,
    args: &Args,
    timely_stream: &Stream<S, TimelyLogBundle>,
    differential_stream: Option<&Stream<S, DifferentialLogBundle>>,
    senders: DataflowSenders,
) -> Result<ProbeHandle<Time>>
where
    S: Scope<Timestamp = Time>,
{
    let mut probe = ProbeHandle::new();

    let (
        operator_lifespans,
        operator_activations,
        _operator_creations,
        _channel_creations,
        // TODO: Refactor the channel logic to not need this
        raw_channels,
        // TODO: Refactor the logic to not need this
        raw_operators,
        operator_names,
        operator_ids,
        operator_addrs,
        operator_addrs_by_self,
        channel_scopes,
        dataflow_ids,
        timely_events,
    ) = extract_timely_info(scope, timely_stream);

    let operator_stats = operator_stats(scope, &operator_activations);

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
    let (leaves, subgraphs) = sift_leaves_and_scopes(scope, &operator_addrs_by_self);
    let (leaves_arranged, subgraphs_arranged) =
        (leaves.arrange_by_self(), subgraphs.arrange_by_self());
    let subgraph_ids = subgraphs_arranged
        .join_core(&operator_addrs, |&(worker, _), &(), &id| {
            iter::once((worker, id))
        })
        .arrange_by_self();

    let channels = rewire_channels(scope, &raw_channels, &subgraphs_arranged);

    let edges = attach_operators(scope, &raw_operators, &channels, &leaves_arranged);

    // TODO: Make `extract_timely_info()` get the relevant event information
    let timeline_events =
        worker_timeline(scope, differential_stream, &operator_names, &timely_events);

    // TODO: Arrange this
    let addressed_operators =
        raw_operators.map(|(worker, operator)| ((worker, operator.addr.clone()), operator));

    let (program_stats, worker_stats) = program_stats::aggregate_worker_stats(
        &timely_stream,
        differential_stream,
        &raw_operators,
        &channels,
        &subgraphs_arranged,
    );

    let dataflow_stats = dataflow_stats(
        &operator_lifespans,
        &dataflow_ids,
        &operator_ids,
        &subgraph_ids,
        &channel_scopes,
    );

    install_data_extraction(
        senders,
        &mut probe,
        program_stats,
        worker_stats,
        leaves,
        edges,
        subgraphs,
        operator_stats,
        addressed_operators,
        cross_aggregated_operator_stats,
        dataflow_stats,
        timeline_events,
        operator_names,
        operator_ids,
    );

    // TODO: Save ddflow logs
    // TODO: Probably want to prefix things with the current system time to allow
    //       "appending" logs by simply running ddshow at a later time and replaying
    //       log files in order of timestamp
    // TODO: For pause/resume profiling/debugging we'll probably need a custom log
    //       hook within timely, we can make it serve us rkyv events while we're at it
    // If saving logs is enabled, write all log messages to the `save_logs` directory
    if let Some(save_logs) = args.save_logs.as_ref() {
        tracing::info!(
            "installing timely{} log sinks",
            if differential_stream.is_some() {
                " and differential"
            } else {
                ""
            },
        );

        logging_event_sink(
            save_logs,
            scope,
            timely_stream,
            &mut probe,
            differential_stream,
        )?;
    }

    Ok(probe)
}

#[allow(clippy::clippy::too_many_arguments)]
fn install_data_extraction<S>(
    senders: DataflowSenders,
    probe: &mut ProbeHandle<Duration>,
    program_stats: Collection<S, ProgramStats>,
    worker_stats: Collection<S, (WorkerId, WorkerStats)>,
    nodes: Collection<S, (WorkerId, OperatorAddr)>,
    edges: Collection<S, (WorkerId, OperatesEvent, Channel, OperatesEvent)>,
    subgraphs: Collection<S, (WorkerId, OperatorAddr)>,
    operator_stats: Collection<S, ((WorkerId, OperatorId), OperatorStats)>,
    addressed_operators: Collection<S, ((WorkerId, OperatorAddr), OperatesEvent)>,
    aggregated_operator_stats: Collection<S, (OperatorId, OperatorStats)>,
    dataflow_stats: Collection<S, DataflowStats>,
    timeline_events: Collection<S, WorkerTimelineEvent>,
    operator_names: ArrangedVal<S, (WorkerId, OperatorId), String>,
    operator_ids: ArrangedVal<S, (WorkerId, OperatorId), OperatorAddr>,
) where
    S: Scope<Timestamp = Duration>,
{
    let worker_stats = worker_stats
        .map(|(worker, stats)| ((), (worker, stats)))
        .hierarchical_sort_by(|&(worker, _)| worker)
        .map(|((), sorted_stats)| sorted_stats);
    let nodes = addressed_operators.semijoin(&nodes);
    let subgraphs = addressed_operators.semijoin(&subgraphs);
    let operator_names = operator_names
        .as_collection(|&(worker, operator), name| ((worker, operator), name.to_owned()));
    let operator_ids = operator_ids.as_collection(|&key, addr| (key, addr.clone()));

    senders.install_sinks(
        probe,
        (&program_stats, true),
        (&worker_stats, true),
        (&nodes, true),
        (&edges, true),
        (&subgraphs, true),
        (&operator_stats, true),
        (&aggregated_operator_stats, true),
        (&dataflow_stats, true),
        (&timeline_events, true),
        (&operator_names, false),
        (&operator_ids, false),
    );
}

fn dataflow_stats<S, Tr1, Tr2, Tr3, Tr4>(
    operator_lifespans: &Collection<S, ((WorkerId, OperatorId), Lifespan), Diff>,
    dataflow_ids: &Arranged<S, TraceAgent<Tr1>>,
    addr_lookup: &Arranged<S, TraceAgent<Tr2>>,
    subgraph_ids: &Arranged<S, TraceAgent<Tr3>>,
    channel_scopes: &Arranged<S, TraceAgent<Tr4>>,
) -> Collection<S, DataflowStats, Diff>
where
    S: Scope<Timestamp = Duration>,
    Tr1: TraceReader<Key = (WorkerId, OperatorId), Val = (), Time = S::Timestamp, R = Diff>
        + 'static,
    Tr2: TraceReader<Key = (WorkerId, OperatorId), Val = OperatorAddr, Time = S::Timestamp, R = Diff>
        + 'static,
    Tr3: TraceReader<Key = (WorkerId, OperatorId), Val = (), Time = S::Timestamp, R = Diff>
        + 'static,
    Tr4: TraceReader<Key = (WorkerId, ChannelId), Val = OperatorAddr, Time = S::Timestamp, R = Diff>
        + 'static,
{
    let subgraph_addrs = subgraph_ids.join_core(&addr_lookup, |&(worker, id), &(), addr| {
        iter::once(((worker, addr.clone()), id))
    });

    // Addresses consist of sequences of parent operator ids like `[0, 1, 2]` where `[0, 1]` is a child of `[0]`
    // Therefore, to get all children of a given subgraph we can simply find all operators where the subgraph's
    // address (`[0]`) is contained within another operator's address (`[0, 1]` or `[0, 1, 2, 3, 4]`)
    let operator_parents = addr_lookup.flat_map_ref(|&(worker, operator), addr| {
        let mut parents = Vec::with_capacity(addr.len());
        parents
            .extend((1..addr.len()).map(|i| ((worker, OperatorAddr::from(&addr[..i])), operator)));

        parents
    });

    // Join all subgraphs against their children
    let subgraph_children = subgraph_addrs.join_map(
        &operator_parents,
        |&(worker, _), &subgraph_id, &operator_id| ((worker, operator_id), subgraph_id),
    );
    let subgraph_children_arranged = subgraph_children.arrange_by_key();

    // Get the number of operators underneath each subgraph
    let subgraph_operators = subgraph_children
        .map(|((worker, _), subgraph)| ((worker, subgraph), ()))
        .count_total()
        .map(|(((worker, subgraph), ()), operators)| ((worker, subgraph), operators as usize));

    // Get the number of subgraphs underneath each subgraph
    let subgraph_subgraphs = subgraph_children_arranged
        .semijoin_arranged(&subgraph_ids)
        .map(|((worker, _), subgraph)| ((worker, subgraph), ()))
        .count_total()
        .map(|(((worker, subgraph), ()), subgraphs)| ((worker, subgraph), subgraphs as usize));

    // Get all parents of channels
    let channel_parents = channel_scopes.flat_map_ref(|&(worker, channel), addr| {
        let mut parents = Vec::with_capacity(addr.len());
        parents
            .extend((1..addr.len()).map(|i| ((worker, OperatorAddr::from(&addr[..i])), channel)));

        parents
    });

    let subgraph_channels = subgraph_addrs
        .join_map(&channel_parents, |&(worker, _), &subgraph_id, _| {
            ((worker, subgraph_id), ())
        })
        .count_total()
        .map(|(((worker, subgraph), ()), channels)| ((worker, subgraph), channels as usize));

    // Find the addresses of all dataflows
    let dataflows = addr_lookup.semijoin_arranged(dataflow_ids);

    dataflows
        .join(&operator_lifespans)
        .join(&subgraph_operators)
        .join(&subgraph_subgraphs)
        .join(&subgraph_channels)
        .map(
            |((worker, id), ((((addr, lifespan), operators), subgraphs), channels))| {
                DataflowStats {
                    id,
                    addr,
                    worker,
                    operators,
                    subgraphs,
                    channels,
                    lifespan,
                }
            },
        )
}

type LeavesAndScopes<S, R> = (
    Collection<S, (WorkerId, OperatorAddr), R>,
    Collection<S, (WorkerId, OperatorAddr), R>,
);

fn sift_leaves_and_scopes<S>(
    scope: &mut S,
    operator_addrs: &ArrangedKey<S, (WorkerId, OperatorAddr)>,
) -> LeavesAndScopes<S, Diff>
where
    S: Scope,
    S::Timestamp: Lattice + TotalOrder,
{
    scope.region_named("Sift Leaves and Scopes", |region| {
        let operator_addrs = operator_addrs.enter_region(region);

        // The addresses of potential scopes, excluding leaf operators
        let potential_scopes = operator_addrs
            .flat_map_ref(|&(worker, ref addr), &()| {
                let mut addr = addr.clone();
                addr.pop();

                iter::once((worker, addr))
            })
            .distinct_total()
            .arrange_by_self();

        // Leaf operators
        let leaf_operators = operator_addrs
            .antijoin_arranged(&potential_scopes)
            .map(|(addr, _)| addr)
            .leave_region();

        // Only retain subgraphs that are observed within the logs
        let observed_subgraphs = operator_addrs
            .semijoin_arranged(&potential_scopes)
            .map(|(addr, ())| addr)
            .leave_region();

        (leaf_operators, observed_subgraphs)
    })
}

fn attach_operators<S, D>(
    scope: &mut S,
    operators: &Collection<S, (WorkerId, OperatesEvent), D>,
    channels: &Collection<S, (WorkerId, Channel), D>,
    leaves: &ChannelAddrs<S, D>,
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
    should_consolidate: bool,
) where
    S: Scope,
    S::Timestamp: Lattice,
    D: ExchangeData + Hashable,
    R: Semigroup + ExchangeData,
{
    let collection = if should_consolidate {
        collection.consolidate()
    } else {
        collection.clone()
    };

    collection
        .inner
        .probe_with(probe)
        .capture_into(CrossbeamPusher::new(channel));

    tracing::debug!(
        "installed channel sink on worker {}",
        collection.scope().index(),
    );
}

/// Store all timely and differential events to disk
fn logging_event_sink<S>(
    save_logs: &Path,
    scope: &mut S,
    timely_stream: &Stream<S, (Duration, WorkerId, TimelyEvent)>,
    probe: &mut ProbeHandle<Duration>,
    differential_stream: Option<&Stream<S, (Duration, WorkerId, DifferentialEvent)>>,
) -> Result<()>
where
    S: Scope<Timestamp = Duration>,
{
    // Create the directory for log files to go to
    fs::create_dir_all(&save_logs).context("failed to create `--save-logs` directory")?;

    let timely_path = log_file_path(TIMELY_DISK_LOG_FILE, save_logs, scope.index());

    tracing::debug!(
        "installing timely file sink on worker {} pointed at {}",
        scope.index(),
        timely_path.display(),
    );

    let timely_file = BufWriter::new(
        File::create(timely_path).context("failed to create `--save-logs` timely file")?,
    );

    timely_stream
        .probe_with(probe)
        .capture_into(EventWriter::new(timely_file));

    if let Some(differential_stream) = differential_stream {
        let differential_path = log_file_path(DIFFERENTIAL_DISK_LOG_FILE, save_logs, scope.index());

        tracing::debug!(
            "installing differential file sink on worker {} pointed at {}",
            scope.index(),
            differential_path.display(),
        );

        let differential_file = BufWriter::new(
            File::create(differential_path)
                .context("failed to create `--save-logs` differential file")?,
        );

        differential_stream
            .probe_with(probe)
            .capture_into(EventWriter::new(differential_file));
    }

    Ok(())
}

/// Constructs the path to a logging file for the given worker
fn log_file_path(file_prefix: &str, dir: &Path, worker_id: usize) -> PathBuf {
    dir.join(format!(
        "{}.replay-worker-{}.ddshow",
        file_prefix, worker_id
    ))
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub enum Channel {
    ScopeCrossing {
        channel_id: ChannelId,
        source_addr: OperatorAddr,
        target_addr: OperatorAddr,
    },

    Normal {
        channel_id: ChannelId,
        source_addr: OperatorAddr,
        target_addr: OperatorAddr,
    },
}

impl Channel {
    pub const fn channel_id(&self) -> ChannelId {
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
