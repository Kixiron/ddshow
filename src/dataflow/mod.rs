// mod channel_stats;
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
mod types;
mod worker_timeline;

pub use operator_stats::OperatorStats;
pub use send_recv::{DataflowData, DataflowExtractor, DataflowReceivers, DataflowSenders};
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
        send_recv::ChannelAddrs,
        subgraphs::rewire_channels,
        worker_timeline::worker_timeline,
    },
    ui::{DataflowStats, Lifespan},
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
        Consolidate, CountTotal, Join, JoinCore, Reduce, ThresholdTotal,
    },
    trace::TraceReader,
    Collection, ExchangeData, Hashable,
};
#[cfg(feature = "timely-next")]
use reachability::TrackerEvent;
use std::{
    collections::HashMap,
    convert::TryFrom,
    fmt::Debug,
    fs::{self, File},
    io::BufWriter,
    iter,
    path::PathBuf,
    time::Duration,
};
use timely::{
    dataflow::{
        channels::pact::Pipeline,
        operators::{
            capture::{Capture, Event},
            probe::Handle as ProbeHandle,
            Exchange, Map, Operator, Probe,
        },
        Scope, Stream,
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

pub type TimelyLogBundle<Id = WorkerId> = (Time, Id, RkyvTimelyEvent);
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
    mut senders: DataflowSenders,
) -> Result<ProbeHandle<Time>>
where
    S: Scope<Timestamp = Time>,
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

    let addr_lookup = operators
        .map(|(worker, event)| ((worker, event.id), event.addr))
        .arrange_by_key();
    let id_lookup = operators
        .map(|(worker, event)| ((worker, event.addr), event.id))
        .arrange_by_key();

    // TODO: Turn these into collections of `(WorkerId, OperatorId)` and arrange them
    let (leaves, subgraphs) = sift_leaves_and_scopes(scope, &operators);
    let (leaves_arranged, subgraphs_arranged) =
        (leaves.arrange_by_self(), subgraphs.arrange_by_self());
    let subgraph_ids = subgraphs_arranged
        .join_core(&id_lookup, |&(worker, _), &(), &id| {
            iter::once((worker, id))
        })
        .arrange_by_self();

    let channel_events = channel_creations(&timely_stream);
    let channel_scopes = channel_events
        .map(|(worker, event)| ((worker, event.id), event.scope_addr))
        .arrange_by_key();
    let channels = rewire_channels(scope, &channel_events, &subgraphs_arranged);

    let edges = attach_operators(scope, &operators, &channels, &leaves_arranged);

    let worker_timeline =
        worker_timeline(scope, &timely_stream, differential_stream, &operator_names);

    // TODO: Arrange this
    let addressed_operators =
        operators.map(|(worker, operator)| ((worker, operator.addr.clone()), operator));

    let dataflow_ids = addressed_operators
        .filter_map(|((worker, addr), event)| {
            if addr.is_top_level() {
                Some((worker, event.id))
            } else {
                None
            }
        })
        .arrange_by_self();

    let (program_stats, worker_stats) = program_stats::aggregate_worker_stats(
        &timely_stream,
        differential_stream,
        &operators,
        &channels,
        &subgraphs_arranged,
    );

    let dataflow_stats = dataflow_stats(
        timely_stream,
        &dataflow_ids,
        &addr_lookup,
        &subgraph_ids,
        &channel_scopes,
    );

    channel_sink(
        &operator_names
            .as_collection(|&(worker, operator), name| ((worker, operator), name.to_owned())),
        &mut probe,
        senders.name_lookup(),
        false,
    );
    channel_sink(
        &addr_lookup.as_collection(|&key, addr| (key, addr.clone())),
        &mut probe,
        senders.addr_lookup(),
        false,
    );
    channel_sink(&program_stats, &mut probe, senders.program_stats(), true);
    channel_sink(
        &worker_stats
            .map(|(worker, stats)| ((), (worker, stats)))
            .hierarchical_sort_by(|&(worker, _)| worker)
            .map(|((), sorted_stats)| sorted_stats),
        &mut probe,
        senders.worker_stats(),
        true,
    );
    channel_sink(
        &addressed_operators.semijoin(&leaves),
        &mut probe,
        senders.nodes(),
        true,
    );
    channel_sink(
        &addressed_operators.semijoin(&subgraphs),
        &mut probe,
        senders.subgraphs(),
        true,
    );
    channel_sink(&edges, &mut probe, senders.edges(), true);
    channel_sink(&operator_stats, &mut probe, senders.operator_stats(), true);
    channel_sink(
        &cross_aggregated_operator_stats,
        &mut probe,
        senders.aggregated_operator_stats(),
        true,
    );
    channel_sink(
        &worker_timeline,
        &mut probe,
        senders.timeline_events(),
        true,
    );
    channel_sink(&dataflow_stats, &mut probe, senders.dataflow_stats(), true);

    // TODO: Save ddflow logs
    // TODO: Probably want to prefix things with the current system time to allow
    //       "appending" logs by simply running ddshow at a later time and replaying
    //       log files in order of timestamp
    // TODO: For pause/resume profiling/debugging we'll probably need a custom log
    //       hook within timely, we can make it serve us rkyv events while we're at it
    // If saving logs is enabled, write all log messages to the `save_logs` directory
    if let Some(save_logs) = args.save_logs.as_ref() {
        fs::create_dir_all(&save_logs).context("failed to create `--save-logs` directory")?;

        let file = if scope.index() == 0 {
            save_logs.join("timely.ddshow")

        // Make other workers pipe to a null output
        } else if cfg!(windows) {
            PathBuf::from("nul")
        } else {
            PathBuf::from("/dev/null")
        };

        tracing::debug!(
            "installing file sink to {} on worker {}",
            file.display(),
            scope.index(),
        );

        let timely_file = BufWriter::new(
            File::create(file).context("failed to create `--save-logs` timely file")?,
        );

        timely_stream
            .exchange(|_| 0)
            .probe_with(&mut probe)
            .capture_into(RkyvEventWriter::new(timely_file));
    }

    Ok(probe)
}

fn dataflow_stats<S, Tr1, Tr2, Tr3, Tr4>(
    timely_stream: &Stream<S, TimelyLogBundle>,
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
    // Collect all operator creation & drop times and make lifespans from the
    // times they occur
    let lifespans = timely_stream
        .unary(Pipeline, "Dataflow Lifespan", |_capability, _info| {
            let mut lifespan_map = HashMap::new();
            let mut buffer = Vec::new();

            move |input, output| {
                input.for_each(|capability, data| {
                    let capability = capability.retain();
                    data.swap(&mut buffer);

                    for (time, worker, event) in buffer.drain(..) {
                        match event {
                            RkyvTimelyEvent::Operates(operates) => {
                                lifespan_map
                                    .insert((worker, operates.id), (time, capability.clone()));
                            }

                            RkyvTimelyEvent::Shutdown(shutdown) => {
                                if let Some((stored_time, mut stored_capability)) =
                                    lifespan_map.remove(&(worker, shutdown.id))
                                {
                                    stored_capability.downgrade(
                                        &stored_capability.time().join(capability.time()),
                                    );

                                    output.session(&stored_capability).give((
                                        ((worker, shutdown.id), Lifespan::new(stored_time, time)),
                                        time,
                                        1,
                                    ));
                                }
                            }

                            RkyvTimelyEvent::Channels(_)
                            | RkyvTimelyEvent::PushProgress(_)
                            | RkyvTimelyEvent::Messages(_)
                            | RkyvTimelyEvent::Schedule(_)
                            | RkyvTimelyEvent::Application(_)
                            | RkyvTimelyEvent::GuardedMessage(_)
                            | RkyvTimelyEvent::GuardedProgress(_)
                            | RkyvTimelyEvent::CommChannels(_)
                            | RkyvTimelyEvent::Input(_)
                            | RkyvTimelyEvent::Park(_)
                            | RkyvTimelyEvent::Text(_) => {}
                        }
                    }
                });
            }
        })
        .as_collection();

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
        .join(&lifespans)
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

fn operator_creations<S>(
    log_stream: &Stream<S, TimelyLogBundle>,
) -> Collection<S, (WorkerId, RkyvOperatesEvent), Diff>
where
    S: Scope<Timestamp = Time>,
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
    S: Scope<Timestamp = Time>,
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

type LeavesAndScopes<S, R> = (
    Collection<S, (WorkerId, OperatorAddr), R>,
    Collection<S, (WorkerId, OperatorAddr), R>,
);

fn sift_leaves_and_scopes<S, R>(
    scope: &mut S,
    operators: &Collection<S, (WorkerId, RkyvOperatesEvent), R>,
) -> LeavesAndScopes<S, R>
where
    S: Scope,
    S::Timestamp: Lattice + TotalOrder,
    R: Semigroup + Monoid + Abelian + ExchangeData + Multiply<isize, Output = R>,
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
        let observed_subgraphs = operators.semijoin(&potential_scopes).map(|(addr, ())| addr);

        (leaf_operators, observed_subgraphs.leave_region())
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
