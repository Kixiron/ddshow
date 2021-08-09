// mod channel_stats;
#[macro_use]
pub mod operators;
pub(crate) mod constants;
mod differential;
mod operator_stats;
mod program_stats;
mod progress_stats;
#[cfg(feature = "timely-next")]
mod reachability;
mod send_recv;
mod shape;
mod subgraphs;
mod summation;
pub(crate) mod utils;
// FIXME: Fix the tests
// mod tests;
mod timely_source;
mod worker;
mod worker_timeline;

pub use constants::PROGRAM_NS_GRANULARITY;
pub use differential::{ArrangementStats, SplineLevel};
pub use operator_stats::OperatorStats;
pub use progress_stats::{Channel, OperatorProgress, ProgressInfo};
pub use send_recv::{DataflowData, DataflowExtractor, DataflowReceivers, DataflowSenders};
pub use shape::OperatorShape;
pub use summation::Summation;
pub use worker::worker_runtime;
pub use worker_timeline::{EventKind, TimelineEvent};

use crate::{
    args::Args,
    dataflow::{
        operator_stats::OperatorStatsRelations,
        operators::{FilterMap, JoinArranged},
        program_stats::GraphStats,
        send_recv::ChannelAddrs,
        subgraphs::rewire_channels,
        timely_source::TimelyCollections,
        utils::{
            ArrangedKey, ArrangedVal, Diff, DifferentialLogBundle, OpKey, ProgressLogBundle, Time,
            TimelyLogBundle,
        },
    },
    ui::{DataflowStats, Lifespan},
};
use anyhow::Result;
use ddshow_types::{timely_logging::OperatesEvent, ChannelId, OperatorAddr, OperatorId, WorkerId};
use differential_dataflow::{
    operators::{
        arrange::{Arrange, ArrangeByKey, ArrangeBySelf},
        CountTotal, Join, JoinCore, ThresholdTotal,
    },
    AsCollection, Collection,
};
use std::{iter, time::Duration};
use timely::dataflow::{
    operators::{generic::operator, probe::Handle as ProbeHandle},
    Scope, Stream,
};

// TODO: Dataflow lints
//  - Inconsistent dataflows across workers
//  - Not arranging before a loop feedback
//  - you aren't supposed to be able to forge capabilities,
//    but you can take an incoming CapabilityRef and turn
//    it in to a Capability for any output, even those that
//    the input should not be connected to via the summary.
//  - Packing `(data, time, diff)` updates in DD where time
//    is not greater or equal to the message capability.
// TODO: Timely progress logging
// TODO: The PDG
// TODO: Timely reachability logging

pub fn dataflow<S>(
    scope: &mut S,
    args: &Args,
    master_probe: &mut ProbeHandle<Time>,
    timely_stream: &Stream<S, TimelyLogBundle>,
    differential_stream: Option<&Stream<S, DifferentialLogBundle>>,
    _progress_stream: Option<&Stream<S, ProgressLogBundle>>,
    senders: DataflowSenders,
) -> Result<Vec<(ProbeHandle<Time>, &'static str)>>
where
    S: Scope<Timestamp = Time>,
{
    let TimelyCollections {
        lifespans,
        activations,
        raw_channel_events,
        raw_operator_events,
        operator_names,
        operator_ids_to_addrs,
        operator_addrs_to_ids,
        operator_addrs,
        channel_scope_addrs,
        dataflow_ids,
        timeline_events,
        ..
    } = timely_source::extract_timely_info(scope, timely_stream, args.disable_timeline);

    let OperatorStatsRelations {
        summarized,
        aggregated_summaries,
        arrangements,
        aggregated_arrangements,
        spline_levels,
    } = operator_stats::operator_stats(scope, &activations, differential_stream);

    // FIXME: This is pretty much a guess since there's no way to actually associate
    //        operators/arrangements/channels across workers
    // TODO: This should use a specialized struct to hold relevant things like "total size across workers"
    //       in addition to per-worker stats
    // let aggregated_operator_stats = operator_stats::aggregate_operator_stats(&operator_stats);

    // TODO: Turn these into collections of `OpKey` and arrange them
    let (leaves, subgraphs) = sift_leaves_and_scopes(scope, &operator_addrs);
    let (leaves_arranged, subgraphs_arranged) = (
        leaves.arrange_by_self_named("ArrangeBySelf: Dataflow Graph Leaves"),
        subgraphs.arrange_by_self_named("ArrangeBySelf: Dataflow Graph Subgraphs"),
    );

    let subgraph_ids = subgraphs_arranged
        .join_map(
            &operator_addrs_to_ids
                .as_collection(|&(worker, ref addr), &id| (addr.clone(), (id, worker))),
            |_, &(), &(id, worker)| ((worker, id), ()),
        )
        .arrange_named("Arrange: Dataflow Graph Subgraph Ids");

    let channels = rewire_channels(scope, &raw_channel_events, &subgraphs_arranged);
    let edges = attach_operators(scope, &raw_operator_events, &channels, &leaves_arranged);

    let operator_shapes = shape::operator_shapes(&raw_operator_events, &raw_channel_events);
    // let operator_progress = progress_stream.map(|progress_stream| {
    //     progress_stats::aggregate_channel_messages(progress_stream, &operator_shapes)
    // });

    // TODO: Make `extract_timely_info()` get the relevant event information
    // TODO: Grabbing events absolutely shits the bed when it comes to large dataflows,
    //       it needs a serious, intrinsic rework and/or disk backed arrangements
    let timeline_events = timeline_events.as_ref().map(|timeline_events| {
        worker_timeline::worker_timeline(scope, timeline_events, differential_stream)
    });

    let addressed_operators = raw_operator_events
        .map(|operator| (operator.addr.clone(), operator))
        .arrange_by_key_named("ArrangeByKey: Addressed Operators");

    let GraphStats {
        workers,
        operators,
        dataflows,
        channels,
        arrangements: arrangement_ids,
        total_runtime,
    } = program_stats::aggregate_program_stats(
        timely_stream,
        differential_stream,
        &channels,
        &subgraphs_arranged,
        &operator_addrs,
    );

    let dataflow_stats = dataflow_stats(
        &lifespans,
        &dataflow_ids,
        &operator_ids_to_addrs,
        &subgraph_ids,
        &channel_scope_addrs,
    );

    let mut probes = install_data_extraction(
        scope,
        master_probe,
        senders,
        workers,
        operators,
        dataflows,
        channels,
        arrangement_ids,
        total_runtime,
        leaves_arranged,
        edges,
        subgraphs_arranged,
        addressed_operators,
        dataflow_stats,
        timeline_events,
        operator_names,
        operator_ids_to_addrs,
        &operator_shapes,
        None,
        activations,
        summarized,
        aggregated_summaries,
        arrangements,
        aggregated_arrangements,
        spline_levels,
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

        let mut probe = ProbeHandle::new();
        utils::logging_event_sink(
            save_logs,
            scope,
            timely_stream,
            &mut probe,
            differential_stream,
        )?;

        probes.push((probe, "log_event_sink"));
    }

    Ok(probes)
}

#[allow(clippy::too_many_arguments, clippy::type_complexity)]
fn install_data_extraction<S>(
    scope: &mut S,
    probe: &mut ProbeHandle<Time>,
    senders: DataflowSenders,
    workers: Collection<S, WorkerId, Diff>,
    operators: Collection<S, OperatorAddr, Diff>,
    dataflows: Collection<S, OperatorAddr, Diff>,
    channels: Collection<S, ChannelId, Diff>,
    arrangement_ids: Option<Collection<S, (WorkerId, OperatorId), Diff>>,
    total_runtime: Collection<S, (WorkerId, (Duration, Duration)), Diff>,
    nodes: ArrangedKey<S, OperatorAddr, Diff>,
    edges: Collection<S, (OperatesEvent, Channel, OperatesEvent), Diff>,
    subgraphs: ArrangedKey<S, OperatorAddr, Diff>,
    addressed_operators: ArrangedVal<S, OperatorAddr, OperatesEvent, Diff>,
    dataflow_stats: Collection<S, DataflowStats, Diff>,
    timeline_events: Option<Collection<S, TimelineEvent, Diff>>,
    operator_names: ArrangedVal<S, OpKey, String, Diff>,
    operator_ids: ArrangedVal<S, OpKey, OperatorAddr, Diff>,
    operator_shapes: &Collection<S, OperatorShape, Diff>,
    operator_progress: Option<&Collection<S, OperatorProgress, Diff>>,
    operator_activations: Collection<S, (OpKey, (Duration, Duration)), Diff>,
    summarized: Collection<S, (OpKey, Summation), Diff>,
    aggregated_summaries: Collection<S, (OperatorId, Summation), Diff>,
    arrangements: Option<Collection<S, (OpKey, ArrangementStats), Diff>>,
    aggregated_arrangements: Option<Collection<S, (OperatorId, ArrangementStats), Diff>>,
    spline_levels: Option<Collection<S, (OpKey, SplineLevel), Diff>>,
) -> Vec<(ProbeHandle<Time>, &'static str)>
where
    S: Scope<Timestamp = Time>,
{
    scope.region_named("Data Extraction", |region| {
        let workers = workers.enter_region(region);
        let operators = operators.enter_region(region);
        let dataflows = dataflows.enter_region(region);
        let channels = channels.enter_region(region);
        let arrangement_ids = arrangement_ids
            .map(|ids| ids.enter_region(region))
            .unwrap_or_else(|| operator::empty(region).as_collection());
        let total_runtime = total_runtime.enter_region(region);
        let nodes = nodes.enter_region(region);
        let edges = edges.enter_region(region);
        let subgraphs = subgraphs.enter_region(region);
        let addressed_operators = addressed_operators.enter_region(region);
        let dataflow_stats = dataflow_stats.enter_region(region);
        let timeline_events = timeline_events
            .map(|events| events.enter_region(region))
            .unwrap_or_else(|| operator::empty(region).as_collection());
        let operator_names = operator_names
            .enter_region(region)
            .as_collection(|&(worker, operator), name| ((worker, operator), name.to_owned()));
        let operator_ids = operator_ids
            .enter_region(region)
            .as_collection(|&key, addr| (key, addr.clone()));
        let operator_shapes = operator_shapes.enter_region(region);
        let operator_progress = operator_progress
            .map(|progress| progress.enter_region(region))
            .unwrap_or_else(|| operator::empty(region).as_collection());
        let operator_activations = operator_activations.enter_region(region);
        let summarized = summarized.enter_region(region);
        let aggregated_summaries = aggregated_summaries.enter_region(region);
        let arrangements = arrangements
            .map(|arrangements| arrangements.enter_region(region))
            .unwrap_or_else(|| operator::empty(region).as_collection());
        let aggregated_arrangements = aggregated_arrangements
            .map(|aggregated| aggregated.enter_region(region))
            .unwrap_or_else(|| operator::empty(region).as_collection());
        let spline_levels = spline_levels
            .map(|splines| splines.enter_region(region))
            .unwrap_or_else(|| operator::empty(region).as_collection());

        let nodes = addressed_operators.semijoin_arranged(&nodes);
        let subgraphs = addressed_operators.semijoin_arranged(&subgraphs);

        // TODO: Since we pseudo-consolidate on the receiver side we may
        //       not actually need to maintain arrangements here,
        //       donno how that will interact with the eventual "live
        //       ddshow" system though, could make some funky results
        //       appear to the user
        senders.install_sinks(
            probe,
            (&workers, false),
            (&operators, false),
            (&dataflows, false),
            (&channels, false),
            (&arrangement_ids, false),
            (&total_runtime, false),
            (&nodes, false),
            (&edges, false),
            (&subgraphs, false),
            (&dataflow_stats, false),
            (&timeline_events, false),
            (&operator_names, false),
            (&operator_ids, false),
            (&operator_shapes, false),
            (&operator_progress, false),
            (&operator_activations, false),
            (&summarized, false),
            (&aggregated_summaries, false),
            (&arrangements, false),
            (&aggregated_arrangements, false),
            (&spline_levels, false),
        )
    })
}

fn dataflow_stats<S>(
    operator_lifespans: &Collection<S, (OpKey, Lifespan), Diff>,
    dataflow_ids: &ArrangedKey<S, OpKey>,
    addr_lookup: &ArrangedVal<S, OpKey, OperatorAddr>,
    subgraph_ids: &ArrangedKey<S, OpKey>,
    channel_scopes: &ArrangedVal<S, (WorkerId, ChannelId), OperatorAddr>,
) -> Collection<S, DataflowStats, Diff>
where
    S: Scope<Timestamp = Time>,
{
    let subgraph_addrs = subgraph_ids.join_core(addr_lookup, |&(worker, id), &(), addr| {
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
        .semijoin_arranged(subgraph_ids)
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

    // TODO: Delta join this :(
    dataflows
        .join(operator_lifespans)
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
    Collection<S, OperatorAddr, R>,
    Collection<S, OperatorAddr, R>,
);

fn sift_leaves_and_scopes<S>(
    scope: &mut S,
    operator_addrs: &ArrangedKey<S, OperatorAddr>,
) -> LeavesAndScopes<S, Diff>
where
    S: Scope<Timestamp = Time>,
{
    scope.region_named("Sift Leaves and Scopes", |region| {
        let operator_addrs = operator_addrs.enter_region(region);

        // The addresses of potential scopes, excluding leaf operators
        let potential_scopes = operator_addrs
            .flat_map_ref(|addr, &()| {
                let mut addr = addr.clone();
                addr.pop();

                iter::once(addr)
            })
            .distinct_total_core::<Diff>()
            .arrange_by_self_named("ArrangeBySelf: Potential Scopes");

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

fn attach_operators<S>(
    scope: &mut S,
    operators: &Collection<S, OperatesEvent, Diff>,
    channels: &Collection<S, Channel, Diff>,
    leaves: &ChannelAddrs<S, Diff>,
) -> Collection<S, (OperatesEvent, Channel, OperatesEvent), Diff>
where
    S: Scope<Timestamp = Time>,
{
    // TODO: Make `Graph` nested so that subgraphs contain a `Vec<Graph>` of all children
    scope.region_named("Attach Operators to Channels", |region| {
        let (operators, channels, leaves) = (
            operators.enter_region(region),
            channels.enter_region(region),
            leaves.enter_region(region),
        );

        let operators_by_address = operators
            .map(|operator| ((operator.addr.clone()), operator))
            .arrange_by_key_named("ArrangeByKey: Operators by Address");

        operators_by_address
            .semijoin_arranged(&leaves)
            .join_map(
                &channels.map(|channel| (channel.source_addr().to_owned(), channel)),
                |_src_addr, src_operator, channel| {
                    (
                        (channel.target_addr().to_owned()),
                        (src_operator.clone(), channel.clone()),
                    )
                },
            )
            .join_core(
                &operators_by_address,
                |_target_addr, (src_operator, channel), target_operator| {
                    iter::once((
                        src_operator.clone(),
                        channel.clone(),
                        target_operator.clone(),
                    ))
                },
            )
            .leave_region()
    })
}
