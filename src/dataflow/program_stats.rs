use crate::{
    dataflow::{
        operators::{DelayExt, DiffDuration, InspectExt, JoinArranged, MapExt, MapTimed, Max, Min},
        send_recv::ChannelAddrs,
        utils::{granulate, ArrangedKey, DifferentialLogBundle, Time, TimelyLogBundle},
        Channel, Diff, OperatorAddr,
    },
    ui::{ProgramStats, WorkerStats},
};
use ddshow_types::{differential_logging::DifferentialEvent, WorkerId};
use differential_dataflow::{
    difference::DiffPair,
    operators::{CountTotal, Join, Reduce, ThresholdTotal},
    AsCollection, Collection, Data,
};
use std::iter;
use timely::dataflow::{operators::Concat, Scope, Stream};

fn combine_events<S, D, TF, TD>(
    timely: &Stream<S, TimelyLogBundle>,
    map_timely: TF,
    differential: Option<&Stream<S, DifferentialLogBundle>>,
    map_differential: TD,
) -> Stream<S, D>
where
    S: Scope<Timestamp = Time>,
    D: Data,
    TF: Fn(&Time, TimelyLogBundle) -> D + 'static,
    TD: Fn(&Time, DifferentialLogBundle) -> D + 'static,
{
    let mut events = timely.map_timed(map_timely);
    if let Some(differential) = differential {
        events = events.concat(&differential.map_timed(map_differential));
    }

    events
}

type AggregatedStats<S> = (
    Collection<S, ProgramStats, Diff>,
    Collection<S, (WorkerId, WorkerStats), Diff>,
);

pub fn aggregate_worker_stats<S>(
    timely: &Stream<S, TimelyLogBundle>,
    differential: Option<&Stream<S, DifferentialLogBundle>>,
    channels: &Collection<S, (WorkerId, Channel), Diff>,
    subgraph_addresses: &ChannelAddrs<S, Diff>,
    operator_addrs_by_self: &ArrangedKey<S, (WorkerId, OperatorAddr), Diff>,
) -> AggregatedStats<S>
where
    S: Scope<Timestamp = Time>,
{
    let only_operators = operator_addrs_by_self.antijoin_arranged(subgraph_addresses);
    let only_subgraphs = operator_addrs_by_self
        .semijoin_arranged(subgraph_addresses)
        .map_named("Map: Select Subgraphs", |((worker, addr), _)| {
            (worker, addr)
        })
        .debug_frontier();
    let only_dataflows = only_subgraphs
        .filter(|(_, addr)| addr.is_top_level())
        .debug_frontier();

    // Collect all dataflow addresses into a single vec
    let dataflow_addrs = only_dataflows
        .reduce_named(
            "Reduce: Collect All Dataflow Addresses",
            |_worker, input, output| {
                let dataflows: Vec<OperatorAddr> = input
                    .iter()
                    .filter_map(|&(addr, diff)| if diff >= 1 { Some(addr.clone()) } else { None })
                    .collect();

                output.push((dataflows, 1));
            },
        )
        .debug_frontier();

    // Count the total number of each item
    let total_dataflows = only_dataflows
        .map_named("Map: Count Dataflows", |(worker, _)| worker)
        .count_total()
        .debug_frontier();
    let total_subgraphs = only_subgraphs
        .map_named("Map: Count Subgraphs", |(worker, _)| worker)
        .count_total()
        .debug_frontier();
    let total_channels = channels
        .map_named("Map: Count Channels", |(worker, _)| worker)
        .count_total()
        .debug_frontier();
    let total_operators = only_operators
        .map_named("Map: Count Operators", |((worker, _), _)| worker)
        .count_total()
        .debug_frontier();

    let mut total_arrangements = if let Some(differential) = differential {
        differential
            .map_timed(|&time, (_event_time, worker, event)| {
                let operator = match event {
                    DifferentialEvent::Batch(batch) => batch.operator,
                    DifferentialEvent::Merge(merge) => merge.operator,
                    DifferentialEvent::Drop(drop) => drop.operator,
                    DifferentialEvent::MergeShortfall(merge) => merge.operator,
                    DifferentialEvent::TraceShare(share) => share.operator,
                };

                (((worker, operator), ()), time, 1)
            })
            .debug_frontier_with("total_arrangements differential map_timed")
            .as_collection()
            .distinct_total_core::<Diff>()
            .debug_frontier_with("total_arrangements distinct")
            .map_named("Map: Count Arrangements", |((worker, _operator), ())| {
                worker
            })
            .debug_frontier_with("total_arrangements mapped")
            .count_total()
            .debug_frontier_with("total_arrangements counted")

    // If we don't have access to differential events just make every worker have zero
    // arrangements
    } else {
        total_channels
            .map_named("Map: Give Each Worker Zero Dataflows", |(worker, _)| {
                (worker, 0)
            })
            .debug_frontier_with("total_arrangements no differential")
    };

    // Add back any workers that didn't contain any arrangements
    total_arrangements = total_arrangements
        .concat(
            &total_arrangements
                .antijoin(&total_channels.map(|(worker, _)| worker))
                .map(|(worker, _)| (worker, 0)),
        )
        .debug_frontier_with("total_arrangements re-added");

    let total_events = combine_events(
        &timely.debug_frontier_with("timely into total_events"),
        |&time, (_, worker, _)| (worker, time, 1isize),
        differential
            .map(|stream| stream.debug_frontier_with("differential into total_events"))
            .as_ref(),
        |&time, (_, worker, _)| (worker, time, 1),
    )
    .debug_frontier_with("total_events raw")
    .as_collection()
    .delay_fast(granulate)
    .debug_frontier_with("total_events after delay")
    .count_total()
    .debug_frontier_with("total_events after count");

    let create_timestamps = |time| {
        DiffPair::new(
            Max::new(DiffDuration::new(time)),
            Min::new(DiffDuration::new(time)),
        )
    };

    let total_runtime = combine_events(
        &timely.debug_frontier_with("timely into total_runtime"),
        move |&time, (event_time, worker, _)| (worker, time, create_timestamps(event_time)),
        differential
            .map(|stream| stream.debug_frontier_with("differential into total_runtime"))
            .as_ref(),
        move |&time, (event_time, worker, _)| (worker, time, create_timestamps(event_time)),
    )
    .debug_frontier_with("total_runtime raw")
    .as_collection()
    .delay_fast(granulate)
    .debug_frontier_with("total_runtime after delay")
    .count_total()
    .debug_frontier_with("total_runtime after count");

    // TODO: For whatever reason this part of the dataflow graph is de-prioritized,
    //       probably because of data dependence. Due to the nature of this data, for
    //       realtime streaming I'd like it to be the first thing being spat out over
    //       the network so that the user gets instant feedback. In order to do this
    //       I think it'll take some mucking about with antijoins (or maybe some clever
    //       stream default values?) to make every field in `ProgramStats` optional
    //       so that as soon as we have any data we can chuck it at them, even if it's
    //       incomplete
    // TODO: This really should be a delta join :(
    // TODO: This may actually be feasibly hoisted into the difference type or something?
    let worker_stats = dataflow_addrs
        .debug_frontier_with("dataflow_addrs")
        .join(&total_dataflows.debug_frontier_with("total_dataflows"))
        .join(&total_operators.debug_frontier_with("total_operators"))
        .join(&total_subgraphs.debug_frontier_with("total_subgraphs"))
        .join(&total_channels.debug_frontier_with("total_channels"))
        .join(&total_arrangements.debug_frontier_with("total_arrangements"))
        .join(&total_events.debug_frontier_with("total_events"))
        .join(&total_runtime.debug_frontier_with("total_runtime"))
        .map(
            |(
                worker,
                (
                    (
                        (
                            ((((dataflow_addrs, dataflows), operators), subgraphs), channels),
                            arrangements,
                        ),
                        events,
                    ),
                    runtime,
                ),
            )| {
                let runtime =
                    runtime.element1.value.to_duration() - runtime.element2.value.to_duration();

                (
                    worker,
                    WorkerStats {
                        id: worker,
                        dataflows: dataflows as usize,
                        operators: operators as usize,
                        subgraphs: subgraphs as usize,
                        channels: channels as usize,
                        arrangements: arrangements as usize,
                        events: events as usize,
                        runtime,
                        dataflow_addrs,
                    },
                )
            },
        )
        .debug_frontier_with("worker_stats");

    let program_stats =
        worker_stats
            .explode(|(_, stats)| {
                let diff = DiffPair::new(
                    1,
                    DiffPair::new(
                        stats.dataflows as isize,
                        DiffPair::new(
                            stats.operators as isize,
                            DiffPair::new(
                                stats.subgraphs as isize,
                                DiffPair::new(
                                    stats.channels as isize,
                                    DiffPair::new(
                                        stats.arrangements as isize,
                                        DiffPair::new(
                                            stats.events as isize,
                                            Max::new(DiffDuration::new(stats.runtime)),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                );

                iter::once(((), diff))
            })
            .debug_frontier_with("worker_stats exploded (program_stats)")
            .count_total()
            .debug_frontier_with("worker_stats after count (program_stats)")
            .map(
                |(
                    (),
                    DiffPair {
                        element1: workers,
                        element2:
                            DiffPair {
                                element1: dataflows,
                                element2:
                                    DiffPair {
                                        element1: operators,
                                        element2:
                                            DiffPair {
                                                element1: subgraphs,
                                                element2:
                                                    DiffPair {
                                                        element1: channels,
                                                        element2:
                                                            DiffPair {
                                                                element1: arrangements,
                                                                element2:
                                                                    DiffPair {
                                                                        element1: events,
                                                                        element2:
                                                                            Max { value: runtime },
                                                                    },
                                                            },
                                                    },
                                            },
                                    },
                            },
                    },
                )| ProgramStats {
                    workers: workers as usize,
                    dataflows: dataflows as usize,
                    operators: operators as usize,
                    subgraphs: subgraphs as usize,
                    channels: channels as usize,
                    arrangements: arrangements as usize,
                    events: events as usize,
                    runtime: runtime.to_duration(),
                },
            )
            .debug_frontier_with("program_stats final");

    (program_stats, worker_stats)
}
