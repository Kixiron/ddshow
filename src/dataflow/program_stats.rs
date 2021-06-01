use crate::{
    dataflow::{
        granulate,
        operators::{DiffDuration, JoinArranged, Max, Min},
        send_recv::ChannelAddrs,
        Channel, Diff, DifferentialLogBundle, OperatorAddr, TimelyLogBundle,
    },
    ui::{ProgramStats, WorkerStats},
};
use ddshow_types::{
    differential_logging::DifferentialEvent, timely_logging::OperatesEvent, WorkerId,
};
use differential_dataflow::{
    difference::DiffPair,
    operators::{arrange::ArrangeByKey, Count, CountTotal, Join, Reduce, ThresholdTotal},
    AsCollection, Collection, Data,
};
use std::{iter, time::Duration};
use timely::dataflow::{
    operators::{Concat, Map},
    Scope, Stream,
};

fn combine_events<S, D, TF, TD>(
    timely: &Stream<S, TimelyLogBundle>,
    map_timely: TF,
    differential: Option<&Stream<S, DifferentialLogBundle>>,
    map_differential: TD,
) -> Stream<S, D>
where
    S: Scope<Timestamp = Duration>,
    D: Data,
    TF: Fn(TimelyLogBundle) -> D + 'static,
    TD: Fn(DifferentialLogBundle) -> D + 'static,
{
    let mut events = timely.map(map_timely);
    if let Some(differential) = differential {
        events = events.concat(&differential.map(map_differential));
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
    operators: &Collection<S, (WorkerId, OperatesEvent), Diff>,
    channels: &Collection<S, (WorkerId, Channel), Diff>,
    subgraph_addresses: &ChannelAddrs<S, Diff>,
) -> AggregatedStats<S>
where
    S: Scope<Timestamp = Duration>,
{
    let addressed_operators = operators
        .map(|(worker, operator)| ((worker, operator.addr), ()))
        .arrange_by_key();

    #[allow(clippy::suspicious_map)]
    let total_operators = addressed_operators
        .antijoin_arranged(subgraph_addresses)
        .map(|((worker, _), _)| worker)
        .count();

    let only_subgraphs = addressed_operators
        .semijoin_arranged(subgraph_addresses)
        .map(|((worker, addr), _)| (worker, addr));

    let only_dataflows = only_subgraphs.filter(|(_, addr)| addr.len() == 1);

    #[allow(clippy::suspicious_map)]
    let total_dataflows = only_dataflows.map(|(worker, _)| worker).count();

    let dataflow_addrs = only_dataflows.reduce(|_worker, input, output| {
        let dataflows: Vec<OperatorAddr> = input
            .iter()
            .filter_map(|&(addr, diff)| if diff >= 1 { Some(addr.clone()) } else { None })
            .collect();

        output.push((dataflows, 1));
    });

    #[allow(clippy::suspicious_map)]
    let total_subgraphs = only_subgraphs.map(|(worker, _)| worker).count();

    #[allow(clippy::suspicious_map)]
    let total_channels = channels.map(|(worker, _)| worker).count();

    let mut total_arrangements = if let Some(differential) = differential {
        differential
            .map(|(time, worker, event)| {
                let operator = match event {
                    DifferentialEvent::Batch(batch) => batch.operator,
                    DifferentialEvent::Merge(merge) => merge.operator,
                    DifferentialEvent::Drop(drop) => drop.operator,
                    DifferentialEvent::MergeShortfall(merge) => merge.operator,
                    DifferentialEvent::TraceShare(share) => share.operator,
                };

                (((worker, operator), ()), time, 1)
            })
            .as_collection()
            .distinct_total()
            .map(|((worker, _operator), ())| (worker, ()))
            .count_total()
            .map(|((worker, ()), arrangements)| (worker, arrangements))

    // If we don't have access to differential events just make every worker have zero
    // arrangements
    } else {
        total_channels.map(|(worker, _)| (worker, 0))
    };

    // Add back any workers that didn't contain any operators
    total_arrangements = total_arrangements.concat(
        &total_arrangements
            .antijoin(&total_channels.map(|(worker, _)| worker))
            .map(|(worker, _)| (worker, 0)),
    );

    let total_events = combine_events(
        timely,
        |(time, worker, _)| (worker, time, 1isize),
        differential,
        |(time, worker, _)| (worker, time, 1),
    )
    .as_collection()
    .delay(granulate)
    .count();

    let create_timestamps = |time, worker| {
        let diff = DiffPair::new(
            Max::new(DiffDuration::new(time)),
            Min::new(DiffDuration::new(time)),
        );

        (worker, time, diff)
    };

    let total_runtime = combine_events(
        timely,
        move |(time, worker, _)| create_timestamps(time, worker),
        differential,
        move |(time, worker, _)| create_timestamps(time, worker),
    )
    .as_collection()
    .delay(granulate)
    .count();

    // TODO: For whatever reason this part of the dataflow graph is de-prioritized,
    //       probably because of data dependence. Due to the nature of this data, for
    //       realtime streaming I'd like it to be the first thing being spat out over
    //       the network so that the user gets instant feedback. In order to do this
    //       I think it'll take some mucking about with antijoins (or maybe some clever
    //       stream default values?) to make every field in `ProgramStats` optional
    //       so that as soon as we have any data we can chuck it at them, even if it's
    //       incomplete
    let worker_stats = dataflow_addrs
        .join(&total_dataflows)
        .join(&total_operators)
        .join(&total_subgraphs)
        .join(&total_channels)
        .join(&total_arrangements)
        .join(&total_events)
        .join(&total_runtime)
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
                let runtime = runtime.element1.value.0 - runtime.element2.value.0;

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
        );

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
            .count()
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
                                                                            Max {
                                                                                value:
                                                                                    DiffDuration(
                                                                                        runtime,
                                                                                    ),
                                                                            },
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
                    runtime,
                },
            );

    (program_stats, worker_stats)
}
