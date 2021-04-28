use crate::{
    dataflow::{
        operators::{DiffDuration, JoinArranged, Max, Min},
        types::WorkerId,
        Channel, ChannelAddrs, Diff, DifferentialLogBundle, OperatesEvent, OperatorAddr,
        TimelyLogBundle, PROGRAM_NS_GRANULARITY,
    },
    ui::{ProgramStats, WorkerStats},
};
use differential_dataflow::{
    difference::DiffPair,
    lattice::Lattice,
    operators::{arrange::ArrangeByKey, Consolidate, Count, Join, Reduce},
    AsCollection, Collection, Data, ExchangeData,
};
use std::{collections::HashSet, hash::Hash, time::Duration};
use timely::dataflow::{
    operators::{Concat, Map},
    Scope, Stream,
};

fn granulate(time: Duration) -> Duration {
    let timestamp = time.as_nanos();
    let window_idx = (timestamp / PROGRAM_NS_GRANULARITY) + 1;

    Duration::from_nanos((window_idx * PROGRAM_NS_GRANULARITY) as u64)
}

pub fn aggregate_program_stats<S>(
    timely: &Stream<S, TimelyLogBundle>,
    differential: Option<&Stream<S, DifferentialLogBundle>>,
    operators: &Collection<S, (WorkerId, OperatesEvent), Diff>,
    channels: &Collection<S, (WorkerId, Channel), Diff>,
    subgraph_addresses: &ChannelAddrs<S, Diff>,
) -> Collection<S, ProgramStats, Diff>
where
    S: Scope<Timestamp = Duration>,
{
    let mut workers = timely.map(|(time, worker, _)| (((), worker), time, 1isize));
    if let Some(differential) = differential {
        workers = workers.concat(&differential.map(|(time, worker, _)| (((), worker), time, 1)));
    }

    let total_workers = count_unique(&workers.as_collection());

    // TODO: Arrange this
    let addressed_operators =
        operators.map(|(worker, operator)| ((worker, operator.addr.clone()), operator));

    let only_subgraphs = addressed_operators.semijoin_arranged(subgraph_addresses);

    // FIXME: Uber inefficient, these three
    //        could have their early stages combined
    #[allow(clippy::suspicious_map)]
    let total_operators = addressed_operators
        .antijoin_arranged(subgraph_addresses)
        .map(|_| ())
        .count();

    #[allow(clippy::suspicious_map)]
    let total_dataflows = only_subgraphs
        .filter(|((_, addr), _)| addr.len() == 1)
        .map(|_| ())
        .count();

    #[allow(clippy::suspicious_map)]
    let total_subgraphs = only_subgraphs.map(|_| ()).count();

    #[allow(clippy::suspicious_map)]
    let total_channels = channels.map(|_| ()).count();

    let mut events = timely.map(|(time, _, _)| ((), time, 1isize));
    if let Some(differential) = differential {
        events = events.concat(&differential.map(|(time, _, _)| ((), time, 1)));
    }
    let total_events = events
        // .delay_batch(|&time| granulate(time))
        .as_collection()
        .count();

    // TODO: Dear god this updates so much, figure out the ddflow
    //       delay thingy and make it not like that
    let mut event_timestamps = timely.map(|(time, _, _)| {
        (
            (),
            time,
            DiffPair::new(
                Max::new(DiffDuration::new(time)),
                Min::new(DiffDuration::new(time)),
            ),
        )
    });
    if let Some(differential) = differential {
        event_timestamps = event_timestamps.concat(&differential.map(|(time, _, _)| {
            (
                (),
                time,
                DiffPair::new(
                    Max::new(DiffDuration::new(time)),
                    Min::new(DiffDuration::new(time)),
                ),
            )
        }));
    }

    let total_runtime = event_timestamps.as_collection().count();

    // TODO: For whatever reason this part of the dataflow graph is de-prioritized,
    //       probably because of data dependence. Due to the nature of this data, for
    //       realtime streaming I'd like it to be the first thing being spat out over
    //       the network so that the user gets instant feedback. In order to do this
    //       I think it'll take some mucking about with antijoins (or maybe some clever
    //       stream default values?) to make every field in `ProgramStats` optional
    //       so that as soon as we have any data we can chuck it at them, even if it's
    //       incomplete
    total_workers
        .join(&total_dataflows)
        .join(&total_operators)
        .join(&total_subgraphs)
        .join(&total_channels)
        .join(&total_events)
        .join(&total_runtime)
        .map(
            |(
                (),
                ((((((workers, dataflows), operators), subgraphs), channels), events), runtime),
            )| {
                let runtime = runtime.element1.value.0 - runtime.element2.value.0;

                ProgramStats {
                    workers,
                    dataflows: dataflows as usize,
                    operators: operators as usize,
                    subgraphs: subgraphs as usize,
                    channels: channels as usize,
                    events: events as usize,
                    runtime,
                }
            },
        )
        .consolidate()
}

// TODO: Make this an operator
fn count_unique<S, D>(workers: &Collection<S, ((), D), Diff>) -> Collection<S, ((), usize), Diff>
where
    S: Scope,
    S::Timestamp: Lattice,
    D: ExchangeData + Hash,
{
    // TODO: Hierarchal aggregation?
    workers.reduce(move |&(), input, output| {
        // TODO: Preserve this buffer
        let mut buffer = HashSet::new();
        buffer.extend(
            input
                .iter()
                .filter_map(|&(data, diff)| if diff >= 1 { Some(data) } else { None }),
        );

        output.push((buffer.len(), 1));
    })
}

fn combine_events<S, D, TF, TD>(
    timely: &Stream<S, TimelyLogBundle>,
    map_timely: TF,
    differential: Option<&Stream<S, DifferentialLogBundle>>,
    map_differential: TD,
) -> Stream<S, D>
where
    S: Scope,
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

pub fn aggregate_worker_stats<S>(
    timely: &Stream<S, TimelyLogBundle>,
    differential: Option<&Stream<S, DifferentialLogBundle>>,
    operators: &Collection<S, (WorkerId, OperatesEvent), Diff>,
    channels: &Collection<S, (WorkerId, Channel), Diff>,
    subgraph_addresses: &ChannelAddrs<S, Diff>,
) -> Collection<S, (WorkerId, WorkerStats), Diff>
where
    S: Scope<Timestamp = Duration>,
{
    // TODO: Arrange this
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

    let total_events = combine_events(
        timely,
        |(time, worker, _)| (worker, time, 1isize),
        differential,
        |(time, worker, _)| (worker, time, 1),
    )
    // .delay_batch(|&time| granulate(time))
    .as_collection()
    .count();

    // TODO: Dear god this updates so much, figure out the ddflow
    //       delay thingy and make it not like that
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
    // .delay_batch(|&time| granulate(time))
    .as_collection()
    .count();

    // TODO: For whatever reason this part of the dataflow graph is de-prioritized,
    //       probably because of data dependence. Due to the nature of this data, for
    //       realtime streaming I'd like it to be the first thing being spat out over
    //       the network so that the user gets instant feedback. In order to do this
    //       I think it'll take some mucking about with antijoins (or maybe some clever
    //       stream default values?) to make every field in `ProgramStats` optional
    //       so that as soon as we have any data we can chuck it at them, even if it's
    //       incomplete
    dataflow_addrs
        .join(&total_dataflows)
        .join(&total_operators)
        .join(&total_subgraphs)
        .join(&total_channels)
        .join(&total_events)
        .join(&total_runtime)
        .map(
            |(
                worker,
                (
                    (((((dataflow_addrs, dataflows), operators), subgraphs), channels), events),
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
                        events: events as usize,
                        runtime,
                        dataflow_addrs,
                    },
                )
            },
        )
        .consolidate()
}
