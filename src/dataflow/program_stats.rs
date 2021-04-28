use crate::{
    dataflow::{
        operators::{DiffDuration, FilterMap, Max, Min},
        types::WorkerId,
        Channel, Diff, DifferentialLogBundle, OperatesEvent, OperatorAddr, TimelyLogBundle,
        PROGRAM_NS_GRANULARITY,
    },
    ui::{ProgramStats, WorkerStats},
};
use differential_dataflow::{
    difference::DiffPair,
    lattice::Lattice,
    operators::{Consolidate, Count, Join, Reduce},
    AsCollection, Collection, ExchangeData,
};
use std::{collections::HashSet, hash::Hash, time::Duration};
use timely::dataflow::{
    operators::{Concat, Delay, Map},
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
    // TODO: Make this an arrangement
    subgraph_addresses: &Collection<S, (WorkerId, OperatorAddr), Diff>,
) -> Collection<S, ProgramStats, Diff>
where
    S: Scope<Timestamp = Duration>,
{
    let mut workers = timely.map(|(time, worker, _)| (((), worker), time, 1isize));
    if let Some(differential) = differential {
        workers = workers.concat(&differential.map(|(time, worker, _)| (((), worker), time, 1)));
    }

    let total_workers = count_unique(&workers.delay_batch(|&time| granulate(time)).as_collection());

    // TODO: Arrange this
    let addressed_operators =
        operators.map(|(worker, operator)| ((worker, operator.addr.clone()), operator));

    // FIXME: Uber inefficient, these three
    //        could have their early stages combined
    let total_operators = addressed_operators
        .antijoin(subgraph_addresses)
        .inner
        .map(|(_, time, _)| ((), time, 1isize))
        .as_collection()
        .count();

    let total_dataflows = addressed_operators
        .semijoin(subgraph_addresses)
        .filter(|((_, addr), _)| addr.len() == 1)
        .inner
        .map(|(_, time, _)| ((), time, 1isize))
        .as_collection()
        .count();

    let total_subgraphs = &addressed_operators
        .semijoin(subgraph_addresses)
        .inner
        .map(|(_, time, _)| ((), time, 1isize))
        .as_collection()
        .count();

    let total_channels = channels
        .inner
        .map(|(_, time, _)| ((), time, 1isize))
        .as_collection()
        .count();

    let mut events = timely.map(|(time, _, _)| ((), time, 1isize));
    if let Some(differential) = differential {
        events = events.concat(&differential.map(|(time, _, _)| ((), time, 1)));
    }
    let total_events = events
        .delay_batch(|&time| granulate(time))
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
        buffer.extend(input.iter().map(|&(data, _)| data));

        output.push((buffer.len(), 1));
    })
}

pub fn aggregate_worker_stats<S>(
    timely: &Stream<S, TimelyLogBundle>,
    differential: Option<&Stream<S, DifferentialLogBundle>>,
    operators: &Collection<S, (WorkerId, OperatesEvent), Diff>,
    channels: &Collection<S, (WorkerId, Channel), Diff>,
    // TODO: Make this an arrangement
    subgraph_addresses: &Collection<S, (WorkerId, OperatorAddr), Diff>,
) -> Collection<S, (WorkerId, WorkerStats), Diff>
where
    S: Scope<Timestamp = Duration>,
{
    // TODO: Arrange this
    let addressed_operators =
        operators.map(|(worker, operator)| ((worker, operator.addr.clone()), operator));

    // FIXME: Uber inefficient, these three
    //        could have their early stages combined
    let total_operators = addressed_operators
        .antijoin(subgraph_addresses)
        .inner
        .map(|(((worker, _), _), time, _)| (worker, time, 1isize))
        .as_collection()
        .count();

    let total_dataflows = addressed_operators
        .semijoin(subgraph_addresses)
        .filter(|((_, addr), _)| addr.len() == 1)
        .inner
        .map(|(((worker, _), _), time, _)| (worker, time, 1isize))
        .as_collection()
        .count();

    let dataflow_addrs = addressed_operators
        .semijoin(subgraph_addresses)
        .filter_map(|((worker, addr), _)| {
            if addr.len() == 1 {
                Some((worker, addr))
            } else {
                None
            }
        })
        .reduce(|_worker, input, output| {
            let dataflows: Vec<OperatorAddr> = input
                .iter()
                .filter_map(|&(addr, diff)| if diff >= 1 { Some(addr.clone()) } else { None })
                .collect();

            output.push((dataflows, 1));
        });

    let total_subgraphs = &addressed_operators
        .semijoin(subgraph_addresses)
        .inner
        .map(|(((worker, _), _), time, _)| (worker, time, 1isize))
        .as_collection()
        .count();

    let total_channels = channels
        .inner
        .map(|((worker, _), time, _)| (worker, time, 1isize))
        .as_collection()
        .count();

    let mut events = timely.map(|(time, worker, _)| (worker, time, 1isize));
    if let Some(differential) = differential {
        events = events.concat(&differential.map(|(time, worker, _)| (worker, time, 1)));
    }
    let total_events = events
        .delay_batch(|&time| granulate(time))
        .as_collection()
        .count();

    // TODO: Dear god this updates so much, figure out the ddflow
    //       delay thingy and make it not like that
    let mut event_timestamps = timely.map(|(time, worker, _)| {
        (
            worker,
            time,
            DiffPair::new(
                Max::new(DiffDuration::new(time)),
                Min::new(DiffDuration::new(time)),
            ),
        )
    });
    if let Some(differential) = differential {
        event_timestamps = event_timestamps.concat(&differential.map(|(time, worker, _)| {
            (
                worker,
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
