use crate::{
    dataflow::{
        operators::{DiffDuration, FilterSplit, Max, Min},
        types::WorkerId,
        Channel, Diff, DifferentialLogBundle, OperatesEvent, OperatorAddr, TimelyLogBundle,
        PROGRAM_NS_GRANULARITY,
    },
    ui::ProgramStats,
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

    let total_workers = count_unique(&workers.as_collection());

    // TODO: Arrange this
    let addressed_operators =
        operators.map(|(worker, operator)| ((worker, operator.addr.clone()), operator));

    let (total_dataflows, total_operators) = addressed_operators
        .antijoin(subgraph_addresses)
        .inner
        .filter_split(|(((_, addr), _), time, _)| {
            if addr.len() == 1 {
                (Some(((), time, 1isize)), None)
            } else {
                (None, Some(((), time, 1isize)))
            }
        });

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
        .delay(|_, &time| granulate(time))
        .as_collection()
        .count();

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

    let total_runtime = event_timestamps
        .delay(|_, &time| granulate(time))
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
    total_workers
        .join(&total_dataflows.as_collection().count())
        .join(&total_operators.as_collection().count())
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
