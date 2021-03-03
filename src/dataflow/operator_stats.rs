use super::{Diff, DiffDuration, Max, Min, TimelyLogBundle};
use abomonation_derive::Abomonation;
use differential_dataflow::{
    collection::AsCollection,
    difference::DiffPair,
    lattice::Lattice,
    operators::{CountTotal, Join, Reduce},
    Collection,
};
use std::{collections::HashMap, time::Duration};
use timely::{
    dataflow::{
        channels::pact::Pipeline,
        operators::{Enter, Map, Operator},
        Scope, Stream,
    },
    logging::{StartStop, TimelyEvent},
};

pub fn operator_stats<S>(
    scope: &mut S,
    log_stream: &Stream<S, TimelyLogBundle>,
) -> Collection<S, (usize, OperatorStats), Diff>
where
    S: Scope<Timestamp = Duration>,
{
    scope.region_named("Collect Operator Execution Durations", |region| {
        let log_stream = log_stream.enter(region);

        let scheduling_events = log_stream.flat_map(|(time, _worker, event)| {
            if let TimelyEvent::Schedule(event) = event {
                Some((event, time, 1))
            } else {
                None
            }
        });

        let execution_durations = scheduling_events
            .unary(
                Pipeline,
                "Associate Start/Stop Events",
                |_capability, _info| {
                    let mut buffer = Vec::new();
                    let mut schedule_map = HashMap::new();

                    move |input, output| {
                        input.for_each(|capability, data| {
                            let capability = capability.retain();
                            data.swap(&mut buffer);

                            for (event, time, _diff) in buffer.drain(..) {
                                match event.start_stop {
                                    StartStop::Start => {
                                        schedule_map.insert(event.id, (time, capability.clone()));
                                    }

                                    StartStop::Stop => {
                                        if let Some((start_time, mut stored_capability)) =
                                            schedule_map.remove(&event.id)
                                        {
                                            let duration = time - start_time;
                                            stored_capability.downgrade(
                                                &stored_capability.time().join(capability.time()),
                                            );

                                            output.session(&stored_capability).give((
                                                (event.id, duration),
                                                time,
                                                // Product::new(start_time, time),
                                                1,
                                            ))
                                        }
                                    }
                                }
                            }
                        })
                    }
                },
            )
            .as_collection();

        let aggregated_durations = execution_durations
            .inner
            .map(|((operator, duration), time, diff)| ((operator, (duration, time)), time, diff))
            .as_collection()
            .reduce(|_id, durations, output| {
                let durations: Vec<_> = durations
                    .iter()
                    .flat_map(|&(&duration, diff)| (0..diff).map(move |_| duration))
                    .collect();
                output.push((durations, 1));
            });

        let execution_statistics = execution_durations
            .explode(|(id, duration)| {
                let duration = DiffDuration::new(duration);
                let (min, max) = (Min::new(duration), Max::new(duration));

                Some((
                    id,
                    DiffPair::new(1, DiffPair::new(duration, DiffPair::new(min, max))),
                ))
            })
            .count_total()
            .map(
                |(
                    id,
                    DiffPair {
                        element1: count,
                        element2:
                            DiffPair {
                                element1: total,
                                element2:
                                    DiffPair {
                                        element1: min,
                                        element2: max,
                                    },
                            },
                    },
                )| {
                    (
                        id,
                        (
                            max.value.0,
                            min.value.0,
                            total.0,
                            total.0 / count as u32,
                            count as usize,
                        ),
                    )
                },
            );

        aggregated_durations
            .join_map(
                &execution_statistics,
                |&id, activation_durations, &(max, min, total, average, invocations)| {
                    let stats = OperatorStats {
                        id,
                        max,
                        min,
                        average,
                        total,
                        invocations,
                        activation_durations: activation_durations.to_owned(),
                    };

                    (id, stats)
                },
            )
            .leave_region()
    })
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct OperatorStats {
    pub id: usize,
    pub max: Duration,
    pub min: Duration,
    pub average: Duration,
    pub total: Duration,
    pub invocations: usize,
    pub activation_durations: Vec<(Duration, Duration)>,
    // pub messages_sent: usize,
    // pub messages_received: usize,
}
