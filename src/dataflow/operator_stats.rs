use crate::dataflow::{
    differential::ArrangementStats,
    summation::{summation, Summation},
    Diff, TimelyLogBundle, WorkerId,
};
use abomonation_derive::Abomonation;
use differential_dataflow::{
    collection::AsCollection,
    lattice::Lattice,
    operators::{Join, Reduce},
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
) -> Collection<S, ((WorkerId, usize), OperatorStats), Diff>
where
    S: Scope<Timestamp = Duration>,
{
    scope.region_named("Collect Operator Execution Durations", |region| {
        let log_stream = log_stream.enter(region);

        let scheduling_events = log_stream.flat_map(|(time, worker, event)| {
            if let TimelyEvent::Schedule(event) = event {
                Some(((worker, event), time, 1))
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

                            for ((worker, event), time, _diff) in buffer.drain(..) {
                                match event.start_stop {
                                    StartStop::Start => {
                                        schedule_map
                                            .insert((worker, event.id), (time, capability.clone()));
                                    }

                                    StartStop::Stop => {
                                        if let Some((start_time, mut stored_capability)) =
                                            schedule_map.remove(&(worker, event.id))
                                        {
                                            let duration = time - start_time;
                                            stored_capability.downgrade(
                                                &stored_capability.time().join(capability.time()),
                                            );

                                            output.session(&stored_capability).give((
                                                ((worker, event.id), duration),
                                                time,
                                                // Product::new(start_time, time),
                                                1,
                                            ))
                                        }
                                    }
                                }
                            }
                        });
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

        let execution_statistics = summation(&execution_durations);

        aggregated_durations
            .join_map(
                &execution_statistics,
                |&(worker, id),
                 activation_durations,
                 &Summation {
                     max,
                     min,
                     total,
                     average,
                     count: invocations,
                 }| {
                    let stats = OperatorStats {
                        id,
                        worker,
                        max,
                        min,
                        average,
                        total,
                        invocations,
                        activation_durations: activation_durations.to_owned(),
                        arrangement_size: None,
                    };

                    ((worker, id), stats)
                },
            )
            .leave_region()
    })
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Abomonation)]
pub struct OperatorStats {
    pub id: usize,
    pub worker: WorkerId,
    pub max: Duration,
    pub min: Duration,
    pub average: Duration,
    pub total: Duration,
    pub invocations: usize,
    pub activation_durations: Vec<(Duration, Duration)>,
    pub arrangement_size: Option<ArrangementStats>,
    // pub messages_sent: usize,
    // pub messages_received: usize,
}
