use super::{Diff, DiffDuration, Max, Min, TimelyLogBundle};
use abomonation_derive::Abomonation;
use differential_dataflow::{
    collection::AsCollection, difference::DiffPair, operators::Count, Collection,
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

        let execution_durations = log_stream
            .flat_map(|(time, _worker, event)| {
                if let TimelyEvent::Schedule(event) = event {
                    Some((event, time, 1))
                } else {
                    None
                }
            })
            .unary(
                Pipeline,
                "Associate Start/Stop Events",
                |_capability, _info| {
                    let mut buffer = Vec::new();
                    let mut schedule_map = HashMap::new();

                    move |input, output| {
                        input.for_each(|capability, data| {
                            let mut output_session = output.session(&capability);
                            data.swap(&mut buffer);

                            for (event, time, _diff) in buffer.drain(..) {
                                // TODO: This helps, but if an operator runs over a period longer than the
                                //       millisecond granularity it may still produce duplicate records.
                                //       Fix it, somehow
                                let granularity_ms = 1000;
                                let time_ms = Duration::from_millis(
                                    (((time.as_millis() / granularity_ms) + 1) * granularity_ms)
                                        as _,
                                );

                                match event.start_stop {
                                    StartStop::Start => {
                                        schedule_map.insert(event.id, time);
                                    }

                                    StartStop::Stop => {
                                        if let Some(start_time) = schedule_map.remove(&event.id) {
                                            let duration = time - start_time;
                                            output_session.give(((event.id, duration), time_ms, 1));
                                        }
                                    }
                                }
                            }
                        })
                    }
                },
            )
            .as_collection();

        // // Go home clippy, you're drunk
        // #[allow(clippy::suspicious_map)]
        // let number_invocations = execution_durations.map(|(id, _)| id).count();
        //
        // let total_execution_times = execution_durations.reduce(|_id, durations, output| {
        //     output.push((
        //         durations
        //             .iter()
        //             .map(|(&duration, _)| duration)
        //             .sum::<Duration>(),
        //         1,
        //     ));
        // });
        //
        // let max_execution_times = execution_durations.reduce(|_id, durations, output| {
        //     output.push((
        //         durations
        //             .iter()
        //             .map(|(&duration, _)| duration)
        //             .max()
        //             .unwrap_or_default(),
        //         1,
        //     ));
        // });
        //
        // let min_execution_times = execution_durations.reduce(|_id, durations, output| {
        //     output.push((
        //         durations
        //             .iter()
        //             .map(|(&duration, _)| duration)
        //             .min()
        //             .unwrap_or_default(),
        //         1,
        //     ));
        // });
        //
        // // TODO: Now `TimelyProgressEvents` are sent to a dedicated `"timely/progress"` logger.
        // //       To deal with getting this, differential and timely all set up and pointed
        // //       to the proper tcp address for debugging, we probably want to offer a
        // //       library crate that contains setup functions that forward things to us for
        // //       users with minimal setup/teardown overhead
        // //
        // // let (messages_received, messages_sent) =
        // //     region.region_named("Operator Message Stats", |region| {
        // //         let progress_events = todo!();
        // //
        // //         let (messages_received, messages_sent) = progress_events
        // //             .map(|progress| (progress.addr, (progress.is_send, progress.messages.len())))
        // //             .join_map(&operator_addrs, |_addr, (&is_send, &num_messages), &id| {
        // //                 (id, (is_send, num_messages))
        // //             })
        // //             .branch(|_time, (is_send, _)| is_send);
        // //
        // //         let messages_received = messages_received
        // //             .map(|(_, num_messages)| num_messages)
        // //             .reduce(|_id, messages, output| {
        // //                 output.push(messages.iter().sum());
        // //             });
        // //
        // //         let messages_sent = messages_sent.map(|(_, num_messages)| num_messages).reduce(
        // //             |_id, messages, output| {
        // //                 output.push(messages.iter().sum());
        // //             },
        // //         );
        // //
        // //         (
        // //             messages_received.leave_region(),
        // //             messages_sent.leave_region(),
        // //         )
        // //     });
        //
        // number_invocations
        //     .join(&total_execution_times)
        //     .join(&max_execution_times)
        //     .join(&min_execution_times)
        //     // .join(&messages_received)
        //     // .join(&messages_sent)
        //     .map(|(id, (((invocations, total), max), min))| {
        //         (
        //             id,
        //             OperatorStats {
        //                 id,
        //                 max,
        //                 min,
        //                 average: Duration::from_nanos(
        //                     (total.as_nanos() / invocations as u128) as u64,
        //                 ),
        //                 total,
        //                 invocations: invocations as usize,
        //                 // messages_sent,
        //                 // messages_received,
        //             },
        //         )
        //     })
        //     .leave_region()

        execution_durations
            .explode(|(id, duration)| {
                let duration = DiffDuration::new(duration);

                Some((
                    id,
                    DiffPair::new(
                        1,
                        DiffPair::new(
                            duration,
                            DiffPair::new(Min::new(duration), Max::new(duration)),
                        ),
                    ),
                ))
            })
            .count()
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
                        OperatorStats {
                            id,
                            max: max.value.0,
                            min: min.value.0,
                            total: total.0,
                            average: total.0 / count as u32,
                            invocations: count as usize,
                        },
                    )
                },
            )
            .leave_region()
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct OperatorStats {
    pub id: usize,
    pub max: Duration,
    pub min: Duration,
    pub average: Duration,
    pub total: Duration,
    pub invocations: usize,
    // pub messages_sent: usize,
    // pub messages_received: usize,
}
