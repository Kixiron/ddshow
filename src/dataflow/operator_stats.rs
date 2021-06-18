use crate::dataflow::{
    differential::{self, ArrangementStats},
    operators::{DiffDuration, Max, Maybe, Min},
    summation::{summation, Summation},
    utils::{Diff, DifferentialLogBundle, Time},
    OperatorId, WorkerId,
};
use abomonation_derive::Abomonation;
use differential_dataflow::{
    difference::DiffPair,
    operators::{CountTotal, Join},
    Collection,
};
use std::{iter, time::Duration};
use timely::dataflow::{Scope, Stream};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Abomonation)]
pub struct OperatorStats {
    pub id: OperatorId,
    pub worker: WorkerId,
    pub max: Duration,
    pub min: Duration,
    pub average: Duration,
    pub total: Duration,
    pub activations: usize,
    /// Operator activation times `(start, duration)`
    pub activation_durations: Vec<(Duration, Duration)>,
    pub arrangement_size: Option<ArrangementStats>,
    // pub messages_sent: usize,
    // pub messages_received: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Abomonation)]
pub struct AggregatedOperatorStats {
    pub id: OperatorId,
    pub max: Duration,
    pub min: Duration,
    pub average: Duration,
    pub total: Duration,
    pub activations: usize,
    /// Operator activation times `(start, duration)`
    pub activation_durations: Vec<(Duration, Duration)>,
    pub arrangement_size: Option<ArrangementStats>,
    // pub messages_sent: usize,
    // pub messages_received: usize,
}

type ActivationTimes<S> = Collection<S, ((WorkerId, OperatorId), (Duration, Duration)), Diff>;

pub fn operator_stats<S>(
    scope: &mut S,
    activation_times: &ActivationTimes<S>,
    differential_stream: Option<&Stream<S, DifferentialLogBundle>>,
) -> Collection<S, ((WorkerId, OperatorId), OperatorStats), Diff>
where
    S: Scope<Timestamp = Duration>,
{
    let operator_stats = scope.region_named("Build Operator Stats", |region| {
        let activation_times = activation_times.enter(region);

        // TODO: This is a super intensive reduce
        // let aggregated_durations = activation_times.reduce(|_id, durations, output| {
        //     let durations: Vec<_> = durations
        //         .iter()
        //         .flat_map(|&(&duration, diff)| (0..diff).map(move |_| duration))
        //         .collect();
        //
        //     output.push((durations, 1));
        // });

        let execution_statistics =
            summation(&activation_times.map(|(operator, (_start, duration))| (operator, duration)));

        // aggregated_durations
        //     .join_map(
        //         &execution_statistics,
        //         |&(worker, id),
        //          activation_durations,
        //          &Summation {
        //              max,
        //              min,
        //              total,
        //              average,
        //              count: activations,
        //          }| {
        //             let stats = OperatorStats {
        //                 id,
        //                 worker,
        //                 max,
        //                 min,
        //                 average,
        //                 total,
        //                 activations,
        //                 activation_durations: activation_durations.to_owned(),
        //                 arrangement_size: None,
        //             };
        //
        //             ((worker, id), stats)
        //         },
        //     )
        //     .leave_region()

        execution_statistics
            .map(
                |(
                    (worker, id),
                    Summation {
                        max,
                        min,
                        total,
                        average,
                        count: activations,
                    },
                )| {
                    let stats = OperatorStats {
                        id,
                        worker,
                        max,
                        min,
                        average,
                        total,
                        activations,
                        activation_durations: Vec::new(),
                        arrangement_size: None,
                    };

                    ((worker, id), stats)
                },
            )
            .leave_region()
    });

    differential_stream
        .as_ref()
        .map(|stream| {
            let arrangement_stats = differential::arrangement_stats(scope, stream);

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
        .unwrap_or(operator_stats)
}

pub(crate) fn aggregate_operator_stats<S>(
    operator_stats: &Collection<S, ((WorkerId, OperatorId), OperatorStats), Diff>,
) -> Collection<S, (OperatorId, AggregatedOperatorStats), Diff>
where
    S: Scope<Timestamp = Time>,
{
    let operator_stats = operator_stats.map(|((_worker, operator), stats)| (operator, stats));

    operator_stats
        .explode(
            |(
                operator,
                OperatorStats {
                    max,
                    min,
                    average,
                    total,
                    activations,
                    arrangement_size,
                    ..
                },
            )| {
                let diff = DiffPair::new(
                    1,
                    DiffPair::new(
                        Max::new(DiffDuration::new(max)),
                        DiffPair::new(
                            Min::new(DiffDuration::new(min)),
                            DiffPair::new(
                                DiffDuration::new(average),
                                DiffPair::new(
                                    DiffDuration::new(total),
                                    DiffPair::new(
                                        activations as isize,
                                        DiffPair::new(
                                            Maybe::from(
                                                arrangement_size
                                                    .map(|arr| Max::new(arr.max_size as isize)),
                                            ),
                                            DiffPair::new(
                                                Maybe::from(
                                                    arrangement_size
                                                        .map(|arr| Min::new(arr.min_size as isize)),
                                                ),
                                                Maybe::from(
                                                    arrangement_size
                                                        .map(|arr| arr.batches as isize),
                                                ),
                                            ),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                );

                iter::once((operator, diff))
            },
        )
        .count_total()
        .map(
            |(
                operator,
                DiffPair {
                    element1: total_workers,
                    element2:
                        DiffPair {
                            element1:
                                Max {
                                    value: max,
                                },
                            element2:
                                DiffPair {
                                    element1:
                                        Min {
                                            value: min,
                                        },
                                    element2:
                                        DiffPair {
                                            element1: summed_average,
                                            element2:
                                                DiffPair {
                                                    element1: summed_total,
                                                    element2: DiffPair {
                                                        element1: activations,
                                                        element2: DiffPair {
                                                            element1: arrangement_max_size,
                                                            element2:
                                                                DiffPair {
                                                                    element1: arrangement_min_size,
                                                                    element2:
                                                                        arrangement_total_batches,
                                                                },
                                                        },
                                                    },
                                                },
                                        },
                                },
                        },
                },
            )| {
                let average = summed_average
                    .to_duration()
                    .checked_div(total_workers as u32)
                    .unwrap_or_else(|| Duration::from_secs(0));
                let total = summed_total
                    .to_duration()
                    .checked_div(total_workers as u32)
                    .unwrap_or_else(|| Duration::from_secs(0));

                let arrangement_size = {
                    let max_size = Option::from(arrangement_max_size);
                    let min_size = Option::from(arrangement_min_size);
                    let batches = Option::from(arrangement_total_batches);

                    max_size
                        .zip(min_size)
                        .zip(batches)
                        .map(|((Max { value: max_size }, Min { value: min_size }), batches): (_, isize)| {
                            ArrangementStats {
                                max_size: max_size as usize,
                                min_size: min_size as usize,
                                batches: batches as usize,
                            }
                        })
                };

                let stats = AggregatedOperatorStats {
                    id: operator,
                    max: max.to_duration(),
                    min: min.to_duration(),
                    average,
                    total,
                    activations: activations as usize,
                    activation_durations: Vec::new(),
                    arrangement_size,
                };

                (operator, stats)
            },
        )
}
