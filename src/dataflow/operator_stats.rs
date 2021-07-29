use crate::dataflow::{
    differential::{self, ArrangementStats, SplineLevel},
    operators::{DiffDuration, Max, Min},
    summation::{summation, Summation},
    utils::{Diff, DifferentialLogBundle, OpKey, Time},
    OperatorId, WorkerId,
};
use abomonation_derive::Abomonation;
use differential_dataflow::{difference::DiffPair, operators::CountTotal, Collection};
use serde::{Deserialize, Serialize};
use std::{iter, time::Duration};
use timely::dataflow::{Scope, Stream};

type ActivationTimes<S> = Collection<S, ((WorkerId, OperatorId), (Duration, Duration)), Diff>;

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Abomonation, Deserialize, Serialize,
)]
pub struct OperatorStats {
    pub id: OperatorId,
    pub worker: WorkerId,
    pub max: Duration,
    pub min: Duration,
    pub average: Duration,
    pub total: Duration,
    pub activations: usize,
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Abomonation, Deserialize, Serialize,
)]
pub struct AggregatedOperatorStats {
    pub id: OperatorId,
    pub max: Duration,
    pub min: Duration,
    pub average: Duration,
    pub total: Duration,
    pub activations: usize,
    // /// Operator activation times `(start, duration)`
    // pub activation_durations: Vec<(Duration, Duration)>,
    // pub arrangement_size: Option<ArrangementStats>,
    // pub messages_sent: usize,
    // pub messages_received: usize,
}

pub struct OperatorStatsRelations<S>
where
    S: Scope<Timestamp = Time>,
{
    pub summarized: Collection<S, (OpKey, Summation), Diff>,
    pub aggregated_summaries: Collection<S, (OperatorId, Summation), Diff>,
    pub arrangements: Option<Collection<S, (OpKey, ArrangementStats), Diff>>,
    pub aggregated_arrangements: Option<Collection<S, (OperatorId, ArrangementStats), Diff>>,
    pub spline_levels: Option<Collection<S, (OpKey, SplineLevel), Diff>>,
}

pub fn operator_stats<S>(
    scope: &mut S,
    activation_times: &ActivationTimes<S>,
    differential_stream: Option<&Stream<S, DifferentialLogBundle>>,
) -> OperatorStatsRelations<S>
where
    S: Scope<Timestamp = Time>,
{
    let summarized =
        summation(&activation_times.map(|(operator, (_start, duration))| (operator, duration)));
    let (arrangements, spline_levels) = if let Some(stream) = differential_stream {
        let (arranged, splines) = differential::arrangement_stats(scope, stream);

        (Some(arranged), Some(splines))
    } else {
        (None, None)
    };

    let aggregated_summaries = summarized
        .explode(
            |(
                (_worker, operator),
                Summation {
                    max,
                    min,
                    average,
                    total,
                    count,
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
                                DiffPair::new(DiffDuration::new(total), count as isize),
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
                            element1: Max { value: max },
                            element2:
                                DiffPair {
                                    element1: Min { value: min },
                                    element2:
                                        DiffPair {
                                            element1: summed_average,
                                            element2:
                                                DiffPair {
                                                    element1: summed_total,
                                                    element2: activations,
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

                let stats = Summation {
                    max: max.to_duration(),
                    min: min.to_duration(),
                    average,
                    total,
                    count: activations as usize,
                };

                (operator, stats)
            },
        );

    let aggregated_arrangements = arrangements.as_ref().map(|arrangements| {
        arrangements
            .explode(|((_worker, operator), stats)| {
                let diff = DiffPair::new(
                    1,
                    DiffPair::new(
                        Max::new(stats.max_size as isize),
                        DiffPair::new(Min::new(stats.min_size as isize), stats.batches as isize),
                    ),
                );

                iter::once((operator, diff))
            })
            .count_total()
            .map(
                |(
                    operator,
                    DiffPair {
                        element1: total_workers,
                        element2:
                            DiffPair {
                                element1: Max { value: max_size },
                                element2:
                                    DiffPair {
                                        element1: Min { value: min_size },
                                        element2: batches,
                                    },
                            },
                    },
                )| {
                    let average_batches = (batches as usize)
                        .checked_div(total_workers as usize)
                        .unwrap_or(0);

                    (
                        operator,
                        ArrangementStats {
                            max_size: max_size as usize,
                            min_size: min_size as usize,
                            batches: average_batches,
                        },
                    )
                },
            )
    });

    OperatorStatsRelations {
        summarized,
        aggregated_summaries,
        arrangements,
        aggregated_arrangements,
        spline_levels,
    }
}

/*
pub(crate) fn aggregate_operator_stats<S>(
    operator_stats: &Collection<S, ((WorkerId, OperatorId), OperatorStats), Diff>,
) -> Collection<S, (OperatorId, AggregatedOperatorStats), Diff>
where
    S: Scope<Timestamp = Time>,
{
    let operator_stats_without_worker =
        operator_stats.map(|((_worker, operator), stats)| (operator, stats));

    let mut activation_durations = operator_stats
        .map(|((_, operator), stats)| (operator, stats.activation_durations))
        .hierarchical_reduce_named(
            "Aggregate Operator Activation Durations",
            |_, activations, output| {
                let activations: Vec<_> = activations
                    .iter()
                    .flat_map(|&(activation, _)| activation)
                    .copied()
                    .collect();

                output.push((activations, 1));
            },
            |_, activations, output| {
                let mut activations: Vec<_> = activations
                    .iter()
                    .flat_map(|&(activation, _)| activation)
                    .copied()
                    .collect();
                activations.sort_unstable_by_key(|activation| activation.0);

                output.push((activations, 1));
            },
        )
        .debug_frontier();
    activation_durations = operator_stats
        .map(|((_, operator), _)| (operator, Vec::new()))
        .antijoin(&activation_durations.keys())
        .concat(&activation_durations)
        .consolidate_named("Consolidate Aggregated Activation Durations")
        .debug_frontier();

    let aggregated = operator_stats_without_worker
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
                                                    .as_ref()
                                                    .map(|arr| Max::new(arr.max_size as isize)),
                                            ),
                                            DiffPair::new(
                                                Maybe::from(
                                                    arrangement_size
                                                        .as_ref()
                                                        .map(|arr| Min::new(arr.min_size as isize)),
                                                ),
                                                Maybe::from(
                                                    arrangement_size
                                                        .as_ref()
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
                    let Min { value: min_size } = Option::from(arrangement_min_size).unwrap_or_else(|| Min::new(0));
                    let batches = Option::from(arrangement_total_batches).unwrap_or(0isize);

                    if let Some(Max { value: max_size }) = max_size {
                          Some(ArrangementStats {
                                max_size: max_size as usize,
                                min_size: min_size as usize,
                                batches: batches as usize,
                                // FIXME: Aggregate all spline levels?
                                spline_levels: Vec::new(),
                            })
                    } else {
                        None
                    }
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
        .debug_frontier();

    aggregated.join_map(&activation_durations, |&operator, stats, activations| {
        let stats = AggregatedOperatorStats {
            activation_durations: activations.clone(),
            ..stats.clone()
        };

        (operator, stats)
    })
}
*/
