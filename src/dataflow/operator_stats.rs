use crate::dataflow::{
    differential::{self, ArrangementStats},
    operators::{DiffDuration, Keys, Max, Maybe, Min},
    summation::{summation, Summation},
    utils::{Diff, DifferentialLogBundle, Time},
    OperatorId, WorkerId,
};
use abomonation_derive::Abomonation;
use differential_dataflow::{
    difference::DiffPair,
    operators::{Consolidate, CountTotal, Join, Reduce},
    Collection,
};
use serde::{Deserialize, Serialize};
use std::{iter, time::Duration};
use timely::dataflow::{operators::Enter, Scope, Stream};

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
    /// Operator activation times `(start, duration)`
    pub activation_durations: Vec<(Duration, Duration)>,
    pub arrangement_size: Option<ArrangementStats>,
    // pub messages_sent: usize,
    // pub messages_received: usize,
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
    scope.region_named("Build Operator Stats", |region| {
        let (activation_times, differential_stream) = (
            activation_times.enter_region(region),
            differential_stream.map(|stream| stream.enter(region)),
        );

        let execution_statistics =
            summation(&activation_times.map(|(operator, (_start, duration))| (operator, duration)));

        let mut activation_durations = activation_times.reduce_named(
            "Aggregate Activation Times",
            |_, activations, output| {
                let mut activations: Vec<_> = activations
                    .iter()
                    .flat_map(|&(&activation, diff)| (0..diff).map(move |_| activation))
                    .collect();
                activations.sort_unstable_by_key(|activation| activation.0);

                output.push((activations, 1));
            },
        );
        activation_durations = activation_durations
            .map(|(operator, _)| (operator, Vec::new()))
            .antijoin(&activation_durations.keys())
            .concat(&activation_durations)
            .consolidate_named("Consolidate Activation Durations");

        let operator_stats = execution_statistics.join_map(
            &activation_durations,
            |&(worker, id),
             &Summation {
                 max,
                 min,
                 total,
                 average,
                 count: activations,
             },
             activation_durations| {
                let stats = OperatorStats {
                    id,
                    worker,
                    max,
                    min,
                    average,
                    total,
                    activations,
                    // TODO: Populate this
                    activation_durations: activation_durations.clone(),
                    arrangement_size: None,
                };

                ((worker, id), stats)
            },
        );

        differential_stream
            .as_ref()
            .map(|stream| {
                let arrangement_stats = differential::arrangement_stats(region, stream);

                let modified_stats = operator_stats.join_map(
                    &arrangement_stats,
                    |&operator, stats, arrangement_stats| {
                        let mut stats = stats.clone();
                        stats.arrangement_size = Some(arrangement_stats.clone());

                        (operator, stats)
                    },
                );

                operator_stats
                    .antijoin(&modified_stats.map(|(operator, _)| operator))
                    .concat(&modified_stats)
            })
            .unwrap_or(operator_stats)
            .leave_region()
    })
}

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
        .reduce_named(
            "Aggregate Operator Activation Durations",
            |_, activations, output| {
                let activations: Vec<_> = activations
                    .iter()
                    .flat_map(|&(activation, diff)| {
                        (0..diff).flat_map(move |_| activation.iter().copied())
                    })
                    .collect();

                output.push((activations, 1));
            },
        );
    activation_durations = operator_stats
        .map(|((_, operator), _)| (operator, Vec::new()))
        .antijoin(&activation_durations.keys())
        .concat(&activation_durations)
        .consolidate_named("Consolidate Aggregated Activation Durations");

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
        );

    aggregated.join_map(&activation_durations, |&operator, stats, activations| {
        let stats = AggregatedOperatorStats {
            activation_durations: activations.clone(),
            ..stats.clone()
        };

        (operator, stats)
    })
}
