use crate::dataflow::{
    differential::{self, ArrangementStats},
    summation::{summation, Summation},
    Diff, DifferentialLogBundle, OperatorId, WorkerId,
};
use abomonation_derive::Abomonation;
use differential_dataflow::{
    operators::{arrange::ArrangeByKey, Join, Reduce},
    Collection,
};
use std::time::Duration;
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
) -> Collection<S, (OperatorId, OperatorStats), Diff>
where
    S: Scope<Timestamp = Duration>,
{
    let operator_stats_arranged = operator_stats
        .map(|((_worker, operator), stats)| (operator, stats))
        .arrange_by_key_named("ArrangeByKey: Aggregate Operator Stats Across Workers");

    operator_stats_arranged.reduce_named(
        "Reduce: Aggregate Operator Stats Across Workers",
        |&operator, worker_stats, aggregated| {
            let activations = worker_stats.len();
            let mut activation_durations = Vec::new();
            let mut arrangements = Vec::new();
            let mut totals = Vec::new();

            for (stats, _diff) in worker_stats {
                totals.push(stats.total);
                activation_durations.extend(stats.activation_durations.iter().copied());

                if let Some(size) = stats.arrangement_size {
                    arrangements.push(size);
                }
            }

            let max = activation_durations
                .iter()
                .map(|&(duration, _)| duration)
                .max()
                .unwrap_or_else(|| Duration::from_secs(0));

            let min = activation_durations
                .iter()
                .map(|&(duration, _)| duration)
                .min()
                .unwrap_or_else(|| Duration::from_secs(0));

            let average = activation_durations
                .iter()
                .map(|&(duration, _)| duration)
                .sum::<Duration>()
                .checked_div(activation_durations.len() as u32)
                .unwrap_or_else(|| Duration::from_secs(0));

            let total = totals
                .iter()
                .sum::<Duration>()
                .checked_div(totals.len() as u32)
                .unwrap_or_else(|| Duration::from_secs(0));

            let arrangement_size = if arrangements.is_empty() {
                None
            } else {
                let max_size = arrangements
                    .iter()
                    .map(|arr| arr.max_size)
                    .max()
                    .unwrap_or(0);

                let min_size = arrangements
                    .iter()
                    .map(|arr| arr.max_size)
                    .min()
                    .unwrap_or(0);

                let batches = arrangements
                    .iter()
                    .map(|arr| arr.batches)
                    .sum::<usize>()
                    .checked_div(arrangements.len())
                    .unwrap_or(0);

                Some(ArrangementStats {
                    max_size,
                    min_size,
                    batches,
                })
            };

            let aggregate = OperatorStats {
                id: operator,
                worker: worker_stats[0].0.worker,
                max,
                min,
                average,
                total,
                activations,
                activation_durations,
                arrangement_size,
            };
            aggregated.push((aggregate, 1))
        },
    )
}
