use crate::dataflow::{
    operators::{FilterMapTimed, Max, Min},
    utils::{Diff, DifferentialLogBundle, OpKey, Time},
};
use abomonation_derive::Abomonation;
use ddshow_types::differential_logging::DifferentialEvent;
#[cfg(not(feature = "timely-next"))]
use differential_dataflow::difference::DiffPair;
use differential_dataflow::{operators::CountTotal, AsCollection, Collection};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use timely::dataflow::{operators::Enter, Scope, Stream};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation, Serialize)]
pub struct SplineLevel {
    pub event_time: Duration,
    pub scale: usize,
    pub complete_size: usize,
}

impl SplineLevel {
    pub const fn new(event_time: Duration, scale: usize, complete_size: usize) -> Self {
        Self {
            event_time,
            scale,
            complete_size,
        }
    }
}

type Arrangements<S> = (
    Collection<S, (OpKey, ArrangementStats), Diff>,
    Collection<S, (OpKey, SplineLevel), Diff>,
);

pub fn arrangement_stats<S>(
    scope: &mut S,
    differential_events: &Stream<S, DifferentialLogBundle>,
) -> Arrangements<S>
where
    S: Scope<Timestamp = Time>,
{
    scope.region_named("Collect arrangement statistics", |region| {
        let differential_events = differential_events.enter(region);

        let merge_diffs = differential_events
            .filter_map_timed(|&time, (_event_time, worker, event)| match event {
                DifferentialEvent::Batch(batch) => Some((
                    ((worker, batch.operator), (batch.length as isize, 1)),
                    time,
                    1,
                )),
                DifferentialEvent::Merge(merge) => merge.complete.map(|complete_size| {
                    (
                        ((worker, merge.operator), (complete_size as isize, 0)),
                        time,
                        1,
                    )
                }),

                DifferentialEvent::MergeShortfall(_)
                | DifferentialEvent::Drop(_)
                | DifferentialEvent::TraceShare(_) => None,
            })
            .as_collection();

        let spline_levels = differential_events
            .filter_map_timed(|&time, (event_time, worker, event)| match event {
                DifferentialEvent::Merge(merge) => merge.complete.map(|complete_size| {
                    (
                        (
                            (worker, merge.operator),
                            SplineLevel::new(event_time, merge.scale, complete_size),
                        ),
                        time,
                        1isize,
                    )
                }),

                DifferentialEvent::Batch(_)
                | DifferentialEvent::MergeShortfall(_)
                | DifferentialEvent::Drop(_)
                | DifferentialEvent::TraceShare(_) => None,
            })
            .as_collection();

        #[cfg(feature = "timely-next")]
        let merge_stats = merge_diffs
            .explode(|(key, (size, batches))| {
                let (min, max) = (Min::new(size), Max::new(size));

                Some((key, (1, size, batches, min, max)))
            })
            .count_total()
            .map(|(key, (_count, _total, batches, min, max))| {
                let stats = ArrangementStats {
                    max_size: max.value as usize,
                    min_size: min.value as usize,
                    batches: batches as usize,
                };

                (key, stats)
            });

        #[cfg(not(feature = "timely-next"))]
        let merge_stats = merge_diffs
            .explode(|(key, (size, batches))| {
                let (min, max) = (Min::new(size), Max::new(size));

                Some((
                    key,
                    DiffPair::new(
                        1,
                        DiffPair::new(size, DiffPair::new(batches, DiffPair::new(min, max))),
                    ),
                ))
            })
            .count_total()
            .map(
                |(
                    key,
                    DiffPair {
                        element1: _count,
                        element2:
                            DiffPair {
                                element1: _total,
                                element2:
                                    DiffPair {
                                        element1: batches,
                                        element2:
                                            DiffPair {
                                                element1: min,
                                                element2: max,
                                            },
                                    },
                            },
                    },
                )| {
                    let stats = ArrangementStats {
                        max_size: max.value as usize,
                        min_size: min.value as usize,
                        batches: batches as usize,
                    };

                    (key, stats)
                },
            );

        (merge_stats.leave_region(), spline_levels.leave_region())
    })
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation, Deserialize, Serialize,
)]
pub struct ArrangementStats {
    pub max_size: usize,
    pub min_size: usize,
    pub batches: usize,
    // /// Merge time, merge scale and the completed size of the merge
    // pub spline_levels: Vec<(Duration, usize, usize)>,
    // TODO: Max/min/average batch size
    // TODO: Arrangement growth trend (Linear, logarithmic, quadratic, etc.)?
}
