use crate::dataflow::{
    operators::{DiffDuration, Max, Min},
    Diff,
};
use abomonation_derive::Abomonation;
#[cfg(not(feature = "timely-next"))]
use differential_dataflow::difference::DiffPair;
use differential_dataflow::{
    lattice::Lattice, operators::CountTotal, Collection, ExchangeData, Hashable,
};
use std::time::Duration;
use timely::{dataflow::Scope, order::TotalOrder};

pub fn summation<S, K>(
    collection: &Collection<S, (K, Duration), Diff>,
) -> Collection<S, (K, Summation), Diff>
where
    S: Scope,
    S::Timestamp: Lattice + TotalOrder,
    K: ExchangeData + Hashable,
{
    #[cfg(not(feature = "timely-next"))]
    let summation = collection
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
                    Summation::new(
                        max.value.to_duration(),
                        min.value.to_duration(),
                        total.to_duration(),
                        total
                            .to_duration()
                            .checked_div(count as u32)
                            .unwrap_or_else(|| Duration::from_secs(0)),
                        count as usize,
                    ),
                )
            },
        );

    #[cfg(feature = "timely-next")]
    let summation = collection
        .explode(|(id, duration)| {
            let duration = DiffDuration::new(duration);
            let (min, max) = (Min::new(duration), Max::new(duration));

            Some((id, (1, duration, min, max)))
        })
        .count_total()
        .map(|(id, (count, total, min, max))| {
            (
                id,
                Summation::new(
                    max.value.0,
                    min.value.0,
                    total.0,
                    total.0 / count as u32,
                    count as usize,
                ),
            )
        });

    summation
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct Summation {
    pub max: Duration,
    pub min: Duration,
    pub total: Duration,
    pub average: Duration,
    pub count: usize,
}

impl Summation {
    pub const fn new(
        max: Duration,
        min: Duration,
        total: Duration,
        average: Duration,
        count: usize,
    ) -> Self {
        Self {
            max,
            min,
            total,
            average,
            count,
        }
    }
}
