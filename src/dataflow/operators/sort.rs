use crate::dataflow::operators::Multiply;
use differential_dataflow::{
    difference::Abelian,
    lattice::Lattice,
    operators::{Consolidate, Reduce},
    AsCollection, Collection, Data, ExchangeData, Hashable,
};
use timely::dataflow::{operators::Map, Scope};

pub trait SortBy<T> {
    type Output;

    fn sort_by<F, K>(&self, key: F) -> Self::Output
    where
        F: Fn(&T) -> K + Clone + 'static,
        K: Ord,
    {
        self.sort_by_named("SortBy", key)
    }

    fn sort_by_named<F, K>(&self, name: &str, key: F) -> Self::Output
    where
        F: Fn(&T) -> K + Clone + 'static,
        K: Ord;
}

impl<S, K, D, R> SortBy<D> for Collection<S, (K, D), R>
where
    S: Scope,
    S::Timestamp: Lattice,
    K: ExchangeData,
    D: ExchangeData + Hashable<Output = u64> + Default,
    Vec<D>: ExchangeData,
    (K, D): Hashable,
    (K, Vec<D>): Hashable,
    ((u64, K), Vec<D>): ExchangeData,
    (u64, K): ExchangeData + Hashable,
    R: Abelian + ExchangeData + Multiply<Output = R> + Into<isize> + From<i8>,
{
    type Output = Collection<S, (K, Vec<D>), R>;

    fn sort_by_named<F, DK>(&self, name: &str, key: F) -> Self::Output
    where
        F: Fn(&D) -> DK + Clone + 'static,
        DK: Ord,
    {
        // Utilizes hierarchical aggregation to minimize the number of recomputation that must happen
        self.scope().region_named(name, |region| {
            let mut hashed = self
                .enter_region(region)
                .map(|(key, data)| ((data.hashed(), key), vec![(data, R::from(1))]));

            for &bucket in [60, 56, 52, 48, 44, 40, 36, 32, 28, 24, 20, 16, 12, 8, 4, 0].iter() {
                hashed = build_sort_bucket(hashed, key.clone(), 1u64 << bucket);
            }

            hashed
                .inner
                .map(|(((_hash, key), data), time, diff)| {
                    let data = data
                        .into_iter()
                        .flat_map(|(data, inner_diff)| {
                            (0..inner_diff.into()).map(move |_| data.clone())
                        })
                        .collect::<Vec<_>>();

                    ((key, data), time, diff)
                })
                .as_collection()
                .consolidate()
                .leave()
        })
    }
}

type Bucketed<S, K, D, R> = Collection<S, ((u64, K), Vec<(D, R)>), R>;

fn build_sort_bucket<S, K, D, R, F, DK>(
    hashed: Bucketed<S, K, D, R>,
    key: F,
    bucket: u64,
) -> Bucketed<S, K, D, R>
where
    S: Scope,
    S::Timestamp: Lattice,
    D: Data + Default,
    Vec<(D, R)>: ExchangeData,
    ((u64, K), Vec<D>): ExchangeData,
    (u64, K): ExchangeData + Hashable,
    R: Abelian + ExchangeData + Multiply<Output = R> + From<i8>,
    F: Fn(&D) -> DK + 'static,
    DK: Ord,
{
    let input = hashed.map(move |((hash, key), data)| ((hash % bucket, key), data));

    // TODO: The buckets could take advantage of their inputs already being sorted
    input.reduce_named::<_, Vec<(D, R)>, R>("SortByBucket", move |_key, input, output| {
        let mut data: Vec<(D, R)> = input
            .iter()
            .flat_map(|(data, diff)| {
                data.iter().cloned().filter_map(move |(data, inner_diff)| {
                    let diff = diff.clone() * inner_diff;

                    if diff.is_zero() {
                        None
                    } else {
                        Some((data, diff))
                    }
                })
            })
            .collect();

        data.sort_unstable_by_key(|(data, _diff)| key(data));

        let mut idx = 0;
        while idx + 1 < data.len() {
            if data[idx].0 == data[idx + 1].0 {
                let diff = data[idx + 1].1.clone();
                data[idx].1 += &diff;

                data.remove(idx + 1);
            } else {
                idx += 1;
            }
        }

        output.push((data, R::from(1)));
    })
}

#[cfg(test)]
mod tests {
    use crate::dataflow::operators::{CrossbeamExtractor, CrossbeamPusher, SortBy};
    use differential_dataflow::input::Input;
    use rand::Rng;
    use timely::dataflow::operators::Capture;

    #[test]
    fn ensure_sorting() {
        let (send, recv) = crossbeam_channel::unbounded();
        timely::execute_directly(|worker| {
            let (mut input, probe) = worker.dataflow(|scope| {
                let (input, collection) = scope.new_collection();

                let sorted = collection.sort_by(|&int| int).map(|((), sorted)| sorted);
                sorted.inner.capture_into(CrossbeamPusher::new(send));

                (input, sorted.probe())
            });

            let mut rng = rand::thread_rng();
            for epoch in 0..rng.gen_range(10..=50) {
                input.advance_to(epoch);

                for _ in 0..rng.gen_range(100..=1000) {
                    let int = rng.gen::<isize>();
                    input.insert(((), int));

                    if rng.gen_bool(0.5) {
                        input.insert(((), int));
                    }

                    if rng.gen_bool(0.25) {
                        input.remove(((), int));
                    }
                }

                input.flush();
                worker.step_or_park_while(None, || probe.less_than(input.time()));
            }

            worker.step_or_park_while(None, || probe.less_than(input.time()));
        });

        let mut result = CrossbeamExtractor::new(recv).extract_all();
        assert_eq!(result.len(), 1);
        for window in result.remove(0).windows(2) {
            assert!(window[0] <= window[1]);
        }
    }

    #[test]
    fn ensure_retractions_propagate() {
        let (send, recv) = crossbeam_channel::unbounded();
        timely::execute_directly(|worker| {
            let (mut input, probe) = worker.dataflow(|scope| {
                let (input, collection) = scope.new_collection();

                let sorted = collection.sort_by(|&int| int).map(|((), sorted)| sorted);
                sorted.inner.capture_into(CrossbeamPusher::new(send));

                (input, sorted.probe())
            });

            input.insert(((), 1));
            input.insert(((), 1));
            input.remove(((), 1));
            input.insert(((), 2));
            input.insert(((), 3));
            input.insert(((), 4));
            input.insert(((), 5));

            input.advance_to(1);
            input.flush();
            worker.step_or_park_while(None, || probe.less_than(input.time()));
        });

        let result = CrossbeamExtractor::new(recv).extract_all();
        assert_eq!(result, vec![vec![1, 2, 3, 4, 5]]);
    }

    #[test]
    fn ensure_retractions_propagate_across_timestamps() {
        let (send, recv) = crossbeam_channel::unbounded();
        timely::execute_directly(|worker| {
            let (mut input, probe) = worker.dataflow(|scope| {
                let (input, collection) = scope.new_collection();

                let sorted = collection.sort_by(|&int| int).map(|((), sorted)| sorted);
                sorted.inner.capture_into(CrossbeamPusher::new(send));

                (input, sorted.probe())
            });

            input.insert(((), 1));
            input.insert(((), 1));
            input.insert(((), 2));
            input.insert(((), 3));
            input.insert(((), 4));
            input.insert(((), 5));
            input.advance_to(1);

            input.remove(((), 1));
            input.advance_to(2);

            input.insert(((), 2));
            input.advance_to(3);

            input.flush();
            worker.step_or_park_while(None, || probe.less_than(input.time()));
        });

        let result = CrossbeamExtractor::new(recv).extract_all();
        assert_eq!(result, vec![vec![1, 2, 2, 3, 4, 5]]);
    }

    #[test]
    fn fully_retract_values() {
        let (send, recv) = crossbeam_channel::unbounded();
        timely::execute_directly(|worker| {
            let (mut input, probe) = worker.dataflow(|scope| {
                let (input, collection) = scope.new_collection();

                let sorted = collection.sort_by(|&int| int).map(|((), sorted)| sorted);
                sorted.inner.capture_into(CrossbeamPusher::new(send));

                (input, sorted.probe())
            });

            input.insert(((), 1));
            input.insert(((), 2));
            input.insert(((), 3));
            input.insert(((), 4));
            input.insert(((), 5));
            input.advance_to(1);

            input.remove(((), 1));
            input.remove(((), 2));
            input.remove(((), 3));
            input.remove(((), 4));
            input.remove(((), 5));
            input.advance_to(2);

            input.flush();
            worker.step_or_park_while(None, || probe.less_than(input.time()));
        });

        let result = CrossbeamExtractor::new(recv).extract_all();
        assert_eq!(result, Vec::<Vec<usize>>::new());
    }
}
