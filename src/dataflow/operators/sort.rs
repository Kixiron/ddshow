use differential_dataflow::{
    difference::Abelian,
    lattice::Lattice,
    operators::{Consolidate, Reduce},
    Collection, Data, ExchangeData, Hashable,
};
use timely::dataflow::Scope;

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
    R: Abelian + ExchangeData + From<i8>,
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
                .consolidate()
                .map(|(key, data)| ((data.hashed(), key), vec![data]));

            for &bucket in [60, 56, 52, 48, 44, 40, 36, 32, 28, 24, 20, 16, 12, 8, 4, 0].iter() {
                hashed = build_sort_bucket(hashed, key.clone(), 1u64 << bucket);
            }

            hashed
                .map(|((_hash, key), data)| (key, data))
                .consolidate()
                .leave()
        })
    }
}

fn build_sort_bucket<S, K, D, R, F, DK>(
    hashed: Collection<S, ((u64, K), Vec<D>), R>,
    key: F,
    bucket: u64,
) -> Collection<S, ((u64, K), Vec<D>), R>
where
    S: Scope,
    S::Timestamp: Lattice,
    D: Data + Default,
    Vec<D>: ExchangeData,
    ((u64, K), Vec<D>): ExchangeData,
    (u64, K): ExchangeData + Hashable,
    R: Abelian + ExchangeData + From<i8>,
    F: Fn(&D) -> DK + 'static,
    DK: Ord,
{
    let input = hashed.map(move |((hash, key), data)| ((hash % bucket, key), data));

    input.reduce_named("SortByBucket", move |_key, input, output| {
        let mut data: Vec<D> = input
            .iter()
            .flat_map(|(data, _diff)| data.iter().cloned())
            .collect();
        data.sort_unstable_by_key(&key);

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

                    if rng.gen_bool(0.5) {
                        input.remove(((), int));
                    }
                }

                input.flush();
                worker.step_or_park_while(None, || probe.less_than(input.time()));
            }

            worker.step_or_park_while(None, || probe.less_than(input.time()));
        });

        let result = CrossbeamExtractor::new(recv).extract_all();

        // FIXME: Replace with `slice::is_sorted()`
        let mut is_sorted = true;
        for window in result.windows(2) {
            if window[0] > window[1] {
                is_sorted = false;
            }
        }
        assert!(is_sorted);
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

            input.flush();
            worker.step_or_park_while(None, || probe.less_than(input.time()));
        });

        let result = CrossbeamExtractor::new(recv).extract_all();
        assert_eq!(result, vec![vec![1, 2, 3, 4, 5]]);
    }
}
