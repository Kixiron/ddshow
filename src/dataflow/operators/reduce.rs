use crate::dataflow::operators::MapExt;
use differential_dataflow::{
    difference::{Abelian, Semigroup},
    lattice::Lattice,
    operators::{arrange::ArrangeByKey, Reduce},
    Collection, ExchangeData, Hashable,
};
use std::panic::Location;
use timely::dataflow::Scope;

#[allow(dead_code)]
const DEFAULT_HIERARCHICAL_BUCKETS: [u64; 16] =
    [60, 56, 52, 48, 44, 40, 36, 32, 28, 24, 20, 16, 12, 8, 4, 0];

pub trait HierarchicalReduce<S, K, V, R>
where
    S: Scope,
    R: Semigroup,
{
    #[track_caller]
    fn hierarchical_reduce<F1, F2, V2, R2>(
        &self,
        initial_reduce: F1,
        reduce: F2,
    ) -> Collection<S, (K, V2), R2>
    where
        F1: FnMut(&K, &[(&V, R)], &mut Vec<(V2, R2)>) + Clone + 'static,
        F2: FnMut(&K, &[(&V2, R2)], &mut Vec<(V2, R2)>) + Clone + 'static,
        V2: ExchangeData,
        R2: ExchangeData + Abelian,
    {
        let caller = Location::caller();
        let name = format!(
            "HierarchicalReduce @ {}:{}:{}",
            caller.file(),
            caller.line(),
            caller.column(),
        );

        self.hierarchical_reduce_named(&name, initial_reduce, reduce)
    }

    fn hierarchical_reduce_named<F1, F2, V2, R2>(
        &self,
        name: &str,
        initial_reduce: F1,
        reduce: F2,
    ) -> Collection<S, (K, V2), R2>
    where
        F1: FnMut(&K, &[(&V, R)], &mut Vec<(V2, R2)>) + Clone + 'static,
        F2: FnMut(&K, &[(&V2, R2)], &mut Vec<(V2, R2)>) + Clone + 'static,
        V2: ExchangeData,
        R2: ExchangeData + Abelian,
    {
        self.hierarchical_reduce_core(
            name,
            DEFAULT_HIERARCHICAL_BUCKETS.iter().copied(),
            initial_reduce,
            reduce,
        )
    }

    fn hierarchical_reduce_core<F1, F2, B, V2, R2>(
        &self,
        name: &str,
        buckets: B,
        initial_reduce: F1,
        reduce: F2,
    ) -> Collection<S, (K, V2), R2>
    where
        B: IntoIterator<Item = u64>,
        F1: FnMut(&K, &[(&V, R)], &mut Vec<(V2, R2)>) + Clone + 'static,
        F2: FnMut(&K, &[(&V2, R2)], &mut Vec<(V2, R2)>) + Clone + 'static,
        V2: ExchangeData,
        R2: ExchangeData + Abelian;
}

impl<S, K, V, R> HierarchicalReduce<S, K, V, R> for Collection<S, (K, V), R>
where
    S: Scope,
    S::Timestamp: Lattice,
    K: ExchangeData + Hashable,
    V: ExchangeData + Hashable<Output = u64>,
    R: ExchangeData + Semigroup,
    (K, u64): Hashable,
{
    fn hierarchical_reduce_core<F1, F2, B, V2, R2>(
        &self,
        name: &str,
        buckets: B,
        mut initial_reduce: F1,
        reduce: F2,
    ) -> Collection<S, (K, V2), R2>
    where
        B: IntoIterator<Item = u64>,
        F1: FnMut(&K, &[(&V, R)], &mut Vec<(V2, R2)>) + Clone + 'static,
        F2: FnMut(&K, &[(&V2, R2)], &mut Vec<(V2, R2)>) + Clone + 'static,
        V2: ExchangeData,
        R2: ExchangeData + Abelian,
    {
        self.scope().region_named(name, |region| {
            let this = self.enter_region(region);

            let mut hashed = this
                .map_named("HierarchicalReduce: Hash Input Data", |(key, data)| {
                    ((key, data.hashed()), data)
                })
                .arrange_by_key_named("HierarchicalReduce: ArrangeByKey for Initial Reduce")
                .reduce_named(
                    "HierarchicalReduce: Initial Reduce",
                    move |(key, _hash), input, output| {
                        initial_reduce(key, input, output);
                    },
                );

            for bucket in buckets {
                hashed = build_reduce_bucket(hashed, reduce.clone(), 1u64 << bucket);
            }

            hashed
                .map_named(
                    "HierarchicalReduce: Extract Data",
                    |((key, _hash), data)| (key, data),
                )
                .leave_region()
        })
    }
}

type Bucketed<S, K, V, R> = Collection<S, ((K, u64), V), R>;

fn build_reduce_bucket<S, K, V, R, F>(
    hashed: Bucketed<S, K, V, R>,
    mut reduce: F,
    bucket: u64,
) -> Bucketed<S, K, V, R>
where
    S: Scope,
    S::Timestamp: Lattice,
    F: FnMut(&K, &[(&V, R)], &mut Vec<(V, R)>) + 'static,
    K: ExchangeData,
    V: ExchangeData,
    R: ExchangeData + Abelian,
    (K, u64): Hashable,
{
    let input = hashed.map_named(
        &format!("HierarchicalReduce: Map Hash for Bucket {}", bucket),
        move |((key, hash), data)| ((key, hash % bucket), data),
    );

    input
        .arrange_by_key_named(&format!(
            "HierarchicalReduce: ArrangeByKey for Bucket {}",
            bucket,
        ))
        .reduce_named(
            &format!("HierarchicalReduce: Reduce for Bucket {}", bucket),
            move |(key, _hash), input, output| {
                reduce(key, input, output);
            },
        )
}
