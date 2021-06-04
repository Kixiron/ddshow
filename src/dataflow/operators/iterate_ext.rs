use differential_dataflow::{
    difference::{Abelian, Semigroup},
    lattice::Lattice,
    operators::iterate::{SemigroupVariable, Variable},
    Collection, Data,
};
use timely::{
    dataflow::{scopes::child::Iterative, Scope},
    order::Product,
};

/// An extension trait for the `iterate` method.
pub trait IterateExt<S, D, R>
where
    S: Scope,
    D: Data,
    R: Semigroup,
{
    fn iterate_named<F>(&self, name: &str, logic: F) -> Collection<S, D, R>
    where
        S::Timestamp: Lattice,
        for<'a> F: FnOnce(
            &Collection<Iterative<'a, S, u64>, D, R>,
        ) -> Collection<Iterative<'a, S, u64>, D, R>;
}

impl<G, D, R> IterateExt<G, D, R> for Collection<G, D, R>
where
    G: Scope,
    D: Ord + Data,
    R: Abelian,
{
    fn iterate_named<F>(&self, name: &str, logic: F) -> Collection<G, D, R>
    where
        G::Timestamp: Lattice,
        for<'a> F: FnOnce(
            &Collection<Iterative<'a, G, u64>, D, R>,
        ) -> Collection<Iterative<'a, G, u64>, D, R>,
    {
        self.inner.scope().scoped(name, |subgraph| {
            // create a new variable, apply logic, bind variable, return.
            //
            // this could be much more succinct if we returned the collection
            // wrapped by `variable`, but it also results in substantially more
            // diffs produced; `result` is post-consolidation, and means fewer
            // records are yielded out of the loop.
            let variable =
                Variable::new_from(self.enter(subgraph), Product::new(Default::default(), 1));
            let result = logic(&variable);
            variable.set(&result);
            result.leave()
        })
    }
}

impl<G, D, R> IterateExt<G, D, R> for G
where
    G: Scope,
    D: Ord + Data,
    R: Semigroup,
{
    fn iterate_named<F>(&self, name: &str, logic: F) -> Collection<G, D, R>
    where
        G::Timestamp: Lattice,
        for<'a> F: FnOnce(
            &Collection<Iterative<'a, G, u64>, D, R>,
        ) -> Collection<Iterative<'a, G, u64>, D, R>,
    {
        // TODO: This makes me think we have the wrong ownership pattern here.
        let mut clone = self.clone();
        clone.scoped(name, |subgraph| {
            // create a new variable, apply logic, bind variable, return.
            //
            // this could be much more succinct if we returned the collection
            // wrapped by `variable`, but it also results in substantially more
            // diffs produced; `result` is post-consolidation, and means fewer
            // records are yielded out of the loop.
            let variable = SemigroupVariable::new(subgraph, Product::new(Default::default(), 1));
            let result = logic(&variable);
            variable.set(&result);
            result.leave()
        })
    }
}
