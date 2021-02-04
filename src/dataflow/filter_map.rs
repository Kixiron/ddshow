use differential_dataflow::{difference::Semigroup, Collection};
use timely::{
    dataflow::{operators::Map, Scope, Stream},
    Data,
};

pub trait FilterMap<T, U> {
    type Output;

    fn filter_map<L>(&self, logic: L) -> Self::Output
    where
        L: FnMut(T) -> Option<U> + 'static;
}

impl<S, T, U> FilterMap<T, U> for Stream<S, T>
where
    S: Scope,
    T: Data,
    U: Data,
{
    type Output = Stream<S, U>;

    fn filter_map<L>(&self, logic: L) -> Self::Output
    where
        L: FnMut(T) -> Option<U> + 'static,
    {
        self.flat_map(logic)
    }
}

impl<S, T, U, R> FilterMap<T, U> for Collection<S, T, R>
where
    S: Scope,
    T: Data,
    U: Data,
    R: Semigroup,
{
    type Output = Collection<S, U, R>;

    fn filter_map<L>(&self, logic: L) -> Self::Output
    where
        L: FnMut(T) -> Option<U> + 'static,
    {
        self.flat_map(logic)
    }
}
