use differential_dataflow::{difference::Semigroup, Collection, Data};
use timely::dataflow::{operators::Inspect, Scope, Stream};

pub trait InspectExt {
    type Value;

    fn debug_inspect<F>(&self, inspect: F) -> Self
    where
        F: FnMut(&Self::Value) + 'static;
}

impl<S, D, R> InspectExt for Collection<S, D, R>
where
    S: Scope,
    D: Data,
    R: Semigroup,
{
    type Value = (D, S::Timestamp, R);

    fn debug_inspect<F>(&self, inspect: F) -> Self
    where
        F: FnMut(&Self::Value) + 'static,
    {
        if cfg!(debug_assertions) {
            self.inspect(inspect)
        } else {
            self.clone()
        }
    }
}

impl<S, D> InspectExt for Stream<S, D>
where
    S: Scope,
    D: Data,
{
    type Value = D;

    fn debug_inspect<F>(&self, inspect: F) -> Self
    where
        F: FnMut(&Self::Value) + 'static,
    {
        if cfg!(debug_assertions) {
            self.inspect(inspect)
        } else {
            self.clone()
        }
    }
}
