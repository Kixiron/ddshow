use differential_dataflow::{difference::Semigroup, Collection, Data};
use std::{fmt::Debug, panic::Location};
use timely::dataflow::{operators::Inspect, Scope, Stream};

pub trait InspectExt {
    type Value;

    fn debug_inspect<F>(&self, inspect: F) -> Self
    where
        F: FnMut(&Self::Value) + 'static;

    #[track_caller]
    fn debug(&self) -> Self
    where
        Self: Sized,
        Self::Value: Debug,
    {
        let location = Location::caller();
        self.debug_inspect(move |value| {
            println!(
                "[{}:{}:{}]: {:?}",
                location.file(),
                location.line(),
                location.column(),
                value,
            );
        })
    }
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

impl<S> InspectExt for Option<S>
where
    S: InspectExt,
{
    type Value = <S as InspectExt>::Value;

    fn debug_inspect<F>(&self, inspect: F) -> Self
    where
        F: FnMut(&Self::Value) + 'static,
    {
        self.as_ref().map(|stream| stream.debug_inspect(inspect))
    }
}
