use differential_dataflow::{difference::Semigroup, AsCollection, Collection, Data};
use std::{fmt::Debug, panic::Location};
use timely::dataflow::{
    channels::pact::Pipeline,
    operators::{Inspect, Operator},
    Scope, Stream,
};

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

    #[track_caller]
    fn debug_with(&self, name: &str) -> Self
    where
        Self: Sized,
        Self::Value: Debug,
    {
        let name = name.to_owned();
        self.debug_inspect(move |value| {
            println!("[{}]: {:?}", name, value);
        })
    }

    fn debug_frontier(&self) -> Self;

    fn debug_frontier_with(&self, with: &str) -> Self;
}

impl<S, D, R> InspectExt for Collection<S, D, R>
where
    S: Scope,
    D: Data,
    R: Semigroup,
{
    type Value = (D, S::Timestamp, R);

    #[track_caller]
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

    #[track_caller]
    fn debug_frontier(&self) -> Self {
        self.inner.debug_frontier().as_collection()
    }

    #[track_caller]
    fn debug_frontier_with(&self, with: &str) -> Self {
        self.inner.debug_frontier_with(with).as_collection()
    }
}

impl<S, D> InspectExt for Stream<S, D>
where
    S: Scope,
    D: Data,
{
    type Value = D;

    #[track_caller]
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

    #[track_caller]
    fn debug_frontier(&self) -> Self {
        if cfg!(debug_assertions) {
            let worker = self.scope().index();
            let caller = Location::caller();
            let mut buffer = Vec::new();

            self.unary_frontier(Pipeline, &located!("InspectFrontier"), move |_, _| {
                move |input, output| {
                    input.for_each(|time, data| {
                        data.swap(&mut buffer);
                        output.session(&time).give_vec(&mut buffer);
                    });

                    tracing::trace!(
                        target: "inspect_frontier",
                        worker = worker,
                        frontier = ?input.frontier().frontier(),
                        "frontier at {}:{}:{}",
                        caller.file(),
                        caller.line(),
                        caller.column(),
                    );
                }
            })
        } else {
            self.clone()
        }
    }

    #[track_caller]
    fn debug_frontier_with(&self, with: &str) -> Self {
        if cfg!(debug_assertions) {
            let with = with.to_owned();
            let worker = self.scope().index();
            let caller = Location::caller();
            let mut buffer = Vec::new();

            self.unary_frontier(Pipeline, &located!("InspectFrontier"), move |_, _| {
                move |input, output| {
                    input.for_each(|time, data| {
                        data.swap(&mut buffer);
                        output.session(&time).give_vec(&mut buffer);
                    });

                    tracing::trace!(
                        target: "inspect_frontier",
                        worker = worker,
                        frontier = ?input.frontier().frontier(),
                        "{} frontier at {}:{}:{}",
                        with,
                        caller.file(),
                        caller.line(),
                        caller.column(),
                    );
                }
            })
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

    #[track_caller]
    fn debug_inspect<F>(&self, inspect: F) -> Self
    where
        F: FnMut(&Self::Value) + 'static,
    {
        self.as_ref().map(|stream| stream.debug_inspect(inspect))
    }

    #[track_caller]
    fn debug_frontier(&self) -> Self {
        self.as_ref().map(|stream| stream.debug_frontier())
    }

    #[track_caller]
    fn debug_frontier_with(&self, with: &str) -> Self {
        self.as_ref().map(|stream| stream.debug_frontier_with(with))
    }
}
