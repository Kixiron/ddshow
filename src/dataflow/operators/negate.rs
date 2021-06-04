use crate::dataflow::operators::MapInPlace;
use differential_dataflow::{difference::Abelian, AsCollection, Collection, Data};
use std::panic::Location;
use timely::dataflow::Scope;

pub trait NegateExt {
    type Output;

    fn negate_named(&self, name: &str) -> Self::Output;

    #[track_caller]
    fn negate_located(&self) -> Self::Output;
}

impl<S, D, R> NegateExt for Collection<S, D, R>
where
    S: Scope,
    D: Data,
    R: Abelian,
{
    type Output = Self;

    fn negate_named(&self, name: &str) -> Self::Output {
        self.inner
            .map_in_place_named(name, |x| x.2 = -x.2.clone())
            .as_collection()
    }

    #[track_caller]
    fn negate_located(&self) -> Self::Output {
        let caller = Location::caller();
        let name = format!(
            "MapInPlace: Negate @ {}:{}:{}",
            caller.file(),
            caller.line(),
            caller.column(),
        );

        self.negate_named(&name)
    }
}
