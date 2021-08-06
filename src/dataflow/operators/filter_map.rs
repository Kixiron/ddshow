use std::panic::Location;

use differential_dataflow::{collection::AsCollection, difference::Semigroup, Collection};
use timely::{
    communication::message::RefOrMut,
    dataflow::{channels::pact::Pipeline, operators::Operator, Scope, Stream},
    Data,
};

pub trait FilterMap<D, D2> {
    type Output;

    #[inline]
    #[track_caller]
    fn filter_map<L>(&self, logic: L) -> Self::Output
    where
        L: FnMut(D) -> Option<D2> + 'static,
    {
        let caller = Location::caller();

        self.filter_map_named(
            &format!(
                "FilterMap @ {}:{}:{}",
                caller.file(),
                caller.line(),
                caller.column(),
            ),
            logic,
        )
    }

    fn filter_map_named<L>(&self, name: &str, logic: L) -> Self::Output
    where
        L: FnMut(D) -> Option<D2> + 'static;

    fn filter_map_ref_named<L>(&self, name: &str, logic: L) -> Self::Output
    where
        L: FnMut(&D) -> Option<D2> + 'static;
}

impl<S, D, D2> FilterMap<D, D2> for Stream<S, D>
where
    S: Scope,
    D: Data,
    D2: Data,
{
    type Output = Stream<S, D2>;

    #[inline]
    fn filter_map_named<L>(&self, name: &str, mut logic: L) -> Self::Output
    where
        L: FnMut(D) -> Option<D2> + 'static,
    {
        let mut buffer = Vec::new();

        self.unary(Pipeline, name, move |_capability, _info| {
            move |input, output| {
                input.for_each(|capability, data| {
                    data.swap(&mut buffer);

                    output
                        .session(&capability)
                        .give_iterator(buffer.drain(..).filter_map(|data| logic(data)));
                });
            }
        })
    }

    #[inline]
    fn filter_map_ref_named<L>(&self, name: &str, mut logic: L) -> Self::Output
    where
        L: FnMut(&D) -> Option<D2> + 'static,
    {
        self.unary(Pipeline, name, move |_capability, _info| {
            move |input, output| {
                input.for_each(|capability, data| {
                    let buffer = match data {
                        RefOrMut::Ref(data) => data,
                        RefOrMut::Mut(ref data) => &**data,
                    };

                    output
                        .session(&capability)
                        .give_iterator(buffer.iter().filter_map(|data| logic(data)));

                    if let RefOrMut::Mut(data) = data {
                        data.clear();
                    }
                });
            }
        })
    }
}

impl<S, D, D2, R> FilterMap<D, D2> for Collection<S, D, R>
where
    S: Scope,
    S::Timestamp: Clone,
    D: Data,
    D2: Data,
    R: Semigroup,
{
    type Output = Collection<S, D2, R>;

    #[inline]
    fn filter_map_named<L>(&self, name: &str, mut logic: L) -> Self::Output
    where
        L: FnMut(D) -> Option<D2> + 'static,
    {
        self.inner
            .filter_map_named(name, move |(data, time, diff)| {
                logic(data).map(|data| (data, time, diff))
            })
            .as_collection()
    }

    #[inline]
    fn filter_map_ref_named<L>(&self, name: &str, mut logic: L) -> Self::Output
    where
        L: FnMut(&D) -> Option<D2> + 'static,
    {
        self.inner
            .filter_map_ref_named(name, move |(data, time, diff)| {
                logic(data).map(|data| (data, time.clone(), diff.clone()))
            })
            .as_collection()
    }
}

pub trait FilterMapTimed<T, D, D2> {
    type Output;

    #[inline]
    #[track_caller]
    fn filter_map_timed<L>(&self, logic: L) -> Self::Output
    where
        L: FnMut(&T, D) -> Option<D2> + 'static,
    {
        let caller = Location::caller();

        self.filter_map_timed_named(
            &format!(
                "FilterMap @ {}:{}:{}",
                caller.file(),
                caller.line(),
                caller.column(),
            ),
            logic,
        )
    }

    fn filter_map_timed_named<L>(&self, name: &str, logic: L) -> Self::Output
    where
        L: FnMut(&T, D) -> Option<D2> + 'static;

    fn filter_map_ref_timed_named<L>(&self, name: &str, logic: L) -> Self::Output
    where
        L: FnMut(&T, &D) -> Option<D2> + 'static;
}

impl<S, D, D2> FilterMapTimed<S::Timestamp, D, D2> for Stream<S, D>
where
    S: Scope,
    D: Data,
    D2: Data,
{
    type Output = Stream<S, D2>;

    #[inline]
    fn filter_map_timed_named<L>(&self, name: &str, mut logic: L) -> Self::Output
    where
        L: FnMut(&S::Timestamp, D) -> Option<D2> + 'static,
    {
        let mut buffer = Vec::new();
        self.unary(Pipeline, name, move |_capability, _info| {
            move |input, output| {
                input.for_each(|time, data| {
                    data.swap(&mut buffer);

                    output
                        .session(&time)
                        .give_iterator(buffer.drain(..).filter_map(|data| logic(&time, data)));
                });
            }
        })
    }

    #[inline]
    fn filter_map_ref_timed_named<L>(&self, name: &str, mut logic: L) -> Self::Output
    where
        L: FnMut(&S::Timestamp, &D) -> Option<D2> + 'static,
    {
        self.unary(Pipeline, name, move |_capability, _info| {
            move |input, output| {
                input.for_each(|time, data| {
                    let buffer = match data {
                        RefOrMut::Ref(data) => data,
                        RefOrMut::Mut(ref data) => &**data,
                    };

                    output
                        .session(&time)
                        .give_iterator(buffer.iter().filter_map(|data| logic(&time, data)));

                    if let RefOrMut::Mut(data) = data {
                        data.clear();
                    }
                });
            }
        })
    }
}

impl<S, D, D2, R> FilterMapTimed<S::Timestamp, D, D2> for Collection<S, D, R>
where
    S: Scope,
    S::Timestamp: Clone,
    D: Data,
    D2: Data,
    R: Semigroup,
{
    type Output = Collection<S, D2, R>;

    #[inline]
    fn filter_map_timed_named<L>(&self, name: &str, mut logic: L) -> Self::Output
    where
        L: FnMut(&S::Timestamp, D) -> Option<D2> + 'static,
    {
        self.inner
            .filter_map_named(name, move |(data, time, diff)| {
                logic(&time, data).map(|data| (data, time, diff))
            })
            .as_collection()
    }

    #[inline]
    fn filter_map_ref_timed_named<L>(&self, name: &str, mut logic: L) -> Self::Output
    where
        L: FnMut(&S::Timestamp, &D) -> Option<D2> + 'static,
    {
        self.inner
            .filter_map_ref_named(name, move |(data, time, diff)| {
                logic(time, data).map(|data| (data, time.clone(), diff.clone()))
            })
            .as_collection()
    }
}
