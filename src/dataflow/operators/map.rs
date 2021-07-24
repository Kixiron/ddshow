use differential_dataflow::{difference::Semigroup, AsCollection, Collection};
use timely::{
    dataflow::{channels::pact::Pipeline, operators::Operator, Scope, Stream},
    Data,
};

pub trait MapExt<D1, D2> {
    type Output;

    fn map_named<L>(&self, name: &str, logic: L) -> Self::Output
    where
        D1: Data,
        D2: Data,
        L: FnMut(D1) -> D2 + 'static;
}

impl<S, D1, D2> MapExt<D1, D2> for Stream<S, D1>
where
    S: Scope,
    D1: Data,
{
    type Output = Stream<S, D2>;

    fn map_named<L>(&self, name: &str, mut logic: L) -> Self::Output
    where
        D1: Data,
        D2: Data,
        L: FnMut(D1) -> D2 + 'static,
    {
        let mut buffer = Vec::new();

        self.unary(Pipeline, name, move |_, _| {
            move |input, output| {
                input.for_each(|time, data| {
                    data.swap(&mut buffer);

                    output
                        .session(&time)
                        .give_iterator(buffer.drain(..).map(|x| logic(x)));
                });
            }
        })
    }
}

impl<S, D1, D2, R> MapExt<D1, D2> for Collection<S, D1, R>
where
    S: Scope,
    D1: Data,
    R: Semigroup,
{
    type Output = Collection<S, D2, R>;

    fn map_named<L>(&self, name: &str, mut logic: L) -> Self::Output
    where
        D1: Data,
        D2: Data,
        L: FnMut(D1) -> D2 + 'static,
    {
        self.inner
            .map_named(name, move |(data, time, diff)| (logic(data), time, diff))
            .as_collection()
    }
}

pub trait MapInPlace<D> {
    type Output;

    fn map_in_place_named<L>(&self, name: &str, logic: L) -> Self::Output
    where
        D: Data,
        L: FnMut(&mut D) + 'static;
}

impl<S, D> MapInPlace<D> for Stream<S, D>
where
    S: Scope,
    D: Data,
{
    type Output = Stream<S, D>;

    fn map_in_place_named<L>(&self, name: &str, mut logic: L) -> Self::Output
    where
        D: Data,
        L: FnMut(&mut D) + 'static,
    {
        let mut buffer = Vec::new();

        self.unary(Pipeline, name, move |_, _| {
            move |input, output| {
                input.for_each(|time, data| {
                    data.swap(&mut buffer);

                    for elem in buffer.iter_mut() {
                        logic(elem);
                    }

                    output.session(&time).give_vec(&mut buffer);
                });
            }
        })
    }
}

impl<S, D, R> MapInPlace<D> for Collection<S, D, R>
where
    S: Scope,
    D: Data,
    R: Semigroup,
{
    type Output = Collection<S, D, R>;

    fn map_in_place_named<L>(&self, name: &str, mut logic: L) -> Self::Output
    where
        D: Data,
        L: FnMut(&mut D) + 'static,
    {
        self.inner
            .map_in_place_named(name, move |(data, _time, _diff)| logic(data))
            .as_collection()
    }
}

pub trait MapTimed<T, D1, D2> {
    type Output;

    #[track_caller]
    fn map_timed<L>(&self, logic: L) -> Self::Output
    where
        D1: Data,
        D2: Data,
        L: FnMut(&T, D1) -> D2 + 'static,
    {
        self.map_timed_named(&located!("MapTimed"), logic)
    }

    fn map_timed_named<L>(&self, name: &str, logic: L) -> Self::Output
    where
        D1: Data,
        D2: Data,
        L: FnMut(&T, D1) -> D2 + 'static;
}

impl<S, D1, D2> MapTimed<S::Timestamp, D1, D2> for Stream<S, D1>
where
    S: Scope,
    D1: Data,
{
    type Output = Stream<S, D2>;

    fn map_timed_named<L>(&self, name: &str, mut logic: L) -> Self::Output
    where
        D1: Data,
        D2: Data,
        L: FnMut(&S::Timestamp, D1) -> D2 + 'static,
    {
        let mut buffer = Vec::new();

        self.unary(Pipeline, name, move |_, _| {
            move |input, output| {
                input.for_each(|time, data| {
                    data.swap(&mut buffer);

                    output
                        .session(&time)
                        .give_iterator(buffer.drain(..).map(|x| logic(&time, x)));
                });
            }
        })
    }
}

impl<S, D1, D2, R> MapTimed<S::Timestamp, D1, D2> for Collection<S, D1, R>
where
    S: Scope,
    D1: Data,
    R: Semigroup,
{
    type Output = Collection<S, D2, R>;

    fn map_timed_named<L>(&self, name: &str, mut logic: L) -> Self::Output
    where
        D1: Data,
        D2: Data,
        L: FnMut(&S::Timestamp, D1) -> D2 + 'static,
    {
        self.inner
            .map_named(name, move |(data, time, diff)| {
                (logic(&time, data), time, diff)
            })
            .as_collection()
    }
}
