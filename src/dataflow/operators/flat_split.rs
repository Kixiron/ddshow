use differential_dataflow::{difference::Semigroup, AsCollection, Collection};
use timely::{
    dataflow::{
        channels::pact::Pipeline, operators::generic::builder_rc::OperatorBuilder, Scope, Stream,
    },
    Data,
};

pub trait FlatSplit<D, Left, Right> {
    type LeftStream;
    type RightStream;

    fn flat_split<L, LeftIter, RightIter>(&self, logic: L) -> (Self::LeftStream, Self::RightStream)
    where
        L: FnMut(D) -> (LeftIter, RightIter) + 'static,
        LeftIter: IntoIterator<Item = Left>,
        RightIter: IntoIterator<Item = Right>,
    {
        self.flat_split_named("FlatSplit", logic)
    }

    fn flat_split_named<L, LeftIter, RightIter>(
        &self,
        name: &str,
        logic: L,
    ) -> (Self::LeftStream, Self::RightStream)
    where
        L: FnMut(D) -> (LeftIter, RightIter) + 'static,
        LeftIter: IntoIterator<Item = Left>,
        RightIter: IntoIterator<Item = Right>;
}

impl<S, D, Left, Right> FlatSplit<D, Left, Right> for Stream<S, D>
where
    S: Scope,
    D: Data,
    Left: Data,
    Right: Data,
{
    type LeftStream = Stream<S, Left>;
    type RightStream = Stream<S, Right>;

    fn flat_split_named<L, LeftIter, RightIter>(
        &self,
        name: &str,
        mut logic: L,
    ) -> (Self::LeftStream, Self::RightStream)
    where
        L: FnMut(D) -> (LeftIter, RightIter) + 'static,
        LeftIter: IntoIterator<Item = Left>,
        RightIter: IntoIterator<Item = Right>,
    {
        let mut buffer = Vec::new();

        let mut builder = OperatorBuilder::new(name.to_owned(), self.scope());
        builder.set_notify(false);

        let mut input = builder.new_input(self, Pipeline);
        let (mut left_out, left_stream) = builder.new_output();
        let (mut right_out, right_stream) = builder.new_output();

        builder.build(move |_capabilities| {
            move |_frontiers| {
                let (mut left_out, mut right_out) = (left_out.activate(), right_out.activate());

                input.for_each(|capability, data| {
                    data.swap(&mut buffer);

                    let (mut left_session, mut right_session) = (
                        left_out.session(&capability),
                        right_out.session(&capability),
                    );

                    for data in buffer.drain(..) {
                        let (left, right) = logic(data);

                        left_session.give_iterator(left.into_iter());
                        right_session.give_iterator(right.into_iter());
                    }
                });
            }
        });

        (left_stream, right_stream)
    }
}

impl<S, D, R, Left, Right> FlatSplit<D, Left, Right> for Collection<S, D, R>
where
    S: Scope,
    S::Timestamp: Clone,
    D: Data,
    R: Semigroup + Clone,
    Left: Data,
    Right: Data,
{
    type LeftStream = Collection<S, Left, R>;
    type RightStream = Collection<S, Right, R>;

    fn flat_split_named<L, LeftIter, RightIter>(
        &self,
        name: &str,
        mut logic: L,
    ) -> (Self::LeftStream, Self::RightStream)
    where
        L: FnMut(D) -> (LeftIter, RightIter) + 'static,
        LeftIter: IntoIterator<Item = Left>,
        RightIter: IntoIterator<Item = Right>,
    {
        let (left, right) = self
            .inner
            .flat_split_named(name, move |(data, time, diff)| {
                let (left, right) = logic(data);

                let (left_time, left_diff) = (time.clone(), diff.clone());
                let left = left
                    .into_iter()
                    .map(move |left| (left, left_time.clone(), left_diff.clone()));

                let right = right
                    .into_iter()
                    .map(move |right| (right, time.clone(), diff.clone()));

                (left, right)
            });

        (left.as_collection(), right.as_collection())
    }
}
