use differential_dataflow::{difference::Semigroup, AsCollection, Collection};
use timely::{
    dataflow::{
        channels::pact::Pipeline, operators::generic::builder_rc::OperatorBuilder, Scope, Stream,
    },
    Data,
};

pub trait Split<D, Left, Right> {
    type LeftStream;
    type RightStream;

    fn split<L>(&self, logic: L) -> (Self::LeftStream, Self::RightStream)
    where
        L: FnMut(D) -> (Left, Right) + 'static,
    {
        self.split_named("Split", logic)
    }

    fn split_named<L>(&self, name: &str, logic: L) -> (Self::LeftStream, Self::RightStream)
    where
        L: FnMut(D) -> (Left, Right) + 'static;
}

impl<S, D, Left, Right> Split<D, Left, Right> for Stream<S, D>
where
    S: Scope,
    D: Data,
    Left: Data,
    Right: Data,
{
    type LeftStream = Stream<S, Left>;
    type RightStream = Stream<S, Right>;

    fn split_named<L>(&self, name: &str, mut logic: L) -> (Self::LeftStream, Self::RightStream)
    where
        L: FnMut(D) -> (Left, Right) + 'static,
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

                        left_session.give(left);
                        right_session.give(right);
                    }
                });
            }
        });

        (left_stream, right_stream)
    }
}

impl<S, D, R, Left, Right> Split<D, Left, Right> for Collection<S, D, R>
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

    fn split_named<L>(&self, name: &str, mut logic: L) -> (Self::LeftStream, Self::RightStream)
    where
        L: FnMut(D) -> (Left, Right) + 'static,
    {
        let (left, right) = self.inner.split_named(name, move |(data, time, diff)| {
            let (left, right) = logic(data);
            ((left, time.clone(), diff.clone()), (right, time, diff))
        });

        (left.as_collection(), right.as_collection())
    }
}
