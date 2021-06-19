use differential_dataflow::{difference::Semigroup, AsCollection, Collection, Data};
use std::{collections::HashMap, panic::Location};
use timely::{
    dataflow::{channels::pact::Pipeline, operators::Operator, Scope},
    progress::Timestamp,
    PartialOrder,
};

pub trait DelayExt<T> {
    type Output;

    fn delay_fast<F>(&self, delay: F) -> Self::Output
    where
        F: FnMut(&T) -> T + Clone + 'static;
}

impl<S, D, R> DelayExt<S::Timestamp> for Collection<S, D, R>
where
    S: Scope,
    S::Timestamp: Timestamp,
    D: Data,
    R: Semigroup + Data,
{
    type Output = Self;

    #[track_caller]
    fn delay_fast<F>(&self, mut delay: F) -> Self::Output
    where
        F: FnMut(&S::Timestamp) -> S::Timestamp + Clone + 'static,
    {
        let caller = Location::caller();
        let name = format!(
            "Delay @ {}:{}:{}",
            caller.file(),
            caller.line(),
            caller.column()
        );

        let mut elements = HashMap::new();
        self.inner
            .unary_notify(Pipeline, &name, None, move |input, output, notificator| {
                input.for_each(|time, data| {
                    let new_time = delay(&time);
                    assert!(time.time().less_equal(&new_time));

                    elements
                        .entry(new_time.clone())
                        .or_insert_with(|| {
                            notificator.notify_at(time.delayed(&new_time));
                            Vec::new()
                        })
                        .push(data.replace(Vec::new()));
                });

                // for each available notification, send corresponding set
                notificator.for_each(|time, _, _| {
                    if let Some(mut buffer) = elements.remove(&time) {
                        for data in buffer.drain(..) {
                            output.session(&time).give_iterator(
                                data.into_iter()
                                    .map(|(data, time, diff)| (data, delay(&time), diff)),
                            );
                        }
                    }
                });
            })
            .as_collection()
    }
}
