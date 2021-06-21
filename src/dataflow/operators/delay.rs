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
        let mut idle_buffers = Vec::new();

        self.inner
            .unary_notify(Pipeline, &name, None, move |input, output, notificator| {
                input.for_each(|time, data| {
                    let mut buffer = idle_buffers.pop().unwrap_or_default();
                    data.swap(&mut buffer);

                    // Apply the delay to the timely stream's timestamp
                    let new_time = delay(&time);
                    assert!(time.time().less_equal(&new_time));

                    elements
                        .entry(new_time.clone())
                        .or_insert_with(|| {
                            notificator.notify_at(time.delayed(&new_time));
                            Vec::new()
                        })
                        .push(buffer);
                });

                // for each available notification, send corresponding set
                notificator.for_each(|time, _, _| {
                    if let Some(mut buffers) = elements.remove(&time) {
                        for mut data in buffers.drain(..) {
                            // Apply the delay to the ddflow collection's timestamps
                            for (_data, time, _diff) in data.iter_mut() {
                                let new_time = delay(&*time);
                                debug_assert!(time.less_equal(&new_time));

                                *time = new_time;
                            }

                            output.session(&time).give_vec(&mut data);

                            // Give the freshly empty buffer back to the list of
                            // idle buffers we maintain
                            idle_buffers.push(data);
                        }
                    }
                });

                if elements.capacity() > elements.len() * 4 {
                    elements.shrink_to_fit();
                }

                // Limit the number of extra buffers we keep lying around
                idle_buffers.truncate(16);
                if idle_buffers.capacity() > idle_buffers.len() * 4 {
                    idle_buffers.shrink_to_fit();
                }
            })
            .as_collection()
    }
}
