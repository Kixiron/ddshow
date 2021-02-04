use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use timely::{
    dataflow::{
        channels::pushers::{buffer::Buffer as PushBuffer, Counter as PushCounter},
        operators::{
            capture::event::{Event, EventIterator},
            generic::builder_raw::OperatorBuilder,
        },
    },
    dataflow::{Scope, Stream},
    progress::{frontier::MutableAntichain, Timestamp},
    Data,
};

/// Replay a capture stream into a scope with the same timestamp.
pub trait ReplayWithShutdown<T: Timestamp, D: Data> {
    /// Replays `self` into the provided scope, as a `Stream<S, D>`.
    fn replay_with_shutdown_into<S>(
        self,
        scope: &mut S,
        is_running: Arc<AtomicBool>,
    ) -> Stream<S, D>
    where
        Self: Sized,
        S: Scope<Timestamp = T>,
    {
        self.replay_with_shutdown_into_named("Replay With Shutdown", scope, is_running)
    }

    fn replay_with_shutdown_into_named<N, S>(
        self,
        name: N,
        scope: &mut S,
        is_running: Arc<AtomicBool>,
    ) -> Stream<S, D>
    where
        N: Into<String>,
        S: Scope<Timestamp = T>;
}

impl<T, D, I> ReplayWithShutdown<T, D> for I
where
    T: Timestamp + Default,
    D: Data,
    I: IntoIterator,
    <I as IntoIterator>::Item: EventIterator<T, D> + 'static,
{
    fn replay_with_shutdown_into_named<N, S>(
        self,
        name: N,
        scope: &mut S,
        is_running: Arc<AtomicBool>,
    ) -> Stream<S, D>
    where
        N: Into<String>,
        S: Scope<Timestamp = T>,
    {
        let mut builder = OperatorBuilder::new(name.into(), scope.clone());

        let address = builder.operator_info().address;
        let activator = scope.activator_for(&address);

        let (targets, stream) = builder.new_output();

        let mut output = PushBuffer::new(PushCounter::new(targets));
        let mut event_streams = self.into_iter().collect::<Vec<_>>();
        let mut antichain = MutableAntichain::new();
        let mut started = false;

        builder.build(move |progress| {
            if !started {
                // The first thing we do is modify our capabilities to match the number of streams we manage.
                // This should be a simple change of `self.event_streams.len() - 1`. We only do this once, as
                // our very first action.
                progress.internals[0].update(Default::default(), event_streams.len() as i64 - 1);
                antichain.update_iter(
                    Some((Default::default(), event_streams.len() as i64 - 1)).into_iter(),
                );

                started = true;
            }

            if is_running.load(Ordering::Acquire) {
                for event_stream in event_streams.iter_mut() {
                    while let Some(event) = event_stream.next() {
                        match *event {
                            Event::Progress(ref vec) => {
                                antichain.update_iter(vec.iter().cloned());
                                progress.internals[0].extend(vec.iter().cloned());
                            }
                            Event::Messages(ref time, ref data) => {
                                output.session(time).give_iterator(data.iter().cloned());
                            }
                        }
                    }
                }

                // Always reschedule `replay`.
                activator.activate();

                output.cease();
                output
                    .inner()
                    .produced()
                    .borrow_mut()
                    .drain_into(&mut progress.produceds[0]);
            } else {
                while !antichain.is_empty() {
                    let elements = antichain
                        .frontier()
                        .iter()
                        .map(|t| (t.clone(), -1))
                        .collect::<Vec<_>>();
                    for (t, c) in elements.iter() {
                        progress.internals[0].update(t.clone(), *c);
                    }
                    antichain.update_iter(elements);
                }
            }

            false
        });

        stream
    }
}
