use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
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
    progress::Timestamp,
    Data,
};

const DEFAULT_REACTIVATION_DELAY: Duration = Duration::from_millis(200);

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
        self.replay_with_shutdown_into_core(
            "ReplayWithShutdown",
            scope,
            is_running,
            DEFAULT_REACTIVATION_DELAY,
        )
    }

    fn replay_with_shutdown_into_named<N, S>(
        self,
        name: N,
        scope: &mut S,
        is_running: Arc<AtomicBool>,
    ) -> Stream<S, D>
    where
        Self: Sized,
        N: Into<String>,
        S: Scope<Timestamp = T>,
    {
        self.replay_with_shutdown_into_core(name, scope, is_running, DEFAULT_REACTIVATION_DELAY)
    }

    fn replay_with_shutdown_into_core<N, S>(
        self,
        name: N,
        scope: &mut S,
        is_running: Arc<AtomicBool>,
        reactivation_delay: Duration,
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
    fn replay_with_shutdown_into_core<N, S>(
        self,
        name: N,
        scope: &mut S,
        is_running: Arc<AtomicBool>,
        reactivation_delay: Duration,
    ) -> Stream<S, D>
    where
        N: Into<String>,
        S: Scope<Timestamp = T>,
    {
        let mut builder = OperatorBuilder::new(name.into(), scope.clone());

        let address = builder.operator_info().address;
        let activator = scope.activator_for(&address[..]);

        let (targets, stream) = builder.new_output();

        let mut output = PushBuffer::new(PushCounter::new(targets));
        let mut event_streams = self.into_iter().collect::<Vec<_>>();
        let mut started = false;

        builder.build(move |progress| {
            if !started {
                // The first thing we do is modify our capabilities to match the number of streams we manage.
                // This should be a simple change of `self.event_streams.len() - 1`. We only do this once, as
                // our very first action.
                progress.internals[0]
                    .update(S::Timestamp::minimum(), (event_streams.len() as i64) - 1);
                started = true;
            }

            for event_stream in event_streams.iter_mut() {
                while let Some(event) = event_stream.next() {
                    match *event {
                        Event::Progress(ref vec) => {
                            progress.internals[0].extend(vec.iter().cloned());
                        }
                        Event::Messages(ref time, ref data) => {
                            output.session(time).give_iterator(data.iter().cloned());
                        }
                    }
                }
            }

            // Reactivate according to the re-activation delay
            activator.activate_after(reactivation_delay);

            output.cease();
            output
                .inner()
                .produced()
                .borrow_mut()
                .drain_into(&mut progress.produceds[0]);

            !is_running.load(Ordering::Acquire)
        });

        stream
    }
}
