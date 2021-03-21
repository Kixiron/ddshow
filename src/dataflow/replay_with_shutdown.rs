use anyhow::Result;
use crossbeam_channel::Receiver;
use std::{
    io::Read,
    iter,
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
    dataflow::{operators::capture::EventReader, Scope, Stream},
    progress::{frontier::MutableAntichain, Timestamp},
    Data,
};

const DEFAULT_REACTIVATION_DELAY: Duration = Duration::from_millis(200);

type EventReceivers<T, D, R> = Arc<[Receiver<Vec<EventReader<T, D, R>>>]>;

pub fn make_streams<I, T, D, R>(num_workers: usize, sources: I) -> Result<EventReceivers<T, D, R>>
where
    I: IntoIterator<Item = R>,
    R: Read,
{
    let mut readers = Vec::with_capacity(num_workers);
    readers.extend(iter::repeat_with(Vec::new).take(num_workers));

    for (idx, source) in sources.into_iter().enumerate() {
        readers[idx % num_workers].push(EventReader::new(source));
    }

    let (senders, receivers): (Vec<_>, Vec<_>) = (0..num_workers)
        .map(|_| crossbeam_channel::bounded(1))
        .unzip();

    for (sender, bundle) in senders.into_iter().zip(readers) {
        sender
            .send(bundle)
            .map_err(|_| anyhow::anyhow!("failed to send events to worker"))?;
    }

    Ok(Arc::from(receivers))
}

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
                progress.internals[0]
                    .update(S::Timestamp::minimum(), (event_streams.len() as i64) - 1);
                antichain.update_iter(
                    Some((Default::default(), event_streams.len() as i64 - 1)).into_iter(),
                );

                started = true;
            }

            let mut fuel: usize = 1_000_000;
            for event_stream in event_streams.iter_mut() {
                while let Some(event) = event_stream.next() {
                    match event {
                        Event::Progress(vec) => {
                            progress.internals[0].extend(vec.iter().cloned());
                            antichain.update_iter(vec.iter().cloned());
                        }
                        Event::Messages(time, data) => {
                            output.session(time).give_iterator(data.iter().cloned());
                            fuel = fuel.saturating_sub(data.len());
                        }
                    }

                    if fuel == 0 {
                        break;
                    }
                }

                if fuel == 0 {
                    break;
                }
            }

            if is_running.load(Ordering::Acquire) {
                // Reactivate according to the re-activation delay
                activator.activate_after(reactivation_delay);

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
                        .map(|time| (time.clone(), -1))
                        .collect::<Vec<_>>();

                    for (time, change) in elements.iter() {
                        progress.internals[0].update(time.clone(), *change);
                    }

                    antichain.update_iter(elements);
                }
            }

            // Return whether or not we're incomplete, which we base
            // off of whether or not we're still supposed to be running
            is_running.load(Ordering::Acquire)
        });

        stream
    }
}
