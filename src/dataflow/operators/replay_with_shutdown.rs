use abomonation::Abomonation;
use anyhow::Result;
use crossbeam_channel::Receiver;
use std::{
    io::{self, Read, Write},
    iter,
    marker::PhantomData,
    mem,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use timely::{
    dataflow::{
        channels::pushers::{buffer::Buffer as PushBuffer, Counter as PushCounter},
        operators::{capture::event::Event, generic::builder_raw::OperatorBuilder},
    },
    dataflow::{Scope, Stream},
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

/// Iterates over contained `Event<T, D>`.
///
/// The `EventIterator` trait describes types that can iterate over references to events,
/// and which can be used to replay a stream into a new timely dataflow computation.
///
/// This method is not simply an iterator because of the lifetime in the result.
pub trait EventIterator<T, D> {
    /// Iterates over references to `Event<T, D>` elements.
    fn next(&mut self) -> io::Result<Option<Event<T, D>>>;
}

/// A Wrapper for `R: Read` implementing `EventIterator<T, D>`.
pub struct EventReader<T, D, R> {
    reader: R,
    bytes: Vec<u8>,
    buff1: Vec<u8>,
    buff2: Vec<u8>,
    consumed: usize,
    valid: usize,

    __type: PhantomData<(T, D)>,
}

impl<T, D, R> EventReader<T, D, R> {
    /// Allocates a new `EventReader` wrapping a supplied reader.
    pub fn new(reader: R) -> EventReader<T, D, R> {
        EventReader {
            reader,
            bytes: vec![0u8; 1 << 20],
            buff1: Vec::new(),
            buff2: Vec::new(),
            consumed: 0,
            valid: 0,

            __type: PhantomData,
        }
    }
}

impl<T, D, R> EventIterator<T, D> for EventReader<T, D, R>
where
    Event<T, D>: Clone,
    T: Abomonation,
    D: Abomonation,
    R: Read,
{
    fn next(&mut self) -> io::Result<Option<Event<T, D>>> {
        // if we can decode something, we should just return it! :D
        if let Some((event, rest)) =
            unsafe { abomonation::decode::<Event<T, D>>(&mut self.buff1[self.consumed..]) }
        {
            self.consumed = self.valid - rest.len();
            return Ok(Some(event.clone()));
        }

        // if we exhaust data we should shift back (if any shifting to do)
        if self.consumed > 0 {
            self.buff2.clear();
            self.buff2.write_all(&self.buff1[self.consumed..])?;

            mem::swap(&mut self.buff1, &mut self.buff2);
            self.valid = self.buff1.len();
            self.consumed = 0;
        }

        if let Ok(len) = self.reader.read(&mut self.bytes[..]) {
            self.buff1.write_all(&self.bytes[..len])?;
            self.valid = self.buff1.len();
        }

        Ok(None)
    }
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
        let worker_index = scope.index();
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

            'event_loop: for event_stream in event_streams.iter_mut() {
                'stream_loop: loop {
                    let next = event_stream.next();

                    match next {
                        Ok(Some(event)) => match event {
                            Event::Progress(vec) => {
                                progress.internals[0].extend(vec.iter().cloned());
                                antichain.update_iter(vec.into_iter());
                            }

                            Event::Messages(time, mut data) => {
                                output.session(&time).give_vec(&mut data);
                            }
                        },

                        Ok(None) => {
                            if !is_running.load(Ordering::Acquire) {
                                break 'event_loop;
                            } else {
                                break 'stream_loop;
                            }
                        }

                        Err(err) => {
                            tracing::error!(
                                "encountered an error from the event stream: {:?}",
                                err,
                            );
                            is_running.store(false, Ordering::Release);

                            break 'event_loop;
                        }
                    }
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
                tracing::info!(
                    worker = worker_index,
                    "received shutdown signal within event replay",
                );

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
