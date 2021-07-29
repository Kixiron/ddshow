use crate::dataflow::{constants::DEFAULT_REACTIVATION_DELAY, operators::util::Fuel};
use abomonation::Abomonation;
use indicatif::ProgressBar;
use std::{
    convert::identity,
    fmt::Debug,
    io::{self, Read, Write},
    iter,
    marker::PhantomData,
    mem,
    panic::Location,
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
    logging::{
        InputEvent as RawInputEvent, StartStop as RawStartStop, TimelyEvent as RawTimelyEvent,
        TimelyLogger,
    },
    progress::{frontier::MutableAntichain, Timestamp},
    Data,
};

/// Iterates over contained `Event<T, D>`.
///
/// The `EventIterator` trait describes types that can iterate over references to events,
/// and which can be used to replay a stream into a new timely dataflow computation.
///
/// This method is not simply an iterator because of the lifetime in the result.
pub trait EventIterator<T, D> {
    /// Iterates over references to `Event<T, D>` elements.
    fn next(
        &mut self,
        is_finished: &mut bool,
        bytes_read: &mut usize,
    ) -> io::Result<Option<Event<T, D>>>;

    fn take_events(&mut self) -> io::Result<Vec<Event<T, D>>> {
        let (mut events, mut is_finished) = (Vec::new(), false);

        while !is_finished {
            if let Some(event) = self.next(&mut is_finished, &mut 0)? {
                events.push(event);
            }
        }

        Ok(events)
    }
}

impl<I, T, D> EventIterator<T, D> for Box<I>
where
    I: EventIterator<T, D>,
{
    fn next(
        &mut self,
        is_finished: &mut bool,
        bytes_read: &mut usize,
    ) -> io::Result<Option<Event<T, D>>> {
        self.as_mut().next(is_finished, bytes_read)
    }
}

impl<T, D> EventIterator<T, D> for Box<dyn EventIterator<T, D>> {
    fn next(
        &mut self,
        is_finished: &mut bool,
        bytes_read: &mut usize,
    ) -> io::Result<Option<Event<T, D>>> {
        self.as_mut().next(is_finished, bytes_read)
    }
}

impl<T, D> EventIterator<T, D> for Box<dyn EventIterator<T, D> + Send + 'static> {
    fn next(
        &mut self,
        is_finished: &mut bool,
        bytes_read: &mut usize,
    ) -> io::Result<Option<Event<T, D>>> {
        self.as_mut().next(is_finished, bytes_read)
    }
}

/// A Wrapper for `R: Read` implementing `EventIterator<T, D>`.
#[derive(Debug)]
pub struct EventReader<T, D, R> {
    reader: R,
    bytes: Vec<u8>,
    buff1: Vec<u8>,
    buff2: Vec<u8>,
    consumed: usize,
    valid: usize,
    peer_finished: bool,
    retried: bool,
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
            peer_finished: false,
            retried: false,
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
    fn next(
        &mut self,
        is_finished: &mut bool,
        bytes_read: &mut usize,
    ) -> io::Result<Option<Event<T, D>>> {
        if self.peer_finished && self.retried {
            *is_finished = true;
            return Ok(None);

        // FIXME: This could potentially cause some data to be lost
        } else if self.peer_finished {
            self.retried = true;
        }

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
            *bytes_read += len;
            if len == 0 {
                self.peer_finished = true;
            }

            self.buff1.write_all(&self.bytes[..len])?;
            self.valid = self.buff1.len();
        }

        Ok(None)
    }
}

/// Replay a capture stream into a scope with the same timestamp.
pub trait ReplayWithShutdown<T, D>
where
    T: Timestamp,
    D: Data,
{
    /// Replays `self` into the provided scope, as a `Stream<S, D>`.
    #[track_caller]
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
            Fuel::unlimited(),
            DEFAULT_REACTIVATION_DELAY,
            None,
        )
    }

    #[track_caller]
    fn replay_with_shutdown_into_named<N, S>(
        self,
        name: N,
        scope: &mut S,
        is_running: Arc<AtomicBool>,
        fuel: Fuel,
        progress_bar: Option<ProgressBar>,
    ) -> Stream<S, D>
    where
        Self: Sized,
        N: Into<String>,
        S: Scope<Timestamp = T>,
    {
        self.replay_with_shutdown_into_core(
            name,
            scope,
            is_running,
            fuel,
            DEFAULT_REACTIVATION_DELAY,
            progress_bar,
        )
    }

    fn replay_with_shutdown_into_core<N, S>(
        self,
        name: N,
        scope: &mut S,
        is_running: Arc<AtomicBool>,
        fuel: Fuel,
        reactivation_delay: Duration,
        progress_bar: Option<ProgressBar>,
    ) -> Stream<S, D>
    where
        N: Into<String>,
        S: Scope<Timestamp = T>;
}

// impl<T, D, I> ReplayWithShutdown<Epoch<Duration, T>, D> for I
impl<T, D, I> ReplayWithShutdown<T, D> for I
where
    T: Timestamp + Default,
    D: Debug + Data,
    I: IntoIterator,
    <I as IntoIterator>::Item: EventIterator<T, D> + 'static,
{
    #[track_caller]
    fn replay_with_shutdown_into_core<N, S>(
        self,
        name: N,
        scope: &mut S,
        is_running: Arc<AtomicBool>,
        mut fuel: Fuel,
        reactivation_delay: Duration,
        progress_bar: Option<ProgressBar>,
    ) -> Stream<S, D>
    where
        N: Into<String>,
        // S: Scope<Timestamp = Epoch<Duration, T>>,
        S: Scope<Timestamp = T>,
    {
        let worker_index = scope.index();
        let caller = Location::caller();
        let mut event_streams = self.into_iter().collect::<Vec<_>>();

        tracing::trace!(
            caller = ?caller,
            worker_index = worker_index,
            fuel = ?fuel,
            reactivation_delay = ?reactivation_delay,
            "started up ReplayWithShutdown instance with {} event streams",
            event_streams.len(),
        );

        let mut builder = OperatorBuilder::new(
            format!(
                "{} @ {}:{}:{}",
                name.into(),
                caller.file(),
                caller.line(),
                caller.column(),
            ),
            scope.clone(),
        );
        builder.set_notify(true);

        let address = builder.operator_info().address;
        let activator = scope.activator_for(&address);

        let (targets, stream) = builder.new_output();

        let mut output = PushBuffer::new(PushCounter::new(targets));

        let mut antichain = MutableAntichain::new();
        let (mut started, mut streams_finished, mut bytes_read) = (
            false,
            vec![false; event_streams.len()],
            vec![0; event_streams.len()],
        );

        let logger: Option<TimelyLogger> = scope.log_register().get("timely");
        // let start_time = Instant::now();

        builder.build(move |progress| {
            if let Some(logger) = logger.as_ref() {
                logger.log(RawTimelyEvent::Input(RawInputEvent {
                    start_stop: RawStartStop::Start,
                }));
            }

            if !started {
                tracing::debug!(
                    "acquired {} capabilities from within `.replay_with_shutdown_into_core()`",
                    event_streams.len() as i64 - 1,
                );

                // // Remove the default timestamp given to us by timely
                // progress.internals[0].update(S::Timestamp::minimum(), -1);

                // The first thing we do is modify our capabilities to match the number of streams we manage.
                // This should be a simple change of `event_streams.len() - 1`. We only do this once, as
                // our very first action.
                progress.internals[0]
                    .update(S::Timestamp::minimum(), event_streams.len() as i64 - 1);
                antichain.update_iter(iter::once((
                    S::Timestamp::minimum(),
                    event_streams.len() as i64,
                )));

                started = true;
            }

            // let system_time = start_time.elapsed();
            fuel.reset();

            'event_loop: for (stream_idx, event_stream) in event_streams.iter_mut().enumerate() {
                'stream_loop: loop {
                    let next = event_stream.next(
                        &mut streams_finished[stream_idx],
                        &mut bytes_read[stream_idx],
                    );

                    if bytes_read[stream_idx] != 0 {
                        tracing::trace!(
                            target: "replay_bytes_read",
                            worker = worker_index,
                            bytes_read = bytes_read[stream_idx],
                        );
                    }

                    match next {
                        Ok(Some(event)) => match event {
                            Event::Progress(vec) => {
                                // Exert a little bit of effort for propagating timestamps
                                // fuel.exert(1);

                                progress.internals[0].extend(vec.iter().cloned());
                                antichain.update_iter(vec);

                                // progress.internals[0].extend(vec.iter().cloned().map(
                                //     |(data_time, diff)| (Epoch::new(system_time, data_time), diff),
                                // ));
                                // antichain.update_iter(vec.into_iter().map(|(data_time, diff)| {
                                //     (Epoch::new(system_time, data_time), diff)
                                // }));
                            }

                            Event::Messages(time, mut data) => {
                                let data_len = data.len();

                                // Update the progress bar with the number of messages we've ingested
                                if let Some(bar) = progress_bar.as_ref() {
                                    bar.inc_length(data_len as u64);
                                }

                                // Exert effort for each record we receive
                                fuel.exert(data_len);

                                output.session(&time).give_vec(&mut data);

                                if let Some(bar) = progress_bar.as_ref() {
                                    bar.inc(data_len as u64);
                                }
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

                    if fuel.is_exhausted() {
                        break 'event_loop;
                    }
                }

                if fuel.is_exhausted() {
                    break 'event_loop;
                }
            }

            tracing::debug!(
                target: "replay_frontier",
                worker = worker_index,
                frontier = ?antichain,
            );

            let all_streams_finished = streams_finished.iter().copied().all(identity);

            output.cease();
            output
                .inner()
                .produced()
                .borrow_mut()
                .drain_into(&mut progress.produceds[0]);

            // Reactivate according to the re-activation delay
            activator.activate_after(reactivation_delay);

            // If we're supposed to be running and haven't completed our input streams,
            // flush the output & re-activate ourselves after a delay
            let needs_reactivation = if is_running.load(Ordering::Acquire) && !all_streams_finished
            {
                // Tell timely we have work left to do
                // true
                true

            // If we're not supposed to be running or all input streams are finished,
            // flush our outputs and release all outstanding capabilities so that
            // any downstream consumers know we're done
            } else {
                let reason = if all_streams_finished {
                    "all streams have finished"
                } else {
                    "is_running was set to false"
                };

                tracing::info!(
                    worker = worker_index,
                    is_running = is_running.load(Ordering::Acquire),
                    all_streams_finished = all_streams_finished,
                    "received shutdown signal within event replay: {}",
                    reason,
                );

                // Release all outstanding capabilities
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

                if let Some(bar) = progress_bar.as_ref() {
                    if started {
                        bar.finish_using_style();
                    }
                }

                // Tell timely we're completely done
                false
            };

            if let Some(logger) = logger.as_ref() {
                logger.log(RawTimelyEvent::Input(RawInputEvent {
                    start_stop: RawStartStop::Stop,
                }));
            }

            needs_reactivation
        });

        stream
    }
}
