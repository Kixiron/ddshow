use crate::dataflow::{
    constants::{DEFAULT_REACTIVATION_DELAY, FILE_SOURCED_FUEL},
    operators::util::Fuel,
    utils::JoinOnDrop,
};
use abomonation::Abomonation;
use crossbeam_channel::TryRecvError;
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
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    thread::Builder,
    time::Duration,
};
use timely::{
    dataflow::{
        channels::pushers::{buffer::Buffer as PushBuffer, Counter as PushCounter},
        operators::{capture::event::Event, generic::builder_raw::OperatorBuilder},
    },
    dataflow::{ProbeHandle, Scope, Stream},
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
    #[inline]
    #[track_caller]
    fn replay_with_shutdown_into<S>(
        self,
        scope: &mut S,
        probe: ProbeHandle<S::Timestamp>,
        is_running: Arc<AtomicBool>,
        replays_finished: Arc<AtomicUsize>,
    ) -> Stream<S, D>
    where
        Self: Sized,
        S: Scope<Timestamp = T>,
    {
        self.replay_with_shutdown_into_core(
            "ReplayWithShutdown",
            scope,
            probe,
            is_running,
            replays_finished,
            Fuel::unlimited(),
            DEFAULT_REACTIVATION_DELAY,
            None,
        )
    }

    #[inline]
    #[track_caller]
    #[allow(clippy::too_many_arguments)]
    fn replay_with_shutdown_into_named<N, S>(
        self,
        name: N,
        scope: &mut S,
        probe: ProbeHandle<S::Timestamp>,
        is_running: Arc<AtomicBool>,
        replays_finished: Arc<AtomicUsize>,
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
            probe,
            is_running,
            replays_finished,
            fuel,
            DEFAULT_REACTIVATION_DELAY,
            progress_bar,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn replay_with_shutdown_into_core<N, S>(
        self,
        name: N,
        scope: &mut S,
        probe: ProbeHandle<S::Timestamp>,
        is_running: Arc<AtomicBool>,
        replays_finished: Arc<AtomicUsize>,
        fuel: Fuel,
        reactivation_delay: Duration,
        progress_bar: Option<ProgressBar>,
    ) -> Stream<S, D>
    where
        N: Into<String>,
        S: Scope<Timestamp = T>;
}

impl<T, D, I> ReplayWithShutdown<T, D> for I
where
    T: Timestamp + Default,
    D: Debug + Data + Send,
    I: IntoIterator,
    <I as IntoIterator>::Item: EventIterator<T, D> + Send + 'static,
{
    #[track_caller]
    fn replay_with_shutdown_into_core<N, S>(
        self,
        name: N,
        scope: &mut S,
        // TODO: Implement this
        _probe: ProbeHandle<T>,
        is_running: Arc<AtomicBool>,
        replays_finished: Arc<AtomicUsize>,
        mut fuel: Fuel,
        reactivation_delay: Duration,
        progress_bar: Option<ProgressBar>,
    ) -> Stream<S, D>
    where
        N: Into<String>,
        S: Scope<Timestamp = T>,
    {
        let worker_index = scope.index();
        let caller = Location::caller();
        let mut event_streams = self.into_iter().collect::<Vec<_>>();
        let total_streams = event_streams.len();

        tracing::debug!(
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
        let sync_activator = scope.sync_activator_for(&address);

        let (targets, stream) = builder.new_output();

        let mut output = PushBuffer::new(PushCounter::new(targets));

        let mut antichain = MutableAntichain::new();
        let (mut started, mut disconnected_channels, mut has_incremented) =
            (false, vec![false; total_streams], false);

        let logger: Option<TimelyLogger> = scope.log_register().get("timely");

        let (running, progress) = (is_running.clone(), progress_bar.clone());
        let (senders, receivers): (Vec<_>, Vec<_>) = (0..total_streams)
            .map(|_| crossbeam_channel::bounded(FILE_SOURCED_FUEL.get()))
            .unzip();

        let worker_handle = Builder::new()
            .name(format!("ddshow-replay-worker-{}", worker_index))
            .spawn(move || {
                tracing::debug!(worker = worker_index, "spawned thread worker for replay");

                let (mut streams_finished, mut bytes_read) =
                    (vec![false; total_streams], vec![0; total_streams]);

                while running.load(Ordering::Acquire)
                    && !streams_finished.iter().copied().all(identity)
                {
                    for ((stream_idx, event_stream), channel) in
                        event_streams.iter_mut().enumerate().zip(senders.iter())
                    {
                        'inner: while !streams_finished[stream_idx] {
                            let next = event_stream.next(
                                &mut streams_finished[stream_idx],
                                &mut bytes_read[stream_idx],
                            );

                            match next {
                                Ok(Some(event)) => {
                                    if let Err(err) = sync_activator.activate() {
                                        tracing::error!(
                                            worker = worker_index,
                                            is_running = running.load(Ordering::Acquire),
                                            "replay thread failed to activate replay operator, \
                                                setting is_running to false: {:?}",
                                            err,
                                        );
                                        running.store(false, Ordering::Release);

                                        return;
                                    }

                                    if let Event::Messages(_, data) = &event {
                                        // Update the progress bar with the number of messages we've ingested
                                        if let Some(bar) = progress.as_ref() {
                                            bar.inc_length(data.len() as u64);
                                        }
                                    }

                                    if let Err(err) = channel.send(event) {
                                        tracing::error!(
                                            worker = worker_index,
                                            "failed to send data to replay channel: {:?}",
                                            err,
                                        );
                                        running.store(false, Ordering::Release);

                                        return;
                                    }
                                }
                                Ok(None) => break 'inner,
                                Err(err) => {
                                    tracing::error!(
                                        worker = worker_index,
                                        "encountered an error from the event stream: {:?}",
                                        err,
                                    );
                                    running.store(false, Ordering::Release);

                                    return;
                                }
                            }
                        }
                    }
                }

                tracing::debug!(
                    worker = worker_index,
                    running = running.load(Ordering::Acquire),
                    streams_finished = ?streams_finished,
                    bytes_read = ?bytes_read,
                    "thread worker for replay on worker {} finished",
                    worker_index,
                );
            })
            .expect("the thread's name is valid");
        let worker_handle = JoinOnDrop::new(worker_handle);

        let mut receivers = receivers.into_iter().enumerate().cycle();

        builder.build(move |progress| {
            // Join our worker handle whenever the operator is shut down
            let _worker_handle = &worker_handle;

            if let Some(logger) = logger.as_ref() {
                logger.log(RawTimelyEvent::Input(RawInputEvent {
                    start_stop: RawStartStop::Start,
                }));
            }

            if !started {
                tracing::debug!(
                    "acquired {} capabilities from within `.replay_with_shutdown_into_core()`",
                    total_streams as i64 - 1,
                );

                // The first thing we do is modify our capabilities to match the number of streams we manage.
                // This should be a simple change of `total_streams - 1`. We only do this once, as
                // our very first action.
                progress.internals[0].update(S::Timestamp::minimum(), total_streams as i64 - 1);
                antichain.update_iter(iter::once((S::Timestamp::minimum(), total_streams as i64)));

                started = true;
            } else {
                fuel.reset();

                'outer: for (idx, recv) in receivers.by_ref().take(total_streams) {
                    'inner: while !disconnected_channels[idx] {
                        match recv.try_recv() {
                            Ok(event) => match event {
                                Event::Progress(vec) => {
                                    // Exert a little bit of effort for propagating timestamps
                                    fuel.exert(1);

                                    progress.internals[0].extend(vec.iter().cloned());
                                    antichain.update_iter(vec);
                                }

                                Event::Messages(time, mut data) => {
                                    let data_len = data.len();

                                    // Exert effort for each record we receive
                                    fuel.exert(data_len);

                                    output.session(&time).give_vec(&mut data);

                                    if let Some(bar) = progress_bar.as_ref() {
                                        bar.inc(data_len as u64);
                                    }
                                }
                            },

                            Err(TryRecvError::Empty) => {
                                worker_handle.unpark();

                                if fuel.is_exhausted() || !is_running.load(Ordering::Acquire) {
                                    break 'outer;
                                } else {
                                    break 'inner;
                                }
                            }

                            Err(TryRecvError::Disconnected) => {
                                tracing::debug!(
                                    worker = worker_index,
                                    "channel {} has disconnected from replay",
                                    idx,
                                );

                                disconnected_channels[idx] = true;
                                break 'inner;
                            }
                        }

                        if fuel.is_exhausted() || !is_running.load(Ordering::Acquire) {
                            worker_handle.unpark();
                            break 'outer;
                        }
                    }
                }
            }

            /*
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
                                fuel.exert(1);

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
            */

            tracing::debug!(
                target: "remaining_replay_fuel",
                worker = worker_index,
                fuel = ?fuel,
                remaining = ?fuel.remaining(),
            );

            let all_channels_disconnected = disconnected_channels.iter().copied().all(identity);

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
            let needs_reactivation =
                if is_running.load(Ordering::Acquire) && !all_channels_disconnected {
                    // Tell timely we have work left to do
                    // true
                    false

                // If we're not supposed to be running or all input streams are finished,
                // flush our outputs and release all outstanding capabilities so that
                // any downstream consumers know we're done
                } else {
                    let reason = if all_channels_disconnected {
                        "all streams have finished"
                    } else {
                        "is_running was set to false"
                    };

                    tracing::info!(
                        worker = worker_index,
                        is_running = is_running.load(Ordering::Acquire),
                        all_channels_disconnected = all_channels_disconnected,
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

                    if !has_incremented {
                        replays_finished.fetch_add(1, Ordering::Relaxed);
                        has_incremented = true;
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
