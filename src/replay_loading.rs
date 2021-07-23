use crate::{
    args::{Args, StreamEncoding},
    dataflow::{
        constants::{IDLE_EXTRACTION_FUEL, TCP_READ_TIMEOUT},
        operators::{EventIterator, EventReader, Fuel, RkyvEventReader},
        utils::{self, DifferentialLogBundle, ProgressLogBundle, TimelyLogBundle},
        DataflowData, DataflowReceivers,
    },
    report,
};
use abomonation::Abomonation;
use anyhow::{Context, Result};
use bytecheck::CheckBytes;
use crossbeam_channel::Receiver;
use ddshow_sink::{DIFFERENTIAL_ARRANGEMENT_LOG_FILE, TIMELY_LOG_FILE, TIMELY_PROGRESS_LOG_FILE};
use ddshow_types::progress_logging::TimelyProgressEvent;
use differential_dataflow::logging::DifferentialEvent as RawDifferentialEvent;
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use rkyv::{
    de::deserializers::SharedDeserializeMap, validation::validators::DefaultValidator, Archive,
    Deserialize,
};
use std::{
    collections::HashMap,
    ffi::OsStr,
    fmt::Debug,
    fs::{self, File},
    io::{self, BufReader, BufWriter, Read, Write},
    iter,
    net::{SocketAddr, TcpListener, TcpStream},
    num::NonZeroUsize,
    path::PathBuf,
    sync::{
        atomic::{self, AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use timely::{
    communication::WorkerGuards, dataflow::operators::capture::Event,
    logging::TimelyEvent as RawTimelyEvent,
};

type AcquiredStreams<T, D1, D2> = EventReceivers<
    RkyvEventReader<T, D1, Box<dyn Read + Send + 'static>>,
    EventReader<T, D2, TcpStream>,
>;

pub(crate) type TimelyEventReceivers = Arc<[Receiver<TimelyReplaySource>]>;
pub(crate) type TimelyReplaySource = ReplaySource<
    RkyvEventReader<Duration, TimelyLogBundle, Box<dyn Read + Send + 'static>>,
    EventReader<Duration, (Duration, usize, RawTimelyEvent), TcpStream>,
>;

pub(crate) type DifferentialEventReceivers = Option<Arc<[Receiver<DifferentialReplaySource>]>>;
pub(crate) type DifferentialReplaySource = ReplaySource<
    RkyvEventReader<Duration, DifferentialLogBundle, Box<dyn Read + Send + 'static>>,
    EventReader<Duration, (Duration, usize, RawDifferentialEvent), TcpStream>,
>;

pub(crate) type ProgressEventReceivers = Option<Arc<[Receiver<ProgressReplaySource>]>>;
pub(crate) type ProgressReplaySource = ReplaySource<
    RkyvEventReader<Duration, ProgressLogBundle, Box<dyn Read + Send + 'static>>,
    EventReader<Duration, (Duration, usize, TimelyProgressEvent), TcpStream>,
>;

#[derive(Debug)]
pub enum ReplaySource<R, A> {
    Rkyv(Vec<R>),
    Abomonation(Vec<A>),
}

impl<R, A> ReplaySource<R, A> {
    pub fn len(&self) -> usize {
        match self {
            ReplaySource::Rkyv(rkyv) => rkyv.len(),
            ReplaySource::Abomonation(abomonation) => abomonation.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub const fn kind(&self) -> &'static str {
        match self {
            ReplaySource::Rkyv(_) => "Rkyv",
            ReplaySource::Abomonation(_) => "Abomonation",
        }
    }

    /// Returns `true` if the replay_source is `Abomonation`.
    pub const fn is_abomonation(&self) -> bool {
        matches!(self, Self::Abomonation(..))
    }

    #[cfg(test)]
    pub fn into_rkyv(self) -> Result<Vec<R>, Self> {
        if let Self::Rkyv(rkyv) = self {
            Ok(rkyv)
        } else {
            Err(self)
        }
    }
}

#[tracing::instrument(skip(args))]
pub fn connect_to_sources(
    args: &Args,
) -> Result<
    Option<(
        TimelyEventReceivers,
        DifferentialEventReceivers,
        ProgressEventReceivers,
        usize,
    )>,
> {
    let mut total_sources = 0;

    let timely_listener = if !args.is_file_sourced() {
        Some(TcpListener::bind(args.timely_address).with_context(|| {
            anyhow::anyhow!("failed to bind to timely socket {}", args.timely_address)
        })?)
    } else {
        None
    };
    let differential_listener = if args.differential_enabled && !args.is_file_sourced() {
        Some(
            TcpListener::bind(args.differential_address).with_context(|| {
                anyhow::anyhow!(
                    "failed to bind to differential socket {}",
                    args.differential_address,
                )
            })?,
        )
    } else {
        None
    };
    let progress_listener = if args.progress_enabled && !args.is_file_sourced() {
        Some(TcpListener::bind(args.progress_address).with_context(|| {
            anyhow::anyhow!(
                "failed to bind to progress socket {}",
                args.progress_address,
            )
        })?)
    } else {
        None
    };

    // Connect to the timely sources
    let (timely_event_receivers, are_timely_sources, num_sources) = acquire_replay_sources(
        &args,
        args.timely_address,
        timely_listener,
        args.timely_connections,
        args.workers,
        args.replay_logs.as_deref(),
        TIMELY_LOG_FILE,
        "Timely",
    )?;
    total_sources += num_sources;

    // Connect to the differential sources
    let (differential_event_receivers, are_differential_sources) = if args.differential_enabled {
        let (receivers, are_sources, num_sources) = acquire_replay_sources(
            &args,
            args.differential_address,
            differential_listener,
            args.timely_connections,
            args.workers,
            args.replay_logs.as_deref(),
            DIFFERENTIAL_ARRANGEMENT_LOG_FILE,
            "Differential",
        )?;
        total_sources += num_sources;

        (Some(receivers), are_sources)
    } else {
        (None, true)
    };

    // Connect to progress sources
    let (progress_event_receivers, are_progress_sources) = if args.progress_enabled {
        let (receivers, are_sources, num_sources) = acquire_replay_sources(
            &args,
            args.progress_address,
            progress_listener,
            args.timely_connections,
            args.workers,
            args.replay_logs.as_deref(),
            TIMELY_PROGRESS_LOG_FILE,
            "Progress",
        )?;
        total_sources += num_sources;

        (Some(receivers), are_sources)
    } else {
        (None, true)
    };

    // If no replay sources were provided, exit early
    if !are_timely_sources || !are_differential_sources || !are_progress_sources {
        tracing::warn!(
            are_timely_sources = are_timely_sources,
            are_differential_sources = are_differential_sources,
            differential_enabled = args.differential_enabled,
            are_progress_sources = are_progress_sources,
            progress_enabled = args.progress_enabled,
            total_sources = total_sources,
            "no replay sources were provided",
        );
    }

    Ok(Some((
        timely_event_receivers,
        differential_event_receivers,
        progress_event_receivers,
        total_sources,
    )))
}

/// Connect to and prepare the replay sources
#[tracing::instrument(skip(args))]
#[allow(clippy::too_many_arguments)]
pub fn acquire_replay_sources<T, D1, D2>(
    args: &Args,
    address: SocketAddr,
    listener: Option<TcpListener>,
    connections: NonZeroUsize,
    workers: NonZeroUsize,
    log_dirs: Option<&[PathBuf]>,
    file_prefix: &str,
    target: &str,
) -> Result<(AcquiredStreams<T, D1, D2>, bool, usize)>
where
    Event<T, D2>: Clone,
    D2: Abomonation + Send + 'static,
    T: Debug + Archive + Abomonation + Send + 'static,
    T::Archived: Deserialize<T, SharedDeserializeMap> + for<'a> CheckBytes<DefaultValidator<'a>>,
    D1: Debug + Archive + Send + 'static,
    D1::Archived: Deserialize<D1, SharedDeserializeMap> + for<'a> CheckBytes<DefaultValidator<'a>>,
{
    let mut num_sources = 0;

    let plural = if connections.get() == 1 { "" } else { "s" };
    let prefix = if let Some(log_dirs) = log_dirs {
        format!(
            "Loading {} replays from {} folder{}",
            target,
            log_dirs.len(),
            if log_dirs.len() == 1 { "s" } else { "" },
        )
    } else {
        tracing::info!(
            "started waiting for {} {} connections on {}",
            connections,
            target,
            address,
        );

        format!(
            "Waiting for {} connection{} on {}",
            connections, plural, address,
        )
    };

    let progress_style = ProgressStyle::default_spinner()
        .tick_chars("⠁⠂⠄⡀⢀⠠⠐⠈ ")
        .template("[{elapsed}] {spinner} {prefix}: {wide_msg}");
    // Duplicate the style with the spinner removed so that it
    // looks nicer after the task finishes
    let finished_style =
        ProgressStyle::default_spinner().template("[{elapsed}] {prefix}: {wide_msg}");

    let progress = ProgressBar::with_draw_target(
        connections.get() as u64,
        if args.is_quiet() {
            ProgressDrawTarget::hidden()
        } else {
            ProgressDrawTarget::stdout()
        },
    )
    .with_style(progress_style)
    .with_prefix(prefix);

    utils::set_steady_tick(&progress, connections.get());

    let replay_sources = if let Some(log_dirs) = log_dirs {
        let mut replays = Vec::with_capacity(connections.get());

        for log_dir in log_dirs {
            progress.set_prefix(format!(
                "Loading {} replay from {}",
                target,
                log_dir.display()
            ));

            // Load all files in the directory that have the `.ddshow` extension and a
            // prefix that matches `file_prefix`
            // TODO: Probably want some sort of method to allow distinguishing between
            //       different runs saved to the same folder
            // TODO: Add support for decompressing archived log files
            let dir = fs::read_dir(log_dir).context("failed to read log directory")?;
            for entry in dir.into_iter().filter_map(|entry| {
                entry.map_or_else(
                    |err| {
                        tracing::error!(dir = ?log_dir, "failed to open file: {:?}", err);
                        // TODO: If we're quiet should we make an error print?
                        eprintln!("failed to open file: {:?}", err);

                        None
                    },
                    Some,
                )
            }) {
                let replay_file = entry.path();

                let is_file = entry.file_type().map_or(false, |file| file.is_file());
                let ends_with_ddshow = replay_file.extension() == Some(OsStr::new("ddshow"));
                let starts_with_prefix = replay_file
                    .file_name()
                    .and_then(OsStr::to_str)
                    .and_then(|file| file.split('.').next())
                    .map_or(false, |prefix| prefix == file_prefix);

                if is_file && ends_with_ddshow && starts_with_prefix {
                    progress.set_message(replay_file.display().to_string());
                    progress.inc_length(1);

                    // FIXME: This is kinda crappy
                    if args.debug_replay_files {
                        let input_file = File::open(&replay_file).with_context(|| {
                            format!("failed to open {} log file within replay directory", target)
                        })?;
                        let mut output_file = BufWriter::new(
                            File::create(entry.path().with_extension("ddshow.debug"))
                                .context("failed to create event debug file")?,
                        );

                        let mut reader =
                            RkyvEventReader::<T, D1, _>::new(Box::new(BufReader::new(input_file)));

                        let mut is_finished = false;
                        while !is_finished {
                            if let Some(event) = EventIterator::next(&mut reader, &mut is_finished)
                                .context("failed to replay event for debugging")?
                            {
                                writeln!(&mut output_file, "{:#?}", event)
                                    .context("failed to write event for debugging")?;
                            }
                        }
                    }

                    tracing::debug!("loading {} replay from {}", target, replay_file.display());
                    let replay_file = File::open(&replay_file).with_context(|| {
                        format!("failed to open {} log file within replay directory", target)
                    })?;

                    replays.push(RkyvEventReader::new(
                        Box::new(BufReader::new(replay_file)) as Box<dyn Read + Send + 'static>
                    ));

                    progress.inc(1);
                    num_sources += 1;
                } else {
                    tracing::warn!(
                        dir = ?log_dir,
                        file = ?replay_file,
                        is_file = is_file,
                        ends_with_ddshow = ends_with_ddshow,
                        starts_with_prefix = starts_with_prefix,
                        "the file {} didn't match replay file criteria",
                        replay_file.display(),
                    );

                    let reason = if is_file {
                        "replay files must be files".to_owned()
                    } else if ends_with_ddshow {
                        "did not end with the `.ddshow` extension".to_owned()
                    } else if starts_with_prefix {
                        format!("did not start with the prefix {}", file_prefix)
                    } else {
                        "unknown error".to_owned()
                    };

                    progress.set_message(format!(
                        "skipped {}, reason: {}",
                        replay_file.display(),
                        reason,
                    ));
                }
            }
        }

        progress.set_style(finished_style);
        progress.finish_with_message(format!(
            "loaded {} replay file{}",
            progress.length(),
            if progress.length() == 1 { "" } else { "s" },
        ));

        ReplaySource::Rkyv(replays)
    } else {
        let listener = listener.expect("a listener must be supplied for stream sources");

        tracing::debug!(
            stream_encoding = ?args.stream_encoding,
            address = ?address,
            connections = ?connections,
            "connecting to source of encoding {}",
            args.stream_encoding,
        );

        let source = match args.stream_encoding {
            StreamEncoding::Abomonation => {
                wait_for_abominated_connections(args, listener, &address, connections, &progress)?
            }
            StreamEncoding::Rkyv => {
                wait_for_rkyv_connections(args, listener, &address, connections, &progress)?
            }
        };

        num_sources += connections.get();

        progress.set_style(finished_style);
        progress.finish_with_message(format!(
            "connected to {} trace source{}",
            connections, plural,
        ));

        source
    };

    let are_replay_sources = !replay_sources.is_empty();
    let event_receivers = make_streams(workers.get(), replay_sources)?;

    Ok((event_receivers, are_replay_sources, num_sources))
}

pub type EventReceivers<R, A> = Arc<[Receiver<ReplaySource<R, A>>]>;

#[tracing::instrument(
    skip(sources),
    fields(
        num_sources = sources.len(),
        source_kind = sources.kind(),
    ),
)]
pub fn make_streams<R, A>(
    num_workers: usize,
    sources: ReplaySource<R, A>,
) -> Result<EventReceivers<R, A>> {
    let readers: Vec<_> = match sources {
        ReplaySource::Rkyv(rkyv) => {
            let mut readers = Vec::with_capacity(num_workers);
            readers.extend(iter::repeat_with(Vec::new).take(num_workers));

            tracing::info!(
                "received {} rkyv sources to distribute among {} workers (average of {} per worker)",
                rkyv.len(),
                num_workers,
                rkyv.len() / num_workers,
            );

            for (idx, source) in rkyv.into_iter().enumerate() {
                readers[idx % num_workers].push(source);
            }

            readers.into_iter().map(ReplaySource::Rkyv).collect()
        }

        ReplaySource::Abomonation(abomonation) => {
            let mut readers = Vec::with_capacity(num_workers);
            readers.extend(iter::repeat_with(Vec::new).take(num_workers));

            tracing::info!(
                "received {} abomonation sources to distribute among {} workers (average of {} per worker)",
                abomonation.len(),
                num_workers,
                abomonation.len() / num_workers,
            );

            for (idx, source) in abomonation.into_iter().enumerate() {
                readers[idx % num_workers].push(source);
            }

            readers.into_iter().map(ReplaySource::Abomonation).collect()
        }
    };

    let (senders, receivers): (Vec<_>, Vec<_>) = (0..num_workers)
        .map(|_| crossbeam_channel::bounded(1))
        .unzip();

    for (worker, (sender, bundle)) in senders.into_iter().zip(readers).enumerate() {
        tracing::debug!(
            "sending {} sources to worker {}/{}",
            bundle.len(),
            worker + 1,
            num_workers,
        );

        sender
            .send(bundle)
            .map_err(|_| anyhow::anyhow!("failed to send events to worker {}", worker + 1))?;
    }

    Ok(Arc::from(receivers))
}

/// Connect to the given address and collect `connections` streams, returning all of them
/// in non-blocking mode
#[tracing::instrument(skip(progress))]
pub fn wait_for_abominated_connections<T, D, R>(
    args: &Args,
    listener: TcpListener,
    addr: &SocketAddr,
    connections: NonZeroUsize,
    progress: &ProgressBar,
) -> Result<ReplaySource<R, EventReader<T, D, TcpStream>>>
where
    Event<T, D>: Clone,
    T: Abomonation + Send + 'static,
    D: Abomonation + Send + 'static,
{
    assert_eq!(
        args.stream_encoding,
        StreamEncoding::Abomonation,
        "abominated connections come from abominated stream encodings",
    );

    progress.set_message(format!(
        "connected to 0/{} socket{}",
        connections,
        if connections.get() == 1 { "" } else { "s" },
    ));
    progress.set_length(connections.get() as u64);

    let timely_conns = (0..connections.get())
        .zip(listener.incoming())
        .map(|(idx, socket)| {
            let socket = socket.context("failed to accept socket connection")?;

            socket
                .set_nonblocking(true)
                .context("failed to set socket to non-blocking mode")?;

            if let Err(err) = socket.set_read_timeout(TCP_READ_TIMEOUT) {
                tracing::error!(
                    "failed to set socket to a read timeout of {:?}: {:?}",
                    TCP_READ_TIMEOUT,
                    err,
                );
            };

            tracing::info!(
                socket = ?socket,
                stream_encoding = ?StreamEncoding::Abomonation,
                "connected to socket {}/{}",
                idx + 1,
                connections,
            );

            progress.set_message(format!(
                "connected to {}/{} socket{}",
                idx + 1,
                connections,
                if connections.get() == 1 { "" } else { "s" },
            ));
            progress.inc(1);

            Ok(EventReader::new(socket))
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(ReplaySource::Abomonation(timely_conns))
}

type ConnectedRkyvSource<T, D, A> =
    ReplaySource<RkyvEventReader<T, D, Box<dyn Read + Send + 'static>>, A>;

/// Connect to the given address and collect `connections` streams, returning all of them
/// in non-blocking mode
#[tracing::instrument(skip(progress))]
pub fn wait_for_rkyv_connections<T, D, A>(
    args: &Args,
    listener: TcpListener,
    addr: &SocketAddr,
    connections: NonZeroUsize,
    progress: &ProgressBar,
) -> Result<ConnectedRkyvSource<T, D, A>>
where
    T: Archive,
    T::Archived: Deserialize<T, SharedDeserializeMap> + for<'a> CheckBytes<DefaultValidator<'a>>,
    D: Archive,
    D::Archived: Deserialize<D, SharedDeserializeMap> + for<'a> CheckBytes<DefaultValidator<'a>>,
{
    assert_eq!(
        args.stream_encoding,
        StreamEncoding::Rkyv,
        "rkyv connections come from rkyv stream encodings",
    );

    progress.set_message(format!(
        "connected to 0/{} socket{}",
        connections,
        if connections.get() == 1 { "" } else { "s" },
    ));
    progress.set_length(connections.get() as u64);

    let timely_conns = (0..connections.get())
        .zip(listener.incoming())
        .map(|(idx, socket)| {
            let socket = socket.context("failed to accept socket connection")?;

            socket
                .set_nonblocking(true)
                .context("failed to set socket to non-blocking mode")?;

            if let Err(err) = socket.set_read_timeout(TCP_READ_TIMEOUT) {
                tracing::error!(
                    "failed to set socket to a read timeout of {:?}: {:?}",
                    TCP_READ_TIMEOUT,
                    err,
                );
            };

            tracing::info!(
                socket = ?socket,
                "connected to socket {}/{}",
                idx + 1,
                connections,
            );

            progress.set_message(format!(
                "connected to {}/{} socket{}",
                idx + 1,
                connections,
                if connections.get() == 1 { "" } else { "s" },
            ));
            progress.inc(1);

            Ok(RkyvEventReader::new(
                Box::new(socket) as Box<dyn Read + Send + 'static>
            ))
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(ReplaySource::Rkyv(timely_conns))
}

/// Wait for user input to terminate the trace replay and wait for all timely
/// workers to terminate
// TODO: Add a "haven't received updates in `n` seconds" thingy to tell the user
//       we're no longer getting data
#[tracing::instrument(
    skip(args, worker_guards, receivers),
    fields(workers = worker_guards.guards().len()),
)]
pub fn wait_for_input(
    args: &Args,
    running: &AtomicBool,
    workers_finished: &AtomicUsize,
    worker_guards: WorkerGuards<Result<()>>,
    receivers: DataflowReceivers,
) -> Result<DataflowData> {
    if args.isnt_quiet() {
        // Write a prompt to the terminal for the user
        let message = if args.is_file_sourced() {
            "Press ctrl+c to stop loading trace data (this will cause data to not be fully processed)..."
        } else {
            "Press ctrl+c to stop collecting trace data (this will crash the source computation \
            if it's currently running and cause data to not be fully processed)..."
        };
        println!("{}", message);
    }

    let (mut fuel, mut extractor) = (
        Fuel::limited(IDLE_EXTRACTION_FUEL),
        receivers.into_extractor(),
    );
    let num_threads = worker_guards.guards().len();

    let report_update_duration = args
        .report_update_duration
        .map(|secs| Duration::from_secs(secs as u64));
    let mut last_report_update = Instant::now();

    loop {
        // If all workers finish their computations
        if workers_finished.load(Ordering::Acquire) == num_threads {
            tracing::info!(
                num_threads = num_threads,
                workers_finished = num_threads,
                running = running.load(Ordering::Acquire),
                "main thread got shutdown signal, all workers finished",
            );

            break;
        }

        // If an error is encountered and the dataflow shut down
        if !running.load(Ordering::Acquire) {
            tracing::info!(
                num_threads = num_threads,
                workers_finished = workers_finished.load(Ordering::Acquire),
                running = false,
                "main thread got shutdown signal, `running` was set to false",
            );

            break;
        }

        // After we've checked all of our exit conditions we can pull some
        // data from out of the target dataflow
        fuel.reset();
        extractor.extract_with_fuel(&mut fuel);

        tracing::debug!(
            used = ?fuel.used(),
            remaining = ?fuel.remaining(),
            "spent {} fuel within the main thread's wait loop",
            fuel.used().unwrap_or(usize::MAX),
        );

        if let Some(duration) = report_update_duration {
            let elapsed = last_report_update.elapsed();

            if elapsed >= duration {
                tracing::info!(
                    "the last report file update happened {:#?} ago, updating the report file",
                    elapsed,
                );

                let data = extractor.current_dataflow_data();

                let name_lookup: HashMap<_, _> = data.name_lookup.iter().cloned().collect();
                let addr_lookup: HashMap<_, _> = data.addr_lookup.iter().cloned().collect();

                // Build & emit the textual report
                report::build_report(&*args, &data, &name_lookup, &addr_lookup)?;

                last_report_update = Instant::now();
            }
        }
    }

    // Terminate the replay
    running.store(false, Ordering::Release);
    atomic::fence(Ordering::Acquire);

    {
        let mut stdout = io::stdout();
        write!(stdout, "Processing data...").context("failed to write to stdout")?;
        stdout.flush().context("failed to flush stdout")?;
    }

    tracing::debug!("unparking all worker threads");
    for thread in worker_guards.guards() {
        thread.thread().unpark();
    }

    // Join all timely worker threads
    tracing::debug!("joining all worker threads");
    for result in worker_guards.join() {
        result
            .map_err(|err| anyhow::anyhow!("failed to join timely worker threads: {}", err))??;
    }

    tracing::debug!("extracting all remaining data from the dataflow");
    let data = extractor.extract_all();

    if args.isnt_quiet() {
        println!(" done!");
    }

    Ok(data)
}

#[cfg(test)]
mod tests {
    use crate::{
        args::StreamEncoding,
        dataflow::operators::EventIterator,
        logging,
        replay_loading::{connect_to_sources, ReplaySource},
        Args,
    };
    use bytecheck::CheckBytes;
    use ddshow_sink::{BatchLogger, EventSerializer, EventWriter as RkyvEventWriter};
    use ddshow_types::{
        timely_logging::{InputEvent, StartStop, TimelyEvent},
        WorkerId,
    };
    use rkyv::{validation::validators::DefaultValidator, Serialize};
    use std::{
        fmt::Debug,
        net::{SocketAddr, TcpStream},
        sync::{Arc, Barrier},
        thread,
        time::Duration,
    };
    use timely::dataflow::operators::capture::Event as RawEvent;

    // TODO: Replace with `std::assert_matches!()`
    macro_rules! assert_matches {
        ($left:expr, $( $pattern:pat )|+ $( if $guard: expr )? $(,)?) => ({
            match $left {
                $( $pattern )|+ $( if $guard )? => {}
                ref left_val => {
                    panic!(
                        "assertion failed: `(left matches right)`\n\
                        left: `{:?}`,\n\
                        right: `{:?}`",
                        left_val,
                        stringify!($($pattern)|+ $(if $guard)?),
                    );
                }
            }
        });

        ($left:expr, $( $pattern:pat )|+ $( if $guard: expr )?, $($arg:tt)+) => ({
            match $left {
                $( $pattern )|+ $( if $guard )? => {}
                ref left_val => {
                    panic!(
                        "assertion failed: `(left matches right)`\n\
                        left: `{:?}`,\n\
                        right: `{:?}`: {}",
                        left_val,
                        stringify!($($pattern)|+ $(if $guard)?),
                        format_args!($($arg)+),
                    );
                }
            }
        });
    }

    #[test]
    fn connection_test() {
        let args = Args {
            stream_encoding: StreamEncoding::Rkyv,
            ..Default::default()
        };
        logging::init_logging(&args);

        let barrier = Arc::new(Barrier::new(2));
        let events = vec![
            TimelyEvent::Input(InputEvent::new(StartStop::start())),
            TimelyEvent::Input(InputEvent::new(StartStop::stop())),
        ];

        target_program(barrier.clone(), args.timely_address, events.clone());
        barrier.wait();

        let (timely_recv, differential_recv, progress_recv, total_sources) =
            connect_to_sources(&args).unwrap().unwrap();

        assert_eq!(total_sources, 1);
        assert_matches!(differential_recv, None);
        assert_matches!(progress_recv, None);

        let mut sources: Vec<_> = timely_recv
            .iter()
            .flat_map(|recv| recv.try_iter())
            .collect();
        assert_eq!(sources.len(), 1);

        let source = sources.remove(0);
        assert_matches!(source, ReplaySource::Rkyv(_));

        let mut source = source.into_rkyv().unwrap();
        assert_eq!(source.len(), 1);

        let mut source = source.remove(0);
        let result: Vec<_> = source
            .take_events()
            .unwrap()
            .into_iter()
            .filter_map(|event| match event {
                RawEvent::Messages(_, data) => Some(data),
                RawEvent::Progress(_) => None,
            })
            .flatten()
            .map(|(_, _, event)| event)
            .collect();
        assert_eq!(result, events);
    }

    fn target_program<E>(barrier: Arc<Barrier>, address: SocketAddr, events: Vec<E>)
    where
        E: for<'a> Serialize<EventSerializer<'a>> + Send + Debug + 'static,
        E::Archived: for<'a> CheckBytes<DefaultValidator<'a>>,
    {
        thread::spawn(move || {
            barrier.wait();
            thread::sleep(Duration::from_millis(200));

            let mut writer = BatchLogger::<E, WorkerId, _>::new(RkyvEventWriter::new(
                TcpStream::connect(address).unwrap(),
            ));

            let mut time = Duration::from_secs(0);
            for event in events {
                writer.publish_batch(&time, &mut vec![(time, WorkerId::new(0), event)]);
                time += Duration::from_millis(100);
            }
        });
    }
}
