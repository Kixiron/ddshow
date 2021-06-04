use crate::{
    args::Args,
    dataflow::{
        constants::{IDLE_EXTRACTION_FUEL, TCP_READ_TIMEOUT},
        operators::{EventReader, Fuel, RkyvEventReader},
        DataflowData, DataflowReceivers,
    },
};
use abomonation::Abomonation;
use anyhow::{Context, Result};
use crossbeam_channel::Receiver;
use std::{
    ffi::OsStr,
    fs::{self, File},
    hint,
    io::{self, stdout, BufReader, Read, Write},
    iter,
    net::{SocketAddr, TcpListener, TcpStream},
    num::NonZeroUsize,
    path::Path,
    sync::{
        atomic::{self, AtomicBool, AtomicUsize, Ordering},
        Arc, Barrier,
    },
    thread,
    time::{Duration, Instant},
};
use timely::{communication::WorkerGuards, dataflow::operators::capture::Event};

type AcquiredStreams<T, D1, D2> =
    EventReceivers<RkyvEventReader<T, D1, BufReader<File>>, EventReader<T, D2, TcpStream>>;

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
}

/// Connect to and prepare the replay sources
#[tracing::instrument]
pub fn acquire_replay_sources<T, D1, D2>(
    address: SocketAddr,
    connections: NonZeroUsize,
    workers: NonZeroUsize,
    log_dir: Option<&Path>,
    file_prefix: &str,
    target: &str,
) -> Result<(AcquiredStreams<T, D1, D2>, bool)>
where
    Event<T, D2>: Clone,
    T: Abomonation + Send + 'static,
    D2: Abomonation + Send + 'static,
{
    let span = tracing::trace_span!("loading replay sources", target = target);
    span.in_scope(|| {
        let start_time = Instant::now();
        let plural = if connections.get() == 1 { "" } else { "s" };

        if let Some(log_dir) = log_dir {
            print!(
                "Loading {} replay from {}...",
                target,
                log_dir
                    .canonicalize()
                    .context("failed to canonicalize replay directory")?
                    .display(),
            );
            stdout().flush().context("failed to flush stdout")?;
        } else {
            tracing::info!(
                "started waiting for {} {} connections on {}",
                connections,
                target,
                address,
            );

            println!(
                "Waiting for {} {} connection{} on {}...",
                connections, target, plural, address,
            );
        }

        let replay_sources = if let Some(log_dir) = log_dir {
            let mut replays = Vec::with_capacity(connections.get());

            // Load all files in the directory that have the `.ddshow` extension and a
            // prefix that matches `file_prefix`
            // TODO: Probably want some sort of method to allow distinguishing between
            //       different runs saved to the same folder
            // TODO: Add support for decompressing archived log files
            let dir = fs::read_dir(log_dir).context("failed to read log directory")?;
            for entry in dir.into_iter().filter_map(Result::ok) {
                let replay_file = entry.path();
                if entry.file_type().map_or(false, |file| file.is_file())
                    && replay_file.extension() == Some(OsStr::new("ddshow"))
                    && replay_file
                        .file_name()
                        .and_then(OsStr::to_str)
                        .and_then(|file| file.split('.').next())
                        .map_or(false, |prefix| prefix == file_prefix)
                {
                    tracing::debug!("loading {} replay from {}", target, replay_file.display());
                    let timely_file = File::open(&replay_file).with_context(|| {
                        format!("failed to open {} log file within replay directory", target)
                    })?;

                    replays.push(RkyvEventReader::new(BufReader::new(timely_file)));
                }
            }

            ReplaySource::Rkyv(replays)
        } else {
            wait_for_connections(address, connections)?
        };

        let are_replay_sources = !replay_sources.is_empty();
        let event_receivers = make_streams(workers.get(), replay_sources)?;

        let elapsed = start_time.elapsed();
        if !are_replay_sources {
            println!("No replay sources found");
        } else if log_dir.is_none() {
            println!(
                "Connected to {} {} trace{} in {:#?}",
                connections, target, plural, elapsed,
            );
        } else {
            println!(" done!");
        }

        Ok((event_receivers, are_replay_sources))
    })
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
#[tracing::instrument]
pub fn wait_for_connections<T, D, R>(
    timely_addr: SocketAddr,
    connections: NonZeroUsize,
) -> Result<ReplaySource<R, EventReader<T, D, TcpStream>>>
where
    Event<T, D>: Clone,
    T: Abomonation + Send + 'static,
    D: Abomonation + Send + 'static,
{
    let timely_listener =
        TcpListener::bind(timely_addr).context("failed to bind to socket address")?;

    let timely_conns = (0..connections.get())
        .zip(timely_listener.incoming())
        .map(|(i, socket)| {
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
                i + 1,
                connections,
            );
            println!("Connected to socket {}/{}", i + 1, connections);

            Ok(EventReader::new(socket))
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(ReplaySource::Abomonation(timely_conns))
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
    let (mut stdin, mut stdout) = (io::stdin(), io::stdout());

    let (send, recv) = crossbeam_channel::bounded(1);
    let barrier = Arc::new(Barrier::new(2));

    let thread_barrier = barrier.clone();
    thread::spawn(move || {
        thread_barrier.wait();

        // Wait for input
        let _ = stdin.read(&mut [0]);

        tracing::debug!("stdin thread got input from stdin");
        send.send(()).unwrap();
    });

    // Write a prompt to the terminal for the user
    let message = if args.is_file_sourced() {
        "Press enter to finish loading trace data (this will cause data to not be fully processed)..."
    } else {
        "Press enter to finish collecting trace data (this will crash the source computation \
            if it's currently running and cause data to not be fully processed)..."
    };
    stdout
        .write_all(message.as_bytes())
        .context("failed to write termination message to stdout")?;

    stdout.flush().context("failed to flush stdout")?;

    // Sync up with the user input thread
    barrier.wait();

    let (mut fuel, mut extractor) = (
        Fuel::limited(IDLE_EXTRACTION_FUEL),
        receivers.into_extractor(),
    );
    let num_threads = worker_guards.guards().len();

    loop {
        hint::spin_loop();

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

        // If the user shuts down the dataflow
        if recv.recv_timeout(Duration::from_millis(500)).is_ok() {
            tracing::info!(
                num_threads = num_threads,
                workers_finished = workers_finished.load(Ordering::Acquire),
                running = running.load(Ordering::Acquire),
                "main thread got shutdown signal, received input from user",
            );

            break;
        }

        // After we've checked all of our exit conditions we can pull some
        // data from out of the target dataflow
        extractor.extract_with_fuel(&mut fuel);

        tracing::debug!(
            used = ?fuel.used(),
            remaining = ?fuel.remaining(),
            "spent {} fuel within the main thread's wait loop",
            fuel.used().unwrap_or(usize::MAX),
        );

        fuel.reset();
    }

    // Terminate the replay
    running.store(false, Ordering::Release);
    atomic::fence(Ordering::Acquire);

    write!(stdout, " done!\nProcessing data...").context("failed to write to stdout")?;
    stdout.flush().context("failed to flush stdout")?;

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

    Ok(data)
}
