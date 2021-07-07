mod batch_logger;
mod writer;

pub use batch_logger::BatchLogger;
pub use writer::EventWriter;

#[cfg(feature = "ddflow")]
use ddshow_types::differential_logging::DifferentialEvent;
use ddshow_types::{timely_logging::TimelyEvent, WorkerId};
#[cfg(feature = "ddflow")]
use differential_dataflow::logging::DifferentialEvent as RawDifferentialEvent;
use std::{
    any::Any,
    fs::{self, File},
    io::{self, BufWriter, Write},
    path::{Path, PathBuf},
};
use timely::{communication::Allocate, logging::TimelyEvent as RawTimelyEvent, worker::Worker};

// TODO: Allow configuring what events are saved and support compression

/// The name of the timely log stream for timely events
pub const TIMELY_LOGGER_NAME: &str = "timely";

/// The name of the timely log stream for differential arrangement events
pub const DIFFERENTIAL_ARRANGEMENT_LOGGER_NAME: &str = "differential/arrange";

/// The name of the timely log stream for timely progress events
pub const TIMELY_PROGRESS_LOGGER_NAME: &str = "timely/progress";

/// The file that all timely events will be stored in
pub const TIMELY_LOG_FILE: &str = "timely";

/// The file that all differential arrangement events will be stored in
pub const DIFFERENTIAL_ARRANGEMENT_LOG_FILE: &str = "differential";

/// The file that all timely progress events will be stored in
pub const TIMELY_PROGRESS_LOG_FILE: &str = "timely-progress";

/// Constructs the path to a logging file for the given worker
pub fn log_file_path<A>(worker: &Worker<A>, file_prefix: &str, dir: &Path) -> PathBuf
where
    A: Allocate,
{
    dir.join(format!("{}.worker-{}.ddshow", file_prefix, worker.index()))
}

/// Writes all timely event logs to the given writer
///
/// See [`TimelyEvent`] for the events logged
///
/// ## Examples
///
/// ```rust
/// use std::{env, net::TcpStream};
/// use timely::dataflow::operators::{Inspect, ToStream};
///
/// timely::execute_directly(|worker| {
///     // If `TIMELY_WORKER_LOG_ADDR` is set, `ddshow_sink` will
///     // send all events to the address that it's set with
///     if let Ok(addr) = env::var("TIMELY_WORKER_LOG_ADDR") {
///         if let Ok(stream) = TcpStream::connect(&addr) {
///             ddshow_sink::enable_timely_logging(worker, stream);
///         }
///     }
///     
///     worker.dataflow::<(),_,_>(|scope| {
///         (0..10).to_stream(scope)
///             .inspect(|x| println!("seen: {:?}", x));
///     });
/// });
/// ```
///
pub fn enable_timely_logging<A, W>(
    worker: &mut Worker<A>,
    writer: W,
) -> Option<Box<dyn Any + 'static>>
where
    A: Allocate,
    W: Write + 'static,
{
    #[cfg(feature = "tracing")]
    tracing_dep::info!(
        worker = worker.index(),
        logging_stream = TIMELY_LOGGER_NAME,
        "installing a {} event logger on worker {}",
        TIMELY_LOGGER_NAME,
        worker.index(),
    );

    let mut logger: BatchLogger<TimelyEvent, WorkerId, _> =
        BatchLogger::new(EventWriter::new(writer));

    worker
        .log_register()
        .insert::<RawTimelyEvent, _>(TIMELY_LOGGER_NAME, move |time, data| {
            logger.publish_batch(time, data)
        })
}

pub fn save_timely_logs_to_disk<P, A>(
    worker: &mut Worker<A>,
    directory: P,
) -> io::Result<Option<Box<dyn Any + 'static>>>
where
    P: AsRef<Path>,
    A: Allocate,
{
    let directory = directory.as_ref();
    let path = directory.join(format!(
        "{}.worker-{}.ddshow",
        TIMELY_LOG_FILE,
        worker.index()
    ));

    #[cfg(feature = "tracing")]
    tracing_dep::info!(
        worker = worker.index(),
        logging_stream = TIMELY_LOGGER_NAME,
        directory = ?directory,
        path = ?path,
        "installing a disk backed {} event logger on worker {} pointed at {}",
        TIMELY_LOGGER_NAME,
        worker.index(),
        path.display(),
    );

    fs::create_dir_all(directory)?;
    let writer = BufWriter::new(File::create(path)?);
    Ok(enable_timely_logging(worker, writer))
}

/// Writes all differential dataflow event logs to the given writer
///
/// See [`DifferentialEvent`] for the events logged
///
/// ## Examples
///
/// ```rust
/// use std::{env, net::TcpStream};
/// use timely::dataflow::operators::{Inspect, ToStream};
///
/// timely::execute_directly(|worker| {
///     // If `TIMELY_WORKER_LOG_ADDR` is set, `ddshow_sink` will
///     // send all events to the address that it's set with
///     if let Ok(addr) = env::var("DIFFERENTIAL_LOG_ADDR") {
///         if let Ok(stream) = TcpStream::connect(&addr) {
///             ddshow_sink::enable_differential_logging(worker, stream);
///         }
///     }
///     
///     worker.dataflow::<(),_,_>(|scope| {
///         (0..10).to_stream(scope)
///             .inspect(|x| println!("seen: {:?}", x));
///     });
/// });
/// ```
///
#[cfg(feature = "ddflow")]
pub fn enable_differential_logging<A, W>(
    worker: &mut Worker<A>,
    writer: W,
) -> Option<Box<dyn Any + 'static>>
where
    A: Allocate,
    W: Write + 'static,
{
    #[cfg(feature = "tracing")]
    tracing_dep::info!(
        worker = worker.index(),
        logging_stream = DIFFERENTIAL_ARRANGEMENT_LOGGER_NAME,
        "installing a differential event logger on worker {}",
        worker.index(),
    );

    let mut logger: BatchLogger<DifferentialEvent, WorkerId, _> =
        BatchLogger::new(EventWriter::new(writer));

    worker.log_register().insert::<RawDifferentialEvent, _>(
        DIFFERENTIAL_ARRANGEMENT_LOGGER_NAME,
        move |time, data| {
            logger.publish_batch(time, data);
        },
    )
}

pub fn save_differential_logs_to_disk<P, A>(
    worker: &mut Worker<A>,
    directory: P,
) -> io::Result<Option<Box<dyn Any + 'static>>>
where
    P: AsRef<Path>,
    A: Allocate,
{
    let directory = directory.as_ref();
    let path = directory.join(format!(
        "{}.worker-{}.ddshow",
        DIFFERENTIAL_ARRANGEMENT_LOG_FILE,
        worker.index()
    ));

    #[cfg(feature = "tracing")]
    tracing_dep::info!(
        worker = worker.index(),
        logging_stream = DIFFERENTIAL_ARRANGEMENT_LOGGER_NAME,
        directory = ?directory,
        path = ?path,
        "installing a disk backed {} event logger on worker {} pointed at {}",
        DIFFERENTIAL_ARRANGEMENT_LOGGER_NAME,
        worker.index(),
        path.display(),
    );

    fs::create_dir_all(directory)?;
    let writer = BufWriter::new(File::create(path)?);
    Ok(enable_differential_logging(worker, writer))
}

/*
pub fn enable_timely_progress_logging<A, W>(
    worker: &mut Worker<A>,
    writer: W,
) -> Option<Box<dyn Any + 'static>>
where
    A: Allocate,
    W: Write + 'static,
{
    #[cfg(feature = "tracing")]
    tracing_dep::info!(
        worker = worker.index(),
        logging_stream = TIMELY_PROGRESS_LOGGER_NAME,
        "installing a {} logger on worker {}",
        TIMELY_PROGRESS_LOGGER_NAME,
        worker.index(),
    );

    let mut logger: BatchLogger<TimelyProgressEvent, WorkerId, _> =
        BatchLogger::new(EventWriter::new(writer));

    worker
        .log_register()
        .insert::<RawTimelyProgressEvent, _>(TIMELY_PROGRESS_LOGGER_NAME, move |time, data| {
            logger.publish_batch(time, data)
        })
}

pub fn save_timely_progress_to_disk<P, A>(
    worker: &mut Worker<A>,
    directory: P,
) -> io::Result<Option<Box<dyn Any + 'static>>>
where
    P: AsRef<Path>,
    A: Allocate,
{
    let directory = directory.as_ref();
    let path = directory.join(format!(
        "{}.worker-{}.ddshow",
        TIMELY_PROGRESS_LOG_FILE,
        worker.index()
    ));

    #[cfg(feature = "tracing")]
    tracing_dep::info!(
        worker = worker.index(),
        logging_stream = TIMELY_PROGRESS_LOGGER_NAME,
        directory = ?directory,
        path = ?path,
        "installing a disk backed {} logger on worker {} pointed at {}",
        TIMELY_PROGRESS_LOGGER_NAME,
        worker.index(),
        path.display(),
    );

    fs::create_dir_all(directory)?;
    let writer = BufWriter::new(File::create(path)?);
    Ok(enable_timely_progress_logging(worker, writer))
}
*/
