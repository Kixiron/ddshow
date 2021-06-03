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
    path::Path,
};
use timely::{communication::Allocate, logging::TimelyEvent as RawTimelyEvent, worker::Worker};

/// Writes all timely event logs to the given writer
///
/// See [`TimelyEvent`] for the events logged
///
/// ## Examples
///
/// ```rust
/// use std::{env, net::TcpStream};
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
    _tracing::info!(
        worker = worker.index(),
        logging_stream = "timely",
        "installing a timely event logger on worker {}",
        worker.index(),
    );

    let mut logger: BatchLogger<TimelyEvent, WorkerId, _> =
        BatchLogger::new(EventWriter::new(writer));

    worker
        .log_register()
        .insert::<RawTimelyEvent, _>("timely", move |time, data| logger.publish_batch(time, data))
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
    let path = directory.join(format!("timely.worker-{}.ddshow", worker.index()));

    #[cfg(feature = "tracing")]
    _tracing::info!(
        worker = worker.index(),
        logging_stream = "timely",
        directory = ?directory,
        path = ?path,
        "installing a disk backed timely event logger on worker {} pointed at {}",
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
    _tracing::info!(
        worker = worker.index(),
        logging_stream = "differential/arrange",
        "installing a differential event logger on worker {}",
        worker.index(),
    );

    let mut logger: BatchLogger<DifferentialEvent, WorkerId, _> =
        BatchLogger::new(EventWriter::new(writer));

    worker.log_register().insert::<RawDifferentialEvent, _>(
        "differential/arrange",
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
    let path = directory.join(format!("differential.worker-{}.ddshow", worker.index()));

    #[cfg(feature = "tracing")]
    _tracing::info!(
        worker = worker.index(),
        logging_stream = "differential",
        directory = ?directory,
        path = ?path,
        "installing a disk backed differential event logger on worker {} pointed at {}",
        worker.index(),
        path.display(),
    );

    fs::create_dir_all(directory)?;
    let writer = BufWriter::new(File::create(path)?);
    Ok(enable_differential_logging(worker, writer))
}
