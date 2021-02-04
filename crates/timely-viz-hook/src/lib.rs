// TODO: Bincode over abomonation

use abomonation_derive::Abomonation;
use std::{
    io,
    net::{Ipv4Addr, SocketAddr, TcpStream},
};
use timely::{
    dataflow::operators::capture::EventWriter,
    logging::{BatchLogger, TimelyEvent},
    worker::AsWorker,
};

const TIMELY: &str = "timely";
const TIMELY_PROGRESS: &str = "timely/progress";
const DIFFERENTIAL_ARRANGEMENTS: &str = "differential/arrange";

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct LoggingConfig {
    pub timely_addr: SocketAddr,
    pub timely_progress_addr: Option<SocketAddr>,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            timely_addr: SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1).into(), 51317),
            timely_progress_addr: Some(SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1).into(), 51318)),
        }
    }
}

pub fn set_log_hooks<W>(worker: &mut W, config: &LoggingConfig) -> io::Result<()>
where
    W: AsWorker,
{
    let timely_stream = TcpStream::connect(&config.timely_addr)?;
    let mut logger = BatchLogger::new(EventWriter::new(timely_stream));

    worker
        .log_register()
        .insert::<TimelyEvent, _>(TIMELY, move |time, data| logger.publish_batch(time, data));

    // if let Some(progress_addr) = config.timely_progress_addr.as_ref() {
    //     let progress_stream = TcpStream::connect(progress_addr)?;
    //     let mut logger = BatchLogger::new(EventWriter::new(progress_stream));
    //     let mut buf = Vec::new();
    //
    //     worker.log_register().insert::<TimelyProgressEvent, _>(
    //         TIMELY_PROGRESS,
    //         move |time, data| {
    //             buf.clear();
    //             buf.extend(data.drain(..).map(|(time, worker, event)| {
    //                 (time, worker, InternalTimelyProgressEvent::from(event))
    //             }));
    //
    //             logger.publish_batch(time, &mut buf);
    //         },
    //     );
    // }

    Ok(())
}

/// Removes all logging hooks that could be set by [`set_log_hooks()`], regardless
/// of whether they've been set or not. Allows Timely to shut down workers, as any
/// pending loggers will cause it to hang
pub fn remove_log_hooks<W>(worker: &mut W)
where
    W: AsWorker,
{
    let log_hooks = [TIMELY, TIMELY_PROGRESS, DIFFERENTIAL_ARRANGEMENTS];
    for hook in log_hooks.iter().copied() {
        worker.log_register().remove(hook);
    }
}

/// A temporary workaround for the fact that [`TimelyProgressEvent`] doesn't
/// currently implement [`Serialize`], [`Deserialize`] or [`Abomonation`]
// TODO: Remove this once timely-dataflow/#356 is resolved
//       https://github.com/TimelyDataflow/timely-dataflow/issues/356
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct InternalTimelyProgressEvent {
    pub is_send: bool,
    pub source: usize,
    pub channel: usize,
    pub seq_no: usize,
    pub addr: Vec<usize>,
    pub num_messages: usize,
    pub num_capability_updates: usize,
}

// impl From<TimelyProgressEvent> for InternalTimelyProgressEvent {
//     fn from(event: TimelyProgressEvent) -> Self {
//         Self {
//             is_send: event.is_send,
//             source: event.source,
//             channel: event.channel,
//             seq_no: event.seq_no,
//             addr: event.addr,
//             num_messages: event.messages.iter().count(),
//             num_capability_updates: event.internal.iter().count(),
//         }
//     }
// }

// pub fn bincode_options() -> impl Options {
//     DefaultOptions::new()
//         .with_varint_encoding()
//         .with_little_endian()
//         .with_no_limit()
//         .allow_trailing_bytes()
// }
