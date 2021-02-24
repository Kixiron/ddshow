// TODO: Bincode over abomonation

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
const DIFFERENTIAL_ARRANGEMENTS: &str = "differential/arrange";

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct LoggingConfig {
    pub timely_addr: SocketAddr,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            timely_addr: SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1).into(), 51317),
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

    Ok(())
}

/// Removes all logging hooks that could be set by [`set_log_hooks()`], regardless
/// of whether they've been set or not. Allows Timely to shut down workers, as any
/// pending loggers will cause it to hang
pub fn remove_log_hooks<W>(worker: &mut W)
where
    W: AsWorker,
{
    let log_hooks = [TIMELY, DIFFERENTIAL_ARRANGEMENTS];
    for hook in log_hooks.iter().copied() {
        worker.log_register().remove(hook);
    }
}
