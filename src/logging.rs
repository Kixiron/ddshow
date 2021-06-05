use crate::args::Args;
use anyhow::Result;
use std::{env, net::TcpStream};
use timely::{communication::Allocate, worker::Worker};
use tracing_subscriber::{
    fmt::time::Uptime, prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt,
    EnvFilter,
};

pub(crate) fn init_logging(args: &Args) {
    let filter_layer = EnvFilter::from_env("DDSHOW_LOG");
    let fmt_layer = tracing_subscriber::fmt::layer()
        .pretty()
        .with_timer(Uptime::default())
        .with_thread_names(true)
        .with_ansi(args.color.is_always() || args.color.is_auto())
        .with_level(false);

    let _ = tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt_layer)
        .try_init();
}

// TODO: Progress logging & configure logging via the cli
pub(crate) fn init_dataflow_logging<A>(worker: &mut Worker<A>) -> Result<()>
where
    A: Allocate,
{
    let (differential_log_addr, timely_disk_log, differential_disk_log) = (
        env::var("DIFFERENTIAL_LOG_ADDR"),
        env::var("TIMELY_DISK_LOG"),
        env::var("DIFFERENTIAL_DISK_LOG"),
    );

    if timely_disk_log.as_ref().map_or(true, |dir| dir.is_empty()) {
        if let Ok(addr) = differential_log_addr {
            if let Ok(stream) = TcpStream::connect(&addr) {
                differential_dataflow::logging::enable(worker, stream);

                tracing::info!("connected to differential log stream at {}", addr);
            } else {
                anyhow::bail!("Could not connect to differential log address: {:?}", addr);
            }
        }
    } else {
        if let Ok(dir) = timely_disk_log {
            if !dir.is_empty() {
                ddshow_sink::save_timely_logs_to_disk(worker, &dir).unwrap();
                tracing::info!("saving timely logs to {}", dir);
            }
        }

        if let Ok(dir) = differential_disk_log {
            if !dir.is_empty() {
                ddshow_sink::save_differential_logs_to_disk(worker, &dir).unwrap();
                tracing::info!("saving differential logs to {}", dir);
            }
        }
    }

    Ok(())
}
