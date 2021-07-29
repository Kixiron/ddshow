use crate::args::TerminalColor;
use anyhow::{Context, Result};
use ddshow_sink::{
    DIFFERENTIAL_ARRANGEMENT_LOGGER_NAME, TIMELY_LOGGER_NAME, TIMELY_PROGRESS_LOGGER_NAME,
};
use differential_dataflow::logging::DifferentialEvent as RawDifferentialEvent;
use std::{env, net::TcpStream};
use timely::{
    communication::Allocate,
    dataflow::operators::capture::EventWriter as TimelyEventWriter,
    logging::{BatchLogger as TimelyBatchLogger, TimelyEvent as RawTimelyEvent},
    worker::Worker,
};
use tracing_subscriber::{
    fmt::time::Uptime, prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt,
    EnvFilter,
};

pub(crate) fn init_logging(color: TerminalColor) {
    let ansi_enabled = match color {
        TerminalColor::Auto => atty::is(atty::Stream::Stdout),
        TerminalColor::Always => true,
        TerminalColor::Never => false,
    };

    let filter_layer = EnvFilter::from_env("DDSHOW_LOG");
    // TODO: Write an improved version of the pretty formatter
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_timer(Uptime::default())
        .with_thread_names(true)
        .with_ansi(ansi_enabled)
        .with_level(true);

    let registry = tracing_subscriber::registry().with(filter_layer);
    let _ = if cfg!(test) {
        registry.with(fmt_layer.with_test_writer()).try_init()
    } else {
        registry.with(fmt_layer).try_init()
    };
}

// TODO: Progress logging & configure logging via the cli
pub(crate) fn init_dataflow_logging<A>(worker: &mut Worker<A>) -> Result<()>
where
    A: Allocate,
{
    // If `DDSHOW_OBEY_TCP` is set to 1 then the tcp logging links take priority
    let obey_tcp_raw = env::var("DDSHOW_OBEY_TCP");
    let obey_tcp = obey_tcp_raw
        .ok()
        .filter(|val| !val.is_empty())
        .and_then(|value| value.parse::<isize>().ok())
        .map_or(false, |value| value == 1);
    tracing::debug!(DDSHOW_OBEY_TCP = ?obey_tcp, obey_tcp);

    if obey_tcp {
        tracing::info!("logging to a tcp-based consumer");

        let (timely_log_addr, progress_log_addr, differential_log_addr) = (
            env::var("TIMELY_LOG_ADDR"),
            env::var("TIMELY_PROGRESS_LOG_ADDR"),
            env::var("DIFFERENTIAL_LOG_ADDR"),
        );
        tracing::debug!(
            TIMELY_LOG_ADDR = ?timely_log_addr,
            TIMELY_PROGRESS_LOG_ADDR = ?progress_log_addr,
            DIFFERENTIAL_LOG_ADDR = ?differential_log_addr,
        );

        if let Ok(addr) = timely_log_addr {
            if !addr.is_empty() {
                let socket = TcpStream::connect(&addr).with_context(|| {
                    anyhow::anyhow!("failed to connect to timely log addr: {}", addr)
                })?;
                let mut logger = TimelyBatchLogger::new(TimelyEventWriter::new(socket));

                let old_logger = worker
                    .log_register()
                    .insert::<RawTimelyEvent, _>(TIMELY_LOGGER_NAME, move |time, data| {
                        logger.publish_batch(time, data)
                    });

                if let Some(old_logger) = old_logger {
                    tracing::warn!(
                        type_id = ?old_logger.type_id(),
                        "overwrote previously registered timely logger at {:p}",
                        old_logger,
                    );
                }

                tracing::info!("installed timely tcp log to {}", addr);
            }
        }

        // if let Ok(addr) = progress_log_addr {
        //     if !addr.is_empty() {
        //         todo!()
        //     }
        // }

        if let Ok(addr) = differential_log_addr {
            if !addr.is_empty() {
                let socket = TcpStream::connect(&addr).with_context(|| {
                    anyhow::anyhow!("failed to connect to differential log addr: {}", addr)
                })?;
                let mut logger = TimelyBatchLogger::new(TimelyEventWriter::new(socket));

                let old_logger = worker.log_register().insert::<RawDifferentialEvent, _>(
                    DIFFERENTIAL_ARRANGEMENT_LOGGER_NAME,
                    move |time, data| logger.publish_batch(time, data),
                );

                if let Some(old_logger) = old_logger {
                    tracing::warn!(
                        type_id = ?old_logger.type_id(),
                        "overwrote previously registered differential logger at {:p}",
                        old_logger,
                    );
                }

                tracing::info!("installed differential tcp log to {}", addr);
            }
        }
    } else {
        tracing::info!("logging to a disk-based consumer");

        let (timely_disk_log, progress_disk_log, differential_disk_log) = (
            env::var("TIMELY_DISK_LOG"),
            env::var("TIMELY_PROGRESS_DISK_LOG"),
            env::var("DIFFERENTIAL_DISK_LOG"),
        );
        tracing::debug!(
            TIMELY_DISK_LOG = ?timely_disk_log,
            TIMELY_PROGRESS_DISK_LOG = ?progress_disk_log,
            DIFFERENTIAL_DISK_LOG = ?differential_disk_log,
        );

        if let Ok(dir) = timely_disk_log {
            if !dir.is_empty() {
                ddshow_sink::save_timely_logs_to_disk(worker, &dir).with_context(|| {
                    anyhow::anyhow!("failed to install timely disk logger to '{}'", dir)
                })?;

                tracing::info!("installed timely disk log to {}", dir);
            }
        }

        // if let Ok(dir) = progress_disk_log {
        //     if !dir.is_empty() {
        //         ddshow_sink::save_timely_progress_to_disk(worker, &dir).unwrap();
        //         tracing::info!("saving timely progress logs to {}", dir);
        //     }
        // }

        if let Ok(dir) = differential_disk_log {
            if !dir.is_empty() {
                ddshow_sink::save_differential_logs_to_disk(worker, &dir).with_context(|| {
                    anyhow::anyhow!("failed to install differential disk logger to '{}'", dir)
                })?;

                tracing::info!("installed differential disk log to {}", dir);
            }
        }
    }

    Ok(())
}

/// Timely may register some logging hooks automatically,
/// this just attempts to remove all of them
pub(crate) fn unset_logging_hooks<A>(worker: &mut Worker<A>)
where
    A: Allocate,
{
    let mut register = worker.log_register();
    let builtin_hooks = [
        TIMELY_LOGGER_NAME,
        DIFFERENTIAL_ARRANGEMENT_LOGGER_NAME,
        TIMELY_PROGRESS_LOGGER_NAME,
    ];

    for hook_name in IntoIterator::into_iter(builtin_hooks) {
        if let Some(hook) = register.remove(hook_name) {
            tracing::debug!(
                hook_name = hook_name,
                hook_type = ?hook.type_id(),
                "removed builtin logging hook from timely at {:p}",
                hook,
            );
        }
    }
}
