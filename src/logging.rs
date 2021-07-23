use crate::args::{Args, TerminalColor};
use anyhow::Result;
use ddshow_sink::{
    DIFFERENTIAL_ARRANGEMENT_LOGGER_NAME, TIMELY_LOGGER_NAME, TIMELY_PROGRESS_LOGGER_NAME,
};
use std::env;
use timely::{communication::Allocate, worker::Worker};
use tracing_subscriber::{
    fmt::time::Uptime, prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt,
    EnvFilter,
};

pub(crate) fn init_logging(args: &Args) {
    let ansi_enabled = match args.color {
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
    let (timely_disk_log, _progress_disk_log, differential_disk_log) = (
        env::var("TIMELY_DISK_LOG"),
        env::var("TIMELY_PROGRESS_DISK_LOG"),
        env::var("DIFFERENTIAL_DISK_LOG"),
    );

    if let Ok(dir) = timely_disk_log {
        if !dir.is_empty() {
            ddshow_sink::save_timely_logs_to_disk(worker, &dir).unwrap();
            tracing::info!("saving timely logs to {}", dir);
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
            ddshow_sink::save_differential_logs_to_disk(worker, &dir).unwrap();
            tracing::info!("saving differential logs to {}", dir);
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

    for hook in builtin_hooks.iter() {
        if register.remove(hook).is_some() {
            tracing::debug!(hook_name = hook, "removed builtin logging hook from timely");
        }
    }
}
