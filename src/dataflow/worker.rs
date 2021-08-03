use crate::{
    args::Args,
    dataflow::{
        self,
        constants::FILE_SOURCED_FUEL,
        operators::{EventIterator, Fuel, InspectExt, ReplayWithShutdown},
        utils::{self, Time},
        DataflowSenders,
    },
    logging,
    replay_loading::{
        DifferentialReplaySource, ProgressReplaySource, ReplaySource, TimelyReplaySource,
    },
};
use anyhow::Result;
use ddshow_types::{
    differential_logging::DifferentialEvent, progress_logging::TimelyProgressEvent,
    timely_logging::TimelyEvent, WorkerId,
};
use differential_dataflow::{logging::DifferentialEvent as RawDifferentialEvent, Data};
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressFinish, ProgressStyle};
use std::{
    fmt::Debug,
    panic::Location,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};
use timely::{
    communication::Allocate,
    dataflow::{
        operators::{Filter, Map},
        ProbeHandle, Scope, Stream,
    },
    logging::TimelyEvent as RawTimelyEvent,
    worker::Worker,
};

/// The main runtime for a ddshow timely worker
#[allow(clippy::too_many_arguments)]
pub fn worker_runtime<A>(
    worker: &mut Worker<A>,
    args: Arc<Args>,
    senders: DataflowSenders,
    replay_shutdown: Arc<AtomicBool>,
    workers_finished: Arc<AtomicUsize>,
    replays_finished: Arc<AtomicUsize>,
    multi_progress: Arc<MultiProgress>,
    timely_traces: TimelyReplaySource,
    differential_traces: Option<DifferentialReplaySource>,
    progress_traces: Option<ProgressReplaySource>,
) -> Result<()>
where
    A: Allocate,
{
    tracing::info!(
        "spun up timely worker {}/{}",
        worker.index() + 1,
        args.workers,
    );

    // Timely has builtin log hooks, remove them all
    logging::unset_logging_hooks(worker);

    // If self profiling is enabled, set logging hooks within timely
    if args.dataflow_profiling {
        logging::init_dataflow_logging(worker)?;
    }

    // worker
    //     .log_register()
    //     .insert::<RawTimelyEvent, _>("timely", move |outer_time, data| {
    //         for (inner_time, worker, event) in data {
    //             tracing::trace!(
    //                 target: "ddshow_timely_events",
    //                 outer_time = ?*outer_time,
    //                 inner_time = ?*inner_time,
    //                 worker = *worker,
    //                 event = ?&*event,
    //             );
    //         }
    //     });

    let dataflow_id = worker.next_dataflow_index();
    let mut progress_bars = Vec::new();

    let (index, peers, differential, progress) = (
        worker.index(),
        worker.peers(),
        args.differential_enabled as usize,
        args.progress_enabled as usize,
    );

    let mut source_counter = {
        let timely_offset = index;
        let differential_offset = (index * differential) + differential;
        let progress_offset = (index * progress) + progress;

        timely_offset + differential_offset + progress_offset
    };
    let total_sources = {
        let timely_sources = worker.peers();
        let differential_sources = peers * differential;
        let progress_sources = peers * progress;

        timely_sources + differential_sources + progress_sources
    };

    tracing::debug!(
        worker = index,
        peers = peers,
        differential = differential,
        progress = progress,
        source_counter = source_counter,
        total_sources = total_sources,
    );

    let probes = worker.dataflow_named("DDShow Analysis Dataflow", |scope| {
        // If the dataflow is being sourced from a file, limit the
        // number of events read out in each batch so we don't overload
        // downstream consumers
        // TODO: Maybe fuel should always be unlimited so that the
        //       "loading trace data" and "processing data"
        //       prompts are accurate to the user? Does that matter
        //       enough to take precedence over the possible performance
        //       impact? (is there a perf impact?)
        let fuel = if args.is_file_sourced() {
            Fuel::limited(FILE_SOURCED_FUEL)

        // If the dataflow is being sourced from a running program,
        // take as many events as we possibly can so that we don't
        // slow down its execution
        } else {
            Fuel::unlimited()
        };

        tracing::trace!(
            worker_id = scope.index(),
            fuel = ?fuel,
            "giving worker {} {} replay fuel",
            scope.index() + 1,
            if fuel.is_unlimited() { "unlimited" } else { "limited" },
        );

        // The timely log stream filtered down to worker 0's events
        let span = tracing::info_span!("replay timely logs", worker_id = scope.index());
        let timely_stream = span.in_scope(|| {
            replay_traces::<_, TimelyEvent, RawTimelyEvent, _, _>(
                scope,
                &args,
                timely_traces,
                replay_shutdown.clone(),
                replays_finished.clone(),
                fuel.clone(),
                &multi_progress,
                "Timely",
                &mut progress_bars,
                &mut source_counter,
                total_sources,
            )
        });

        let span = tracing::info_span!("replay differential logs", worker_id = scope.index());
        let differential_stream = span.in_scope(|| {
            if let Some(traces) = differential_traces {
                let stream = replay_traces::<_, DifferentialEvent, RawDifferentialEvent, _, _>(
                    scope,
                    &args,
                    traces,
                    replay_shutdown.clone(),
                    replays_finished.clone(),
                    fuel.clone(),
                    &multi_progress,
                    "Differential",
                    &mut progress_bars,
                    &mut source_counter,
                    total_sources,
                )
                .filter(|(_, _, event)| !event.is_trace_share());

                Some(stream)
            } else {
                tracing::trace!("no differential sources were provided");
                None
            }
        });

        let span = tracing::info_span!("replay timely progress logs", worker_id = scope.index());
        let progress_stream = span.in_scope(|| {
            if let Some(traces) = progress_traces {
                if traces.is_abomonation() {
                    anyhow::bail!("Timely progress logging is only supported with rkyv sources",);
                }

                let stream = replay_traces::<_, TimelyProgressEvent, TimelyProgressEvent, _, _>(
                    scope,
                    &args,
                    traces,
                    replay_shutdown.clone(),
                    replays_finished.clone(),
                    fuel.clone(),
                    &multi_progress,
                    "Progress",
                    &mut progress_bars,
                    &mut source_counter,
                    total_sources,
                );

                Ok(Some(stream))
            } else {
                tracing::trace!("no progress sources were provided");
                Ok(None)
            }
        })?;

        let span = tracing::info_span!("dataflow construction", worker_id = scope.index());
        span.in_scope(|| {
            dataflow::dataflow(
                scope,
                &*args,
                &timely_stream,
                differential_stream.as_ref(),
                progress_stream.as_ref(),
                senders.clone(),
            )
        })
    })?;

    // Spawn a thread to display the worker progress bars
    if worker.index() == 0 {
        thread::spawn(move || {
            multi_progress.join().expect("failed to poll progress bars");
        });
    }

    'work_loop: while !probes.iter().all(|(probe, _)| probe.done()) {
        let start_time = Instant::now();
        if !replay_shutdown.load(Ordering::Acquire) {
            tracing::info!(
                worker_id = worker.index(),
                dataflow_id = dataflow_id,
                "forcibly shutting down dataflow {} on worker {}",
                dataflow_id,
                worker.index(),
            );

            worker.drop_dataflow(dataflow_id);
            break 'work_loop;
        }

        if !worker.step_or_park(Some(Duration::from_millis(500))) {
            tracing::info!(
                worker_id = worker.index(),
                "worker {} no longer has work to do",
                worker.index(),
            );

            break 'work_loop;
        }

        let elapsed = start_time.elapsed();
        tracing::debug!(
            target: "worker_step_events",
            probes = ?probes
                .iter()
                .map(|(probe, name)| probe.with_frontier(|frontier| format!("{}: {:?}", name, frontier)))
                .collect::<Vec<_>>(),
            "worker {} stepped for {:#?}",
            worker.index(),
            elapsed,
        );
    }

    for (bar, style) in progress_bars {
        bar.set_style(style);
        bar.finish_using_style();
    }

    tracing::info!(
        workers_finished = workers_finished.fetch_add(1, Ordering::Release),
        "timely worker {}/{} finished ({}/{} workers have finished)",
        worker.index() + 1,
        args.workers,
        workers_finished.fetch_add(1, Ordering::Release),
        args.workers,
    );

    Ok(())
}

#[track_caller]
#[allow(clippy::too_many_arguments)]
fn replay_traces<S, Event, RawEvent, R, A>(
    scope: &mut S,
    args: &Args,
    traces: ReplaySource<R, A>,
    replay_shutdown: Arc<AtomicBool>,
    replays_finished: Arc<AtomicUsize>,
    fuel: Fuel,
    multi_progress: &MultiProgress,
    source: &'static str,
    progress_bars: &mut Vec<(ProgressBar, ProgressStyle)>,
    source_counter: &mut usize,
    total_sources: usize,
) -> Stream<S, (Duration, WorkerId, Event)>
where
    S: Scope<Timestamp = Time>,
    Event: Data + From<RawEvent> + Send,
    RawEvent: Debug + Clone + Send + 'static,
    R: EventIterator<Duration, (Duration, WorkerId, Event)> + Send + 'static,
    A: EventIterator<Duration, (Duration, usize, RawEvent)> + Send + 'static,
{
    let caller = Location::caller();
    let name = format!(
        "{} Replay @ {}:{}:{}",
        source,
        caller.file(),
        caller.line(),
        caller.column(),
    );

    let style = ProgressStyle::default_spinner()
        .tick_chars("⠁⠂⠄⡀⢀⠠⠐⠈ ")
        .template("[{prefix}, {elapsed}] {spinner} {msg}: {len} events, {per_sec}")
        .on_finish(ProgressFinish::WithMessage(
            format!("Finished replaying {} trace", source.to_lowercase()).into(),
        ));
    let finished_style = ProgressStyle::default_spinner()
        .tick_chars("⠁⠂⠄⡀⢀⠠⠐⠈ ")
        .template("[{prefix}, {elapsed}] Finished replaying {len} {msg} trace")
        .on_finish(ProgressFinish::WithMessage(source.to_lowercase().into()));

    let padding_width = if total_sources >= 100 {
        3
    } else if total_sources >= 10 {
        2
    } else {
        1
    };
    let draw_target = if args.is_quiet() {
        ProgressDrawTarget::hidden()
    } else {
        ProgressDrawTarget::stdout()
    };

    let progress = multi_progress
        .add(
            ProgressBar::with_draw_target(0, draw_target)
                .with_style(style)
                .with_prefix(format!(
                    "{:0width$}/{:0width$}",
                    *source_counter,
                    total_sources,
                    width = padding_width,
                )),
        )
        .with_message(format!("Replaying {} trace", source.to_lowercase()));

    // I'm a genius, giving every bar the same tick speed looks weird
    // and artificial (almost like the spinners don't actually mean anything),
    // so each spinner gets a little of an offset so that all of them are
    // slightly out of sync
    utils::set_steady_tick(&progress, *source_counter);

    progress_bars.push((progress.clone(), finished_style));
    *source_counter += 1;

    tracing::debug!(
        "replaying {} {} {} traces",
        traces.len(),
        traces.kind().to_lowercase(),
        source.to_lowercase(),
    );

    match traces {
        ReplaySource::Rkyv(rkyv) => rkyv.replay_with_shutdown_into_named(
            &name,
            scope,
            // TODO: Implement this
            ProbeHandle::new(),
            replay_shutdown,
            replays_finished,
            fuel,
            Some(progress),
        ),

        ReplaySource::Abomonation(abomonation) => abomonation
            .replay_with_shutdown_into_named(
                &name,
                scope,
                // TODO: Implement this
                ProbeHandle::new(),
                replay_shutdown,
                replays_finished,
                fuel,
                Some(progress),
            )
            .map(|(time, worker, event): (Duration, usize, RawEvent)| {
                (time, WorkerId::new(worker), Event::from(event))
            }),
    }
    .debug_inspect(move |x| tracing::trace!("{} event: {:?}", source, x))
    .debug_frontier()
}
