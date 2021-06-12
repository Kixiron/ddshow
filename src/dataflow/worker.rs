use crate::{
    args::Args,
    dataflow::{
        self,
        operators::{EventIterator, Fuel, InspectExt, ReplayWithShutdown},
        DataflowSenders,
    },
    logging,
    replay_loading::{DifferentialReplaySource, ReplaySource, TimelyReplaySource},
};
use anyhow::Result;
use ddshow_types::{
    differential_logging::DifferentialEvent, timely_logging::TimelyEvent, WorkerId,
};
use differential_dataflow::{logging::DifferentialEvent as RawDifferentialEvent, Data};
use indicatif::{MultiProgress, ProgressBar, ProgressFinish, ProgressStyle};
use std::{
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};
use timely::{
    communication::Allocate,
    dataflow::{operators::Map, Scope, Stream},
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
    multi_progress: Arc<MultiProgress>,
    timely_traces: TimelyReplaySource,
    differential_traces: Option<DifferentialReplaySource>,
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

    let dataflow_id = worker.next_dataflow_index();
    let (mut progress_bars, mut source_counter, total_sources) = (
        Vec::new(),
        (worker.index() * if args.differential_enabled { 2 } else { 1 }) + 1,
        worker.peers()
            + if args.differential_enabled {
                worker.peers()
            } else {
                0
            },
    );

    let probe = worker.dataflow_named("DDShow Analysis Dataflow", |scope| {
        // If the dataflow is being sourced from a file, limit the
        // number of events read out in each batch so we don't overload
        // downstream consumers
        // TODO: Maybe fuel should always be unlimited so that the
        //       "loading trace data" and "processing data"
        //       prompts are accurate to the user? Does that matter
        //       enough to take precedence over the possible performance
        //       impact? (is there a perf impact?)
        let fuel = if args.is_file_sourced() {
            Fuel::limited(NonZeroUsize::new(100_000_000).unwrap())

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
                timely_traces,
                replay_shutdown.clone(),
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
                    traces,
                    replay_shutdown.clone(),
                    fuel.clone(),
                    &multi_progress,
                    "Differential",
                    &mut progress_bars,
                    &mut source_counter,
                    total_sources,
                );

                Some(stream)
            } else {
                tracing::trace!("no differential sources were provided");
                None
            }
        });

        // let span =
        //     tracing::info_span!("replay timely progress logs", worker_id = scope.index());
        // let progress_stream = span.in_scope(|| {
        //     if let Some(traces) = progress_traces {
        //         let stream = match traces {
        //             ReplaySource::Rkyv(rkyv) => rkyv.replay_with_shutdown_into_named(
        //                 "Progress Replay",
        //                 scope,
        //                 replay_shutdown.clone(),
        //                 fuel,
        //             ),
        //
        //             ReplaySource::Abomonation(_) => anyhow::bail!(
        //                 "Timely progress logging is only supported with rkyv sources",
        //             ),
        //         }
        //         .debug_inspect(move |x| tracing::trace!("progress event: {:?}", x));
        //
        //         Ok(Some(stream))
        //     } else {
        //         tracing::trace!("no progress sources were provided");
        //         Ok(None)
        //     }
        // })?;

        let span = tracing::info_span!("dataflow construction", worker_id = scope.index());
        span.in_scope(|| {
            dataflow::dataflow(
                scope,
                &*args,
                &timely_stream,
                differential_stream.as_ref(),
                None, // progress_stream.as_ref(),
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

    'work_loop: while !probe.done() {
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

        worker.step_or_park(Some(Duration::from_millis(500)));
    }

    for (bar, style) in progress_bars {
        bar.set_style(style);
        bar.finish_using_style();
    }

    let total_finished = workers_finished.fetch_add(1, Ordering::Release);
    tracing::info!(
        "timely worker {}/{} finished ({}/{} workers have finished)",
        worker.index() + 1,
        args.workers,
        total_finished,
        args.workers,
    );

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn replay_traces<S, Event, RawEvent, R, A>(
    scope: &mut S,
    traces: ReplaySource<R, A>,
    replay_shutdown: Arc<AtomicBool>,
    fuel: Fuel,
    multi_progress: &MultiProgress,
    source: &'static str,
    progress_bars: &mut Vec<(ProgressBar, ProgressStyle)>,
    source_counter: &mut usize,
    total_sources: usize,
) -> Stream<S, (Duration, WorkerId, Event)>
where
    S: Scope<Timestamp = Duration>,
    Event: Data + From<RawEvent>,
    RawEvent: Clone + 'static,
    R: EventIterator<Duration, (Duration, WorkerId, Event)> + 'static,
    A: EventIterator<Duration, (Duration, usize, RawEvent)> + 'static,
{
    let name = format!("{} Replay", source);

    let style = ProgressStyle::default_spinner()
        .tick_chars("⠁⠂⠄⡀⢀⠠⠐⠈ ")
        .template(
            "[{prefix}, {elapsed}] {spinner} {msg}: {len} events, {per_sec} events per second",
        )
        .on_finish(ProgressFinish::WithMessage(
            format!("Finished replaying {} events", source.to_lowercase()).into(),
        ));
    let finished_style = ProgressStyle::default_spinner()
        .tick_chars("⠁⠂⠄⡀⢀⠠⠐⠈ ")
        .template("[{prefix}, {elapsed}] Finished replaying {len} {msg} events")
        .on_finish(ProgressFinish::WithMessage(source.to_lowercase().into()));

    let progress = multi_progress
        .add(ProgressBar::new(0).with_style(style).with_prefix(format!(
            "{:0width$}/{:0width$}",
            *source_counter,
            total_sources,
            width = if total_sources >= 100 {
                3
            } else if total_sources >= 10 {
                2
            } else {
                1
            },
        )))
        .with_message(format!("Replaying {} events", source.to_lowercase()));

    // I'm a genius, giving every bar the same tick speed looks weird
    // and artificial (almost like the spinners don't actually mean anything),
    // so each spinner gets a little of an offset so that all of them are
    // slightly out of sync
    progress.enable_steady_tick(100 + (100 / (*source_counter as u64 + 1)));

    progress_bars.push((progress.clone(), finished_style));
    *source_counter += 1;

    match traces {
        ReplaySource::Rkyv(rkyv) => rkyv.replay_with_shutdown_into_named(
            &name,
            scope,
            replay_shutdown,
            fuel,
            Some(progress),
        ),

        ReplaySource::Abomonation(abomonation) => abomonation
            .replay_with_shutdown_into_named(&name, scope, replay_shutdown, fuel, Some(progress))
            .map(|(time, worker, event): (Duration, usize, RawEvent)| {
                (time, WorkerId::new(worker), Event::from(event))
            }),
    }
    .debug_inspect(move |x| tracing::trace!("{} event: {:?}", source, x))
}
