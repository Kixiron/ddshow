mod args;
mod colormap;
mod dataflow;
mod network;
mod report;
mod ui;

use crate::{
    args::Args,
    colormap::{select_color, Color},
    dataflow::{
        operators::{Fuel, InspectExt, ReplayWithShutdown},
        Channel, DataflowData, DataflowSenders, OperatorStats,
    },
    network::{acquire_replay_sources, wait_for_input, ReplaySource},
    ui::{ActivationDuration, DDShowStats, EdgeKind, Lifespan, TimelineEvent},
};
use anyhow::{Context, Result};
use ddshow_types::{
    timely_logging::{OperatesEvent, TimelyEvent},
    OperatorAddr, OperatorId, WorkerId,
};
use differential_dataflow::logging::DifferentialEvent;
use std::{
    collections::HashMap,
    env,
    fs::{self, File},
    io::BufWriter,
    net::TcpStream,
    num::NonZeroUsize,
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use structopt::StructOpt;
use timely::{dataflow::operators::Map, logging::TimelyEvent as RawTimelyEvent};
use tracing_subscriber::{
    fmt::time::Uptime, prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt,
    EnvFilter,
};

/// The current version of DDShow
pub static DDSHOW_VERSION: &str = env!("VERGEN_GIT_SEMVER");

// TODO: Library-ify a lot of this
// FIXME: Clean this up so much
// TODO: Set the panic hook to shut down the computation
//       so that panics don't stick things
fn main() -> Result<()> {
    init_logging();

    // Grab the args from the user and build the required configs
    let args = Arc::new(Args::from_args());
    tracing::trace!("initialized and received cli args: {:?}", args);

    let config = args.timely_config();

    // Connect to the timely sources
    let timely_event_receivers = acquire_replay_sources(
        args.timely_address,
        args.timely_connections,
        args.workers,
        args.replay_logs.as_deref(),
        "timely.ddshow",
        "Timely",
    )?;

    // Connect to the differential sources
    let differential_event_receivers = if args.differential_enabled {
        Some(acquire_replay_sources::<Duration, DifferentialEvent, _>(
            args.differential_address,
            args.timely_connections,
            args.workers,
            args.replay_logs.as_deref(),
            "differential.ddshow",
            "Differential",
        )?)
    } else {
        None
    };

    let (running, workers_finished) = (
        Arc::new(AtomicBool::new(true)),
        Arc::new(AtomicUsize::new(0)),
    );
    let (replay_shutdown, moved_args, moved_workers_finished) =
        (running.clone(), args.clone(), workers_finished.clone());

    // Create the *many* channels used for extracting data from the dataflow
    let (senders, receivers) = DataflowSenders::create();

    tracing::info!("starting compute dataflow");

    // Spin up the timely computation
    let worker_guards = timely::execute(config, move |worker| {
        let dataflow_name = concat!(env!("CARGO_CRATE_NAME"), " log processor");
        let args = moved_args.clone();
        tracing::info!(
            "spun up timely worker {}/{}",
            worker.index() + 1,
            args.workers,
        );

        if cfg!(debug_assertions) {
            if let Ok(addr) = env::var("DIFFERENTIAL_LOG_ADDR") {
                if let Ok(stream) = TcpStream::connect(&addr) {
                    differential_dataflow::logging::enable(worker, stream);
                    tracing::info!("connected to differential log stream at {}", addr);
                } else {
                    panic!("Could not connect to differential log address: {:?}", addr);
                }
            }
        }

        // Distribute the tcp streams across workers, converting each of them into an event reader
        let timely_traces = timely_event_receivers[worker.index()]
            .clone()
            .recv()
            .expect("failed to receive timely event traces");

        let differential_traces = differential_event_receivers.as_ref().map(|recv| {
            recv[worker.index()]
                .recv()
                .expect("failed to receive differential event traces")
        });

        let dataflow_id = worker.next_dataflow_index();
        let probe = worker.dataflow_named(dataflow_name, |scope| {
            // If the dataflow is being sourced from a file, limit the
            // number of events read out in each batch so we don't overload
            // downstream consumers
            let fuel = if args.is_file_sourced() {
                Fuel::limited(NonZeroUsize::new(100_000_000).unwrap())

            // If the dataflow is being sourced from a running program,
            // take as many events as we possibly can so that we don't
            // slow down its execution
            } else {
                Fuel::unlimited()
            };

            // The timely log stream filtered down to worker 0's events
            let timely_stream = match timely_traces {
                ReplaySource::Rkyv(rkyv) => rkyv.replay_with_shutdown_into_named(
                    "Timely Replay",
                    scope,
                    replay_shutdown.clone(),
                    fuel.clone(),
                ),

                ReplaySource::Abomonation(abomonation) => abomonation
                    .replay_with_shutdown_into_named(
                        "Timely Replay",
                        scope,
                        replay_shutdown.clone(),
                        fuel.clone(),
                    )
                    .map(|(time, worker, event): (Duration, usize, RawTimelyEvent)| {
                        (time, WorkerId::new(worker), TimelyEvent::from(event))
                    }),
            }
            .debug_inspect(|x| tracing::trace!("timely event: {:?}", x));

            let differential_stream = differential_traces.map(|traces| {
                match traces {
                    ReplaySource::Rkyv(_rkyv) => {
                        // rkyv.replay_with_shutdown_into_named(
                        //     "Differential Replay",
                        //     scope,
                        //     replay_shutdown.clone(),
                        //     fuel,
                        // )
                        todo!("Differential disk replays are not yet implemented")
                    }

                    ReplaySource::Abomonation(abomonation) => abomonation
                        .replay_with_shutdown_into_named(
                            "Differential Replay",
                            scope,
                            replay_shutdown.clone(),
                            fuel,
                        ),
                }
                .map(|(time, worker, event)| (time, WorkerId::new(worker), event))
                .debug_inspect(|x| tracing::trace!("differential dataflow event: {:?}", x))
            });

            dataflow::dataflow(
                scope,
                &*args,
                &timely_stream,
                differential_stream.as_ref(),
                senders.clone(),
            )
        })?;

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

        let total_finished = moved_workers_finished.fetch_add(1, Ordering::Release);
        tracing::info!(
            "timely worker {}/{} finished ({}/{} workers have finished)",
            worker.index() + 1,
            args.workers,
            total_finished,
            args.workers,
        );

        Ok(())
    })
    .map_err(|err| anyhow::anyhow!("failed to start up timely computation: {}", err))?;

    // Wait for the user's prompt
    let data = wait_for_input(
        &*args,
        &*running,
        &*workers_finished,
        worker_guards,
        receivers,
    )?;
    let name_lookup: HashMap<_, _> = data.name_lookup.iter().cloned().collect();
    let addr_lookup: HashMap<_, _> = data.addr_lookup.iter().cloned().collect();

    // Build & emit the textual report
    report::build_report(&*args, &data, &name_lookup, &addr_lookup)?;

    if let Some(file) = args.dump_json.as_ref() {
        dump_program_json(&*args, file, &data, &name_lookup, &addr_lookup)?;
    }

    // Extract the data from timely
    let mut subgraph_ids = Vec::new();

    let mut node_events = data.nodes;
    node_events.sort_unstable_by_key(|(addr, _)| addr.clone());
    tracing::debug!("finished extracting {} node events", node_events.len());

    let mut subgraph_events = data.subgraphs;
    subgraph_events.sort_unstable_by_key(|(addr, _)| addr.clone());
    tracing::debug!(
        "finished extracting {} subgraph events",
        subgraph_events.len(),
    );

    for &((worker, ref _addr), ref event) in subgraph_events.iter() {
        subgraph_ids.push((worker, event.id));
    }

    let (mut operator_stats, mut raw_timings) = (HashMap::new(), Vec::new());
    let stats_events = data.operator_stats;
    tracing::debug!("finished extracting {} stats events", stats_events.len());

    for (operator, stats) in stats_events.clone() {
        if !subgraph_ids.contains(&operator) {
            raw_timings.push(stats.total);
        }

        operator_stats.insert(operator, stats);
    }

    let mut edge_events = data.edges;
    edge_events.sort_unstable_by_key(|(worker, _, channel, _)| (*worker, channel.channel_id()));
    tracing::debug!("finished extracting {} edge events", edge_events.len());

    let (max_time, min_time) = (
        raw_timings.iter().max().copied().unwrap_or_default(),
        raw_timings.iter().min().copied().unwrap_or_default(),
    );

    let mut timeline_events = data.timeline_events;
    timeline_events.sort_unstable_by_key(|event| (event.worker, event.start_time));

    tracing::debug!(
        "finished extracting {} timeline events",
        timeline_events.len(),
    );

    let html_nodes: Vec<_> = node_events
        .into_iter()
        .filter_map(|((worker, addr), OperatesEvent { id, name, .. })| {
            let &OperatorStats {
                max,
                min,
                average,
                total,
                activations: invocations,
                ref activation_durations,
                ref arrangement_size,
                ..
            } = operator_stats.get(&(worker, id))?;

            let fill_color = select_color(&args.palette, total, (max_time, min_time));
            let text_color = fill_color.text_color();

            Some(ui::Node {
                id,
                worker,
                addr,
                name,
                max_activation_time: format!("{:#?}", max),
                min_activation_time: format!("{:#?}", min),
                average_activation_time: format!("{:#?}", average),
                total_activation_time: format!("{:#?}", total),
                invocations,
                fill_color: format!("{}", fill_color),
                text_color: format!("{}", text_color),
                activation_durations: activation_durations
                    .iter()
                    .map(|(duration, time)| ActivationDuration {
                        activation_time: duration.as_nanos() as u64,
                        activated_at: time.as_nanos() as u64,
                    })
                    .collect(),
                max_arrangement_size: arrangement_size.as_ref().map(|arr| arr.max_size),
                min_arrangement_size: arrangement_size.as_ref().map(|arr| arr.min_size),
            })
        })
        .collect();

    let html_subgraphs: Vec<_> = subgraph_events
        .into_iter()
        .filter_map(|((worker, addr), OperatesEvent { id, name, .. })| {
            let OperatorStats {
                max,
                min,
                average,
                total,
                activations: invocations,
                ..
            } = *operator_stats.get(&(worker, id))?;

            let fill_color = select_color(&args.palette, total, (max, min));
            let text_color = fill_color.text_color();

            Some(ui::Subgraph {
                id,
                worker,
                addr,
                name,
                max_activation_time: format!("{:#?}", max),
                min_activation_time: format!("{:#?}", min),
                average_activation_time: format!("{:#?}", average),
                total_activation_time: format!("{:#?}", total),
                invocations,
                fill_color: format!("{}", fill_color),
                text_color: format!("{}", text_color),
            })
        })
        .collect();

    let html_edges: Vec<_> = edge_events
        // .clone()
        .into_iter()
        .map(
            |(
                worker,
                OperatesEvent { addr: src, .. },
                channel,
                OperatesEvent { addr: dest, .. },
            )| {
                ui::Edge {
                    src,
                    dest,
                    worker,
                    channel_id: channel.channel_id(),
                    edge_kind: match channel {
                        Channel::Normal { .. } => EdgeKind::Normal,
                        Channel::ScopeCrossing { .. } => EdgeKind::Crossing,
                    },
                }
            },
        )
        .collect();

    let mut palette_colors = Vec::with_capacity(10);
    let mut pos = 0.0;
    for _ in 0..10 {
        palette_colors.push(format!("{}", Color::new(args.palette.eval_continuous(pos))));
        pos += 0.1;
    }

    ui::render(
        &args,
        html_nodes,
        html_subgraphs,
        html_edges,
        palette_colors,
        timeline_events,
    )?;

    println!(" done!");

    if !args.no_report_file {
        println!("Wrote report file to {}", args.report_file.display());
    }

    println!(
        "Wrote output graph to file:///{}",
        fs::canonicalize(&args.output_dir)
            .context("failed to get path of output dir")?
            .join("graph.html")
            .display(),
    );

    Ok(())
}

fn dump_program_json(
    args: &Args,
    file: &Path,
    data: &DataflowData,
    _name_lookup: &HashMap<(WorkerId, OperatorId), String>,
    _addr_lookup: &HashMap<(WorkerId, OperatorId), OperatorAddr>,
) -> Result<()> {
    let file = BufWriter::new(File::create(file).context("failed to create json file")?);

    let program = data.program_stats[0].clone();
    let workers = data.worker_stats[0]
        .iter()
        .map(|(_, stats)| stats.clone())
        .collect();
    let dataflows = data.dataflow_stats.clone();
    let events = data
        .timeline_events
        .iter()
        .map(|event| TimelineEvent {
            worker: event.worker,
            event: (),
            lifespan: Lifespan::new(
                Duration::from_nanos(event.start_time),
                Duration::from_nanos(event.start_time + event.duration),
            ),
        })
        .collect();

    let data = DDShowStats {
        program,
        workers,
        dataflows,
        // FIXME: Do these
        nodes: Vec::new(),
        channels: Vec::new(),
        arrangements: Vec::new(),
        events,
        differential_enabled: args.differential_enabled,
        ddshow_version: DDSHOW_VERSION.to_string(),
    };

    serde_json::to_writer(file, &data).context("failed to write json to file")?;

    Ok(())
}

fn init_logging() {
    let filter_layer = EnvFilter::from_env("DDSHOW_LOG");
    let fmt_layer = tracing_subscriber::fmt::layer()
        .pretty()
        .with_timer(Uptime::default())
        .with_thread_names(true)
        .with_ansi(true);

    let _ = tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt_layer)
        .try_init();
}
