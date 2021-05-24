mod args;
mod colormap;
mod dataflow;
mod network;
mod ui;

use crate::{
    args::Args,
    colormap::{select_color, Color},
    dataflow::{
        operators::{
            make_streams, rkyv_capture::RkyvOperatesEvent, CrossbeamExtractor, EventReader, Fuel,
            InspectExt, ReplayWithShutdown, RkyvEventReader, RkyvTimelyEvent,
        },
        Channel, DataflowSenders, DifferentialLogBundle, OperatorAddr, OperatorId, OperatorStats,
        WorkerId, WorkerTimelineEvent,
    },
    network::{wait_for_connections, wait_for_input, ReplaySource},
    ui::{ActivationDuration, EdgeKind},
};
use anyhow::{Context, Result};
use comfy_table::{presets::UTF8_FULL, Cell, ColumnConstraint, ContentArrangement, Table};
use std::{
    cmp::Reverse,
    collections::HashMap,
    env,
    fs::{self, File},
    io::{BufReader, Write},
    net::TcpStream,
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use structopt::StructOpt;
use timely::{
    dataflow::operators::{capture::Extract, Map},
    logging::TimelyEvent,
};
use tracing_subscriber::{
    fmt::time::Uptime, prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt,
    EnvFilter,
};

// TODO: Library-ify a lot of this
// FIXME: Clean this up so much
fn main() -> Result<()> {
    init_logging();

    // Grab the args from the user and build the required configs
    let args = Arc::new(Args::from_args());
    tracing::trace!("initialized and received cli args: {:?}", args);

    let config = args.timely_config();

    tracing::info!(
        "started waiting for {} timely connections on {}",
        args.timely_connections,
        args.address,
    );
    println!(
        "Waiting for {} Timely connection{} on {}...",
        args.timely_connections,
        if args.timely_connections.get() == 1 {
            ""
        } else {
            "s"
        },
        args.address,
    );
    let start_time = Instant::now();

    let timely_connections = if let Some(log_dir) = args.replay_logs.as_ref() {
        let timely_file = File::open(log_dir.join("timely.ddshow"))
            .context("failed to open timely log file within replay directory")?;

        ReplaySource::Rkyv(vec![RkyvEventReader::new(BufReader::new(timely_file))])
    } else {
        wait_for_connections(args.address, args.timely_connections)?
    };

    let event_receivers = make_streams(args.workers.get(), timely_connections)?;

    if args.replay_logs.is_none() {
        let elapsed = start_time.elapsed();
        println!(
            "Connected to {} Timely trace{} in {:#?}",
            args.timely_connections,
            if args.timely_connections.get() == 1 {
                ""
            } else {
                "s"
            },
            elapsed,
        );
    }

    let differential_receivers = if args.differential_enabled {
        tracing::info!(
            "differential dataflow logging is enabled, started waiting for {} differential connections on {}",
            args.timely_connections,
            args.differential_address,
        );

        println!(
            "Waiting for {} Differential connection{} on {}...",
            args.timely_connections,
            if args.timely_connections.get() == 1 {
                ""
            } else {
                "s"
            },
            args.differential_address,
        );
        let start_time = Instant::now();

        type DifferentialReader = EventReader<Duration, DifferentialLogBundle<usize>, TcpStream>;
        let differential_connections: ReplaySource<DifferentialReader, DifferentialReader> =
            // TODO: Differential replays
            if let Some(log_dir) = args.replay_logs.as_ref() {
                // let differential_file = File::open(log_dir.join("differential.ddshow"))
                //     .context("failed to open differential log file within replay directory")?;
                //
                // vec![
                //     Box::new(RkyvEventReader::new(BufReader::new(differential_file)))
                //         as Box<dyn EventIterator<_, _> + Send + 'static>,
                // ]

                if log_dir.join("differential.ddshow").exists() {
                    tracing::warn!("differential file replays are unimplemented");
                }

                ReplaySource::Rkyv(Vec::new())
            } else {
                wait_for_connections(args.differential_address, args.timely_connections)?
            };
        let event_receivers = make_streams(args.workers.get(), differential_connections)?;

        let elapsed = start_time.elapsed();
        println!(
            "Connected to {} Differential trace{} in {:#?}",
            args.timely_connections,
            if args.timely_connections.get() == 1 {
                ""
            } else {
                "s"
            },
            elapsed,
        );

        Some(event_receivers)
    } else {
        None
    };

    let (running, workers_finished) = (
        Arc::new(AtomicBool::new(true)),
        Arc::new(AtomicUsize::new(0)),
    );
    let (replay_shutdown, moved_args, moved_workers_finished) =
        (running.clone(), args.clone(), workers_finished.clone());

    let (node_sender, node_receiver) = crossbeam_channel::unbounded();
    let (edge_sender, edge_receiver) = crossbeam_channel::unbounded();
    let (subgraph_sender, subgraph_receiver) = crossbeam_channel::unbounded();
    let (stats_sender, stats_receiver) = crossbeam_channel::unbounded();
    let (agg_stats_sender, agg_stats_receiver) = crossbeam_channel::unbounded();
    let (timeline_sender, timeline_receiver) = crossbeam_channel::unbounded();
    let (program_stats_sender, program_stats_receiver) = crossbeam_channel::unbounded();
    let (worker_stats_sender, worker_stats_receiver) = crossbeam_channel::unbounded();
    let (name_lookup_sender, name_lookup_receiver) = crossbeam_channel::unbounded();
    let (addr_lookup_sender, addr_lookup_receiver) = crossbeam_channel::unbounded();

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
        let timely_traces = event_receivers[worker.index()]
            .clone()
            .recv()
            .expect("failed to receive timely event traces");

        let differential_traces = differential_receivers.as_ref().map(|recv| {
            recv[worker.index()]
                .recv()
                .expect("failed to receive differential event traces")
        });

        let senders = DataflowSenders::new(
            node_sender.clone(),
            edge_sender.clone(),
            subgraph_sender.clone(),
            stats_sender.clone(),
            agg_stats_sender.clone(),
            timeline_sender.clone(),
            program_stats_sender.clone(),
            worker_stats_sender.clone(),
            name_lookup_sender.clone(),
            addr_lookup_sender.clone(),
        );

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
                    .map(|(time, worker, event): (Duration, usize, TimelyEvent)| {
                        (time, WorkerId::new(worker), RkyvTimelyEvent::from(event))
                    }),
            }
            .debug_inspect(|x| tracing::trace!("timely event: {:?}", x));

            let differential_stream = differential_traces.map(|traces| {
                match traces {
                    ReplaySource::Rkyv(rkyv) => rkyv.replay_with_shutdown_into_named(
                        "Differential Replay",
                        scope,
                        replay_shutdown.clone(),
                        fuel,
                    ),

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
                senders,
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
    wait_for_input(&*running, &*workers_finished, worker_guards)?;

    let mut report_file = if args.no_report_file {
        None
    } else {
        Some(File::create(&args.report_file).context("failed to create report file")?)
    };

    let mut program_stats = CrossbeamExtractor::new(program_stats_receiver).extract_all();
    if let Some((report_file, stats)) = report_file
        .as_mut()
        .and_then(|file| program_stats.pop().map(|stats| (file, stats)))
    {
        let mut table = Table::new();

        table
            .load_preset(UTF8_FULL)
            .set_content_arrangement(ContentArrangement::Dynamic)
            .set_header(vec!["Program Overview", ""])
            .add_row(vec![Cell::new("Workers"), Cell::new(stats.workers)])
            .add_row(vec![Cell::new("Dataflows"), Cell::new(stats.dataflows)])
            .add_row(vec![Cell::new("Operators"), Cell::new(stats.operators)])
            .add_row(vec![Cell::new("Subgraphs"), Cell::new(stats.subgraphs)])
            .add_row(vec![Cell::new("Channels"), Cell::new(stats.channels)])
            .add_row(vec![Cell::new("Events"), Cell::new(stats.events)])
            .add_row(vec![
                Cell::new("Total Runtime"),
                Cell::new(format!("{:#?}", stats.runtime)),
            ]);

        writeln!(report_file, "{}\n", table).context("failed to write to report file")?;
    }

    let mut worker_stats = CrossbeamExtractor::new(worker_stats_receiver).extract_all();

    if let Some(report_file) = report_file.as_mut() {
        let mut table = Table::new();
        table
            .load_preset(UTF8_FULL)
            .set_content_arrangement(ContentArrangement::Dynamic)
            .set_header(vec![
                "Worker",
                "Dataflows",
                "Operators",
                "Subgraphs",
                "Channels",
                "Events",
                "Runtime",
            ]);

        debug_assert_eq!(worker_stats.len(), 1);
        // Ensure the vector is sorted
        debug_assert!(worker_stats[0].windows(2).all(|x| x[0] <= x[1]));
        for (worker, stats) in worker_stats.remove(0) {
            table.add_row(vec![
                Cell::new(format!("Worker {}", worker.into_inner())),
                Cell::new(stats.dataflows),
                Cell::new(stats.operators),
                Cell::new(stats.subgraphs),
                Cell::new(stats.channels),
                Cell::new(stats.events),
                Cell::new(format!("{:#?}", stats.runtime)),
            ]);
        }

        writeln!(report_file, "Per-Worker Statistics\n{}\n", table)
            .context("failed to write to report file")?;
    }

    // Extract the data from timely
    let mut subgraph_ids = Vec::new();

    let mut node_events = CrossbeamExtractor::new(node_receiver).extract_all();
    node_events.sort_unstable_by_key(|(addr, _)| addr.clone());
    tracing::info!("finished extracting {} node events", node_events.len());

    let mut subgraph_events = CrossbeamExtractor::new(subgraph_receiver).extract_all();
    subgraph_events.sort_unstable_by_key(|(addr, _)| addr.clone());
    tracing::info!(
        "finished extracting {} subgraph events",
        subgraph_events.len(),
    );

    let name_lookup: HashMap<(WorkerId, OperatorId), String> =
        CrossbeamExtractor::new(name_lookup_receiver)
            .extract_all()
            .into_iter()
            .collect();

    let addr_lookup: HashMap<(WorkerId, OperatorId), OperatorAddr> =
        CrossbeamExtractor::new(addr_lookup_receiver)
            .extract_all()
            .into_iter()
            .collect();

    for &((worker, ref _addr), ref event) in subgraph_events.iter() {
        subgraph_ids.push((worker, event.id));
    }

    let (mut operator_stats, mut raw_timings) = (HashMap::new(), Vec::new());
    let stats_events = CrossbeamExtractor::new(stats_receiver).extract_all();
    tracing::info!("finished extracting {} stats events", stats_events.len());

    for (operator, stats) in stats_events.clone() {
        if !subgraph_ids.contains(&operator) {
            raw_timings.push(stats.total);
        }

        operator_stats.insert(operator, stats);
    }

    if let Some(report_file) = report_file.as_mut() {
        // TODO: Sort within timely
        let mut operators_by_total_runtime =
            CrossbeamExtractor::new(agg_stats_receiver).extract_all();
        operators_by_total_runtime.sort_by_key(|(_operator, stats)| Reverse(stats.total));

        let mut table = Table::new();
        table.load_preset(UTF8_FULL);

        let headers = vec![
            "Name",
            "Id",
            "Address",
            "Total Runtime",
            "Activations",
            "Average Activation Time",
            "Max Activation Time",
            "Min Activation Time",
            "Max Arrangement Size",
            "Min Arrangement Size",
            "Arrangement Batches",
        ];
        table
            .set_constraints(
                headers
                    .iter()
                    .map(|header| ColumnConstraint::MinWidth(header.len() as u16)),
            )
            .set_header(headers);

        for (operator, stats, addr, name) in operators_by_total_runtime
            .iter()
            .filter_map(|&(operator, ref stats)| {
                addr_lookup
                    .get(&(stats.worker, operator))
                    .map(|addr| (operator, stats, addr))
            })
            .map(|(operator, stats, addr)| {
                let name: Option<&str> = name_lookup
                    .get(&(stats.worker, operator))
                    .map(|name| &**name);

                (operator, stats, addr, name.unwrap_or("N/A"))
            })
        {
            let arrange = stats.arrangement_size.as_ref().map(|arrange| {
                (
                    format!("{}", arrange.max_size),
                    format!("{}", arrange.min_size),
                    format!("{}", arrange.batches),
                )
            });

            let (max_arrange, min_arrange, arrange_batches) = arrange
                .as_ref()
                .map(|(max, min, batches)| (&**max, &**min, &**batches))
                .unwrap_or(("", "", ""));

            table.add_row(vec![
                Cell::new(name),
                Cell::new(operator),
                Cell::new(addr),
                Cell::new(format!("{:#?}", stats.total)),
                Cell::new(stats.activations),
                Cell::new(format!("{:#?}", stats.average)),
                Cell::new(format!("{:#?}", stats.max)),
                Cell::new(format!("{:#?}", stats.min)),
                Cell::new(max_arrange),
                Cell::new(min_arrange),
                Cell::new(arrange_batches),
            ]);
        }

        writeln!(
            report_file,
            "Operators Ranked by Total Runtime\n{}\n",
            table,
        )
        .context("failed to write to report file")?;

        let mut operators_by_arrangement_size: Vec<_> = operators_by_total_runtime
            .into_iter()
            .filter_map(|(operator, stats)| {
                stats
                    .arrangement_size
                    .map(|arrange| (operator, stats, arrange))
            })
            .collect();
        operators_by_arrangement_size
            .sort_unstable_by_key(|(_, _, arrange)| Reverse(arrange.max_size));

        let mut table = Table::new();
        table
            .load_preset(UTF8_FULL)
            .set_content_arrangement(ContentArrangement::Dynamic)
            .set_header(vec![
                "Name",
                "Id",
                "Address",
                "Total Runtime",
                "Max Arrangement Size",
                "Min Arrangement Size",
                "Arrangement Batches",
            ]);

        for (operator, stats, arrange, addr, name) in operators_by_arrangement_size
            .iter()
            .filter_map(|&(operator, ref stats, arrange)| {
                addr_lookup
                    .get(&(stats.worker, operator))
                    .map(|addr| (operator, stats, arrange, addr))
            })
            .map(|(operator, stats, arrange, addr)| {
                let name: Option<&str> = name_lookup
                    .get(&(stats.worker, operator))
                    .map(|name| &**name);

                (operator, stats, arrange, addr, name.unwrap_or("N/A"))
            })
        {
            table.add_row(vec![
                Cell::new(name),
                Cell::new(operator),
                Cell::new(addr),
                Cell::new(format!("{:#?}", stats.total)),
                Cell::new(arrange.max_size),
                Cell::new(arrange.min_size),
                Cell::new(arrange.batches),
            ]);
        }

        writeln!(
            report_file,
            "Operators Ranked by Arrangement Size\n{}\n",
            table,
        )
        .context("failed to write to report file")?;
    }

    let mut edge_events = CrossbeamExtractor::new(edge_receiver).extract_all();
    edge_events.sort_unstable_by_key(|(worker, _, channel, _)| (*worker, channel.channel_id()));
    tracing::info!("finished extracting {} edge events", edge_events.len());

    let (max_time, min_time) = (
        raw_timings.iter().max().copied().unwrap_or_default(),
        raw_timings.iter().min().copied().unwrap_or_default(),
    );

    let mut timeline_events = HashMap::new();
    for (_time, data) in CrossbeamExtractor::new(timeline_receiver).extract() {
        for (event, start_time, diff) in data {
            timeline_events.entry(event).or_insert((start_time, 0)).1 += diff;
        }
    }

    let mut timeline_events: Vec<_> = timeline_events
        .into_iter()
        .filter_map(|(event, (start_time, diff))| {
            if diff >= 1 {
                Some(WorkerTimelineEvent {
                    start_time: start_time.as_nanos() as u64,
                    ..event
                })
            } else {
                None
            }
        })
        .collect();

    timeline_events.sort_unstable_by_key(|event| (event.worker, event.start_time));
    tracing::info!(
        "finished extracting {} timeline events",
        timeline_events.len(),
    );

    let html_nodes: Vec<_> = node_events
        .into_iter()
        .filter_map(|((worker, addr), RkyvOperatesEvent { id, name, .. })| {
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
        .filter_map(|((worker, addr), RkyvOperatesEvent { id, name, .. })| {
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
                RkyvOperatesEvent { addr: src, .. },
                channel,
                RkyvOperatesEvent { addr: dest, .. },
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
        "Wrote output graph to file://{}",
        fs::canonicalize(&args.output_dir)
            .context("failed to get path of output dir")?
            .join("graph.html")
            .display(),
    );

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
