mod args;
mod colormap;
mod dataflow;
mod logging;
mod replay_loading;
mod report;
mod ui;
mod vega;

use crate::{
    args::Args,
    colormap::{select_color, Color},
    dataflow::{constants::DDSHOW_VERSION, Channel, DataflowData, DataflowSenders, OperatorStats},
    replay_loading::{connect_to_sources, wait_for_input},
    ui::{ActivationDuration, DDShowStats, EdgeKind, Lifespan, TimelineEvent},
};
use anyhow::{Context, Result};
use ddshow_types::{timely_logging::OperatesEvent, OperatorAddr, OperatorId, WorkerId};
use indicatif::{MultiProgress, ProgressDrawTarget};
use std::{
    collections::HashMap,
    fs::{self, File},
    io::{self, BufWriter},
    ops::Deref,
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use structopt::StructOpt;

// TODO: Library-ify a lot of this
// FIXME: Clean this up so much
// TODO: Set the panic hook to shut down the computation
//       so that panics don't stick things
fn main() -> Result<()> {
    // Grab the args from the user and build the required configs
    let args = Arc::new(Args::from_args());
    logging::init_logging(args.color);

    tracing::trace!("initialized and received cli args: {:?}", args);

    if let Some(shell) = args.completions {
        tracing::trace!("generating completions for {}", shell);
        Args::clap().gen_completions_to(env!("CARGO_PKG_NAME"), shell, &mut io::stdout());

        return Ok(());
    }

    let (communication_config, worker_config) = args.timely_config();

    let (
        timely_event_receivers,
        differential_event_receivers,
        progress_event_receivers,
        _total_sources,
    ) = if let Some(sources) = connect_to_sources(&args)? {
        sources
    } else {
        return Ok(());
    };

    let (running, workers_finished, progress_bars) = (
        Arc::new(AtomicBool::new(true)),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(MultiProgress::new()),
    );
    progress_bars.set_draw_target(if args.is_quiet() {
        ProgressDrawTarget::hidden()
    } else {
        ProgressDrawTarget::stdout()
    });

    let (replay_shutdown, moved_args, moved_workers_finished) =
        (running.clone(), args.clone(), workers_finished.clone());

    let ctrlc_running = running.clone();
    ctrlc::set_handler(move || {
        ctrlc_running.store(false, Ordering::Release);
        tracing::info!("received ctrl+c signal, shutting down");
    })
    .context("failed to set ctrl+c handler")?;

    // Create the *many* channels used for extracting data from the dataflow
    let (senders, receivers) = DataflowSenders::create();

    tracing::info!("starting compute dataflow");

    // Build the timely allocators and loggers
    let (builders, others) = communication_config
        .try_build()
        .map_err(|err| anyhow::anyhow!("failed to build timely communication config: {}", err))?;

    // Spin up the timely computation
    // Note: We use `execute_from()` instead of `timely::execute()` because
    //       `execute()` automatically sets log hooks that connect to
    //       `TIMELY_WORKER_LOG_ADDR`, meaning that no matter what we do
    //       our dataflow will always attempt to connect to that address
    //       if it's present in the env, causing things like ddshow/#7.
    //       See https://github.com/Kixiron/ddshow/issues/7
    let worker_guards =
        timely::execute::execute_from(builders, others, worker_config, move |worker| {
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

            let progress_traces = progress_event_receivers.as_ref().map(|recv| {
                recv[worker.index()]
                    .recv()
                    .expect("failed to receive progress traces")
            });

            // Start the analysis worker's runtime
            dataflow::worker_runtime(
                worker,
                moved_args.clone(),
                senders.clone(),
                replay_shutdown.clone(),
                moved_workers_finished.clone(),
                progress_bars.clone(),
                timely_traces,
                differential_traces,
                progress_traces,
            )
        })
        .map_err(|err| anyhow::anyhow!("failed to start up timely computation: {}", err))?;

    // Wait for the user's prompt
    let mut data = wait_for_input(&args, &running, &workers_finished, worker_guards, receivers)?;

    let name_lookup: HashMap<_, _> = data
        .name_lookup
        .iter()
        .map(|(id, name)| (*id, name.deref()))
        .collect();
    let addr_lookup: HashMap<_, _> = data
        .addr_lookup
        .iter()
        .map(|(id, addr)| (*id, addr))
        .collect();

    // Build & emit the textual report
    report::build_report(&*args, &data, &name_lookup, &addr_lookup)?;

    vega::make_data(&args, &data)?;

    if let Some(file) = args.dump_json.as_ref() {
        dump_program_json(&*args, file, &data, &name_lookup, &addr_lookup)?;
    }

    // Extract the data from timely
    let mut subgraph_ids = Vec::new();

    data.nodes
        .sort_unstable_by(|(addr1, _), (addr2, _)| addr1.cmp(addr2));
    tracing::debug!("finished extracting {} node events", data.nodes.len());

    data.subgraphs
        .sort_unstable_by(|(addr1, _), (addr2, _)| addr1.cmp(addr2));
    tracing::debug!(
        "finished extracting {} subgraph events",
        data.subgraphs.len(),
    );

    for &((worker, _), ref event) in data.subgraphs.iter() {
        subgraph_ids.push((worker, event.id));
    }

    let (mut operator_stats, mut raw_timings) = (HashMap::new(), Vec::new());
    tracing::debug!(
        "finished extracting {} stats events",
        data.operator_stats.len(),
    );

    for (operator, stats) in data.operator_stats.iter() {
        if !subgraph_ids.contains(operator) {
            raw_timings.push(stats.total);
        }

        operator_stats.insert(*operator, stats);
    }

    data.edges
        .sort_unstable_by_key(|(worker, _, channel, _)| (*worker, channel.channel_id()));
    tracing::debug!("finished extracting {} edge events", data.edges.len());

    let (max_time, min_time) = (
        raw_timings.iter().max().copied().unwrap_or_default(),
        raw_timings.iter().min().copied().unwrap_or_default(),
    );

    tracing::debug!(
        "finished extracting {} timeline events",
        data.timeline_events.len(),
    );

    let html_nodes: Vec<_> = data
        .nodes
        .iter()
        .filter_map(
            |&((worker, ref addr), OperatesEvent { id, ref name, .. })| {
                let &OperatorStats {
                    max,
                    min,
                    average,
                    total,
                    activations: invocations,
                    ref activation_durations,
                    ref arrangement_size,
                    ..
                } = *operator_stats.get(&(worker, id))?;

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
                    // TODO: Teach JS to deal with durations so we don't have to allocate
                    //       so much garbage
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
            },
        )
        .collect();

    let html_subgraphs: Vec<_> = data
        .subgraphs
        .iter()
        .filter_map(
            |&((worker, ref addr), OperatesEvent { id, ref name, .. })| {
                let OperatorStats {
                    max,
                    min,
                    average,
                    total,
                    activations: invocations,
                    ..
                } = **operator_stats.get(&(worker, id))?;

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
            },
        )
        .collect();

    let html_edges: Vec<_> = data
        .edges
        .iter()
        .map(|&(worker, _, ref channel, _)| ui::Edge {
            src: channel.source_addr(),
            dest: channel.target_addr(),
            worker,
            channel_id: channel.channel_id(),
            edge_kind: match channel {
                Channel::Normal { .. } => EdgeKind::Normal,
                Channel::ScopeCrossing { .. } => EdgeKind::Crossing,
            },
        })
        .collect();

    let mut palette_colors = Vec::with_capacity(10);
    let mut pos = 0.0;
    for _ in 0..10 {
        palette_colors.push(format!("{}", Color::new(args.palette.eval_continuous(pos))));
        pos += 0.1;
    }

    ui::render(
        &args,
        &data,
        &html_nodes,
        &html_subgraphs,
        &html_edges,
        &palette_colors,
    )?;

    if !args.no_report_file {
        let mut report_file = args.report_file.display().to_string();
        if cfg!(windows) && report_file.starts_with(r"\\?\") {
            report_file.replace_range(..r"\\?\".len(), "");
        }

        if args.isnt_quiet() {
            println!("Wrote report file to {}", report_file);
        }
    }

    let mut graph_file = fs::canonicalize(&args.output_dir)
        .context("failed to get path of output dir")?
        .join("graph.html")
        .display()
        .to_string();
    if cfg!(windows) && graph_file.starts_with(r"\\?\") {
        graph_file.replace_range(..r"\\?\".len(), "");
        graph_file = graph_file.replace("\\", "/");
    }

    if args.isnt_quiet() {
        println!("Wrote output graph to file:///{}", graph_file);
    }

    Ok(())
}

fn dump_program_json(
    args: &Args,
    file: &Path,
    data: &DataflowData,
    _name_lookup: &HashMap<(WorkerId, OperatorId), &str>,
    _addr_lookup: &HashMap<(WorkerId, OperatorId), &OperatorAddr>,
) -> Result<()> {
    let file = BufWriter::new(File::create(file).context("failed to create json file")?);

    let workers: Vec<_> = data.worker_stats[0]
        .iter()
        .map(|(_, stats)| stats)
        .collect();
    let events: Vec<_> = data
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
        program: data.program_stats[0].clone(),
        workers: &workers,
        dataflows: &data.dataflow_stats,
        // FIXME: Do these
        nodes: &[],
        channels: &[],
        arrangements: &[],
        events: &events,
        differential_enabled: args.differential_enabled,
        progress_enabled: false, // args.progress_enabled,
        ddshow_version: DDSHOW_VERSION,
    };

    serde_json::to_writer(file, &data).context("failed to write json to file")?;

    Ok(())
}
