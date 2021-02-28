mod args;
mod colormap;
mod dataflow;
mod dot;
mod network;
mod ui;

use anyhow::{Context, Result};
use args::Args;
use colormap::select_color;
use dataflow::make_streams;
use dataflow::{CrossbeamExtractor, DataflowSenders, OperatorStats};
use dot::Graph;
use network::{wait_for_connections, wait_for_input};
use sequence_trie::SequenceTrie;
use std::{
    collections::HashMap,
    fs::{self, File},
    io::{self, Write},
    sync::{atomic::AtomicBool, Arc},
    thread,
    time::Instant,
};
use structopt::StructOpt;
use timely::{communication::Config as ParallelConfig, execute::Config, logging::OperatesEvent};
use tracing_subscriber::{
    fmt::time::Uptime, prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt,
    EnvFilter,
};

fn main() -> Result<()> {
    let filter_layer = EnvFilter::from_env("TIMELY_VIZ_LOG");
    let fmt_layer = tracing_subscriber::fmt::layer()
        .pretty()
        .with_timer(Uptime::default())
        .with_thread_names(true)
        .with_ansi(true);

    tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt_layer)
        .init();

    // Grab the args from the user and build the required configs
    let args = Arc::new(Args::from_args());
    let config = {
        let communication = if args.timely_connections.get() == 1 {
            ParallelConfig::Thread
        } else {
            ParallelConfig::Process(args.timely_connections.get())
        };

        Config {
            communication,
            worker: Default::default(),
        }
    };

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

    let timely_connections = wait_for_connections(args.address, args.timely_connections)?;
    let event_receivers = make_streams(args.timely_connections.get(), timely_connections)?;

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

    let running = Arc::new(AtomicBool::new(true));
    let (replay_shutdown, moved_args) = (running.clone(), args.clone());

    let (node_sender, node_receiver) = crossbeam_channel::unbounded();
    let (edge_sender, edge_receiver) = crossbeam_channel::unbounded();
    let (subgraph_sender, subgraph_receiver) = crossbeam_channel::unbounded();
    let (stats_sender, stats_receiver) = crossbeam_channel::unbounded();

    // Spin up the timely computation
    let worker_guards = timely::execute(config, move |worker| {
        let dataflow_name = concat!(env!("CARGO_CRATE_NAME"), " log processor");
        let args = moved_args.clone();

        // Distribute the tcp streams across workers, converting each of them into an event reader
        let timely_traces = event_receivers[worker.index()]
            .recv()
            .expect("failed to receive event traces");

        let senders = DataflowSenders {
            node_sender: node_sender.clone(),
            edge_sender: edge_sender.clone(),
            subgraph_sender: subgraph_sender.clone(),
            stats_sender: stats_sender.clone(),
        };

        worker.dataflow_named(dataflow_name, |scope| {
            dataflow::dataflow(
                scope,
                &*args,
                timely_traces,
                replay_shutdown.clone(),
                senders,
            )
        })
    })
    .map_err(|err| anyhow::anyhow!("failed to start up timely computation: {}", err))?;

    // Wait for the user's prompt
    wait_for_input(&*running, worker_guards)?;

    // Extract the data from timely
    let mut graph_nodes = SequenceTrie::new();
    let mut operator_addresses = HashMap::new();
    let mut operator_names = HashMap::new();
    let mut subgraph_ids = Vec::new();

    let node_events = CrossbeamExtractor::new(node_receiver).extract_all();
    tracing::info!(
        "finished extracting node events for a total of {}",
        node_events.len(),
    );

    for (addr, event) in node_events.clone() {
        let id = event.id;

        operator_names.insert(id, event.name.clone());
        graph_nodes.insert(&addr, Graph::Node(event));
        operator_addresses.insert(id, addr);
    }

    let subgraph_events = CrossbeamExtractor::new(subgraph_receiver).extract_all();
    tracing::info!(
        "finished extracting subgraph events for a total of {}",
        subgraph_events.len(),
    );

    for (addr, event) in subgraph_events.clone() {
        let id = event.id;

        operator_names.insert(id, event.name.clone());
        graph_nodes.insert(&addr, Graph::Subgraph(event));
        operator_addresses.insert(id, addr);
        subgraph_ids.push(id);
    }

    let (mut operator_stats, mut raw_timings) = (HashMap::new(), Vec::new());
    let stats_events = CrossbeamExtractor::new(stats_receiver).extract_all();
    tracing::info!(
        "finished extracting stats events for a total of {}",
        stats_events.len(),
    );

    for (operator, stats) in stats_events.clone() {
        operator_stats.insert(operator, stats);

        if !subgraph_ids.contains(&operator) {
            raw_timings.push(stats.total);
        }
    }

    let edge_events = CrossbeamExtractor::new(edge_receiver).extract_all();
    tracing::info!(
        "finished extracting edge events for a total of {}",
        edge_events.len(),
    );

    let (max_time, min_time) = (
        raw_timings.iter().max().copied().unwrap_or_default(),
        raw_timings.iter().min().copied().unwrap_or_default(),
    );

    let html_nodes: Vec<_> = node_events
        .into_iter()
        .map(|(addr, OperatesEvent { id, name, .. })| {
            let OperatorStats {
                max,
                min,
                average,
                total,
                invocations,
                ..
            } = operator_stats[&id];

            let fill_color = select_color(&args.palette, total, (max_time, min_time));
            let text_color = fill_color.text_color();

            ui::Node {
                id,
                addr,
                name,
                max_activation_time: format!("{:#?}", max),
                mix_activation_time: format!("{:#?}", min),
                average_activation_time: format!("{:#?}", average),
                total_activation_time: format!("{:#?}", total),
                invocations,
                fill_color: format!("{}", fill_color),
                text_color: format!("{}", text_color),
            }
        })
        .collect();

    let html_subgraphs: Vec<_> = subgraph_events
        .into_iter()
        .map(|(addr, OperatesEvent { id, name, .. })| {
            let OperatorStats {
                max,
                min,
                average,
                total,
                invocations,
                ..
            } = operator_stats[&id];

            let fill_color = select_color(&args.palette, total, (max, min));
            let text_color = fill_color.text_color();

            ui::Subgraph {
                id,
                addr,
                name,
                max_activation_time: format!("{:#?}", max),
                mix_activation_time: format!("{:#?}", min),
                average_activation_time: format!("{:#?}", average),
                total_activation_time: format!("{:#?}", total),
                invocations,
                fill_color: format!("{}", fill_color),
                text_color: format!("{}", text_color),
            }
        })
        .collect();

    let html_edges: Vec<_> = edge_events
        .clone()
        .into_iter()
        //.inspect(|x| println!("Output channel: {:?}", x))
        .map(
            |(OperatesEvent { addr: src, .. }, channel, OperatesEvent { addr: dest, .. })| {
                ui::Edge {
                    src,
                    dest,
                    channel_id: channel.channel_id(),
                }
            },
        )
        .collect();

    ui::render(&args, html_nodes, html_subgraphs, html_edges)?;

    let (thread_stats, thread_graph, thread_args) =
        (operator_stats.clone(), graph_nodes.clone(), args.clone());
    let file_thread = thread::spawn(move || -> Result<()> {
        let mut dot_graph =
            File::create(&thread_args.dot_file).context("failed to open out file")?;

        writeln!(
            &mut dot_graph,
            "digraph {{\n    \
                graph [compound = true];",
        )?;

        dot::render_graph(
            &mut dot_graph,
            &*thread_args,
            &thread_stats,
            &thread_graph,
            (max_time, min_time),
            1,
        )?;

        for (source, _channel, target) in edge_events {
            let mut edge_attrs = "[".to_owned();

            if source.name == "Feedback" {
                edge_attrs.push_str("constraint = false");
            }

            // if let Some((message, capability, total_events)) = stats {
            //     write!(
            //         &mut edge_attrs,
            //         "label = \" {} messages and {} capability&#10;updates over {} events\", labeltooltip = \"Messages: {} max, \
            //             {} min, {:.2} average&#10;Capabilities: {} max, {} min, {:.2} average\", ",
            //         message.total, capability.total, total_events, message.max,
            //         message.min, f32::from_bits(message.average), capability.max,
            //         capability.min, f32::from_bits(capability.average),
            //     )?;
            // }

            writeln!(
                &mut dot_graph,
                "    {} -> {}{}];",
                dot::node_id(&source)?,
                dot::node_id(&target)?,
                edge_attrs.trim(),
            )?;
        }

        writeln!(&mut dot_graph, "}}")?;

        println!(
            "Wrote the output graph to {}",
            fs::canonicalize(&thread_args.dot_file)
                .context("failed to get path of dot file")?
                .display(),
        );

        Ok(())
    });

    // TODO: Process more data with timely
    if !args.no_summary {
        let mut stats: Vec<_> = operator_stats.values().collect();

        // TODO: Filter regions out
        stats.sort_unstable_by_key(|stat| stat.total);
        let highest_total_times: Vec<_> = stats
            .iter()
            .rev()
            .filter(|stats| !subgraph_ids.contains(&stats.id))
            .take(args.top_displayed)
            .map(|&stat| {
                (
                    stat.id,
                    stat,
                    containing_regions(&stat.id, &operator_addresses, &graph_nodes),
                )
            })
            .collect();

        stats.sort_unstable_by_key(|stat| stat.average);
        let highest_average_times: Vec<_> = stats
            .iter()
            .rev()
            .filter(|stats| !subgraph_ids.contains(&stats.id))
            .take(args.top_displayed)
            .map(|&stat| {
                (
                    stat.id,
                    stat,
                    containing_regions(&stat.id, &operator_addresses, &graph_nodes),
                )
            })
            .collect();

        stats.sort_unstable_by_key(|stat| stat.invocations);
        let highest_invocations: Vec<_> = stats
            .iter()
            .rev()
            .filter(|stats| !subgraph_ids.contains(&stats.id))
            .take(args.top_displayed)
            .map(|&stat| {
                (
                    stat.id,
                    stat,
                    containing_regions(&stat.id, &operator_addresses, &graph_nodes),
                )
            })
            .collect();

        let mut stdout = io::stdout();

        writeln!(
            &mut stdout,
            "Top {} highest total operator runtimes",
            highest_total_times.len(),
        )?;

        for (id, stats, containing_regions) in highest_total_times {
            let num_regions = containing_regions.len();
            for (i, region_name) in containing_regions.into_iter().enumerate() {
                writeln!(&mut stdout, "{}{}", " ".repeat(i * 2), region_name)?;
            }

            writeln!(
                &mut stdout,
                "{}{} with a total time of {:#?}",
                " ".repeat(num_regions * 2),
                operator_names.get(&id).unwrap(),
                stats.total,
            )?;
        }
    }

    match file_thread.join() {
        Ok(res) => res?,
        Err(err) => {
            if let Some(panic_msg) = err.downcast_ref::<&str>() {
                anyhow::bail!(
                    "failed to write to file '{}': {}",
                    args.dot_file.display(),
                    panic_msg,
                );
            } else {
                anyhow::bail!("failed to write to file '{}'", args.dot_file.display());
            }
        }
    }

    if args.render {
        println!("Running dot...");

        let out_file = dot::render_graphviz_svg(&args)?;

        println!(
            "Generated a svg at {}",
            fs::canonicalize(out_file)
                .context("failed to get path of out file")?
                .display(),
        );
    }

    Ok(())
}

fn containing_regions<'a>(
    id: &usize,
    operator_addresses: &'a HashMap<usize, Vec<usize>>,
    graph_nodes: &'a SequenceTrie<usize, Graph>,
) -> Vec<&'a str> {
    operator_addresses
        .get(id)
        .map(|addr| {
            graph_nodes
                .get_prefix_nodes(addr)
                .into_iter()
                .flat_map(|trie| trie.value())
                .filter_map(|graph| graph.as_subgraph())
                .map(|subgraph| subgraph.name.as_str())
                .collect()
        })
        .unwrap_or_default()
}
