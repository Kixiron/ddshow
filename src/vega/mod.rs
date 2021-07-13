use crate::{args::Args, dataflow::DataflowData};
use anyhow::{Context as _, Result};
use ddshow_types::{OperatorAddr, OperatorId, WorkerId};
use serde::Serialize;
use std::{
    fs::{self, File},
    io::BufWriter,
};
use tera::{Context, Tera};

const DASHBOARD: &str = include_str!("dashboard.html");

// These types reference as much data as possible to try and preserve memory
#[derive(Debug, Serialize)]
struct VegaNode<'a> {
    id: OperatorId,
    name: &'a str,
    addr: &'a OperatorAddr,
    activations: usize,
    total_runtime: u64,
    average_activation_time: u64,
    max_activation_time: u64,
    min_activation_time: u64,
    activation_durations: Vec<(u64, u64)>,
    max_arrangement_size: Option<usize>,
    min_arrangement_size: Option<usize>,
    arrangement_batches: Option<usize>,
    node_kind: NodeKind,
    per_worker: Vec<(WorkerId, VegaWorkerNode)>,
}

#[derive(Debug, Serialize)]
struct VegaWorkerNode {
    activations: usize,
    total_runtime: u64,
    average_activation_time: u64,
    max_activation_time: u64,
    min_activation_time: u64,
    activation_durations: Vec<(u64, u64)>,
    max_arrangement_size: Option<usize>,
    min_arrangement_size: Option<usize>,
    arrangement_batches: Option<usize>,
    spline_levels: Option<Vec<(u64, usize, usize)>>,
}

#[derive(Debug, Serialize)]
enum NodeKind {
    Node,
    Subgraph,
}

pub fn make_data(args: &Args, data: &DataflowData) -> Result<()> {
    let nodes: Vec<_> = data
        .aggregated_operator_stats
        .iter()
        .filter_map(|&(id, ref stats)| {
            let name = &data
                .name_lookup
                .iter()
                .find(|&&((_, op_id), _)| id == op_id)?
                .1;
            let addr = &data
                .addr_lookup
                .iter()
                .find(|&&((_, op_id), _)| id == op_id)?
                .1;
            let activation_durations = stats
                .activation_durations
                .iter()
                .map(|(start, duration)| (start.as_nanos() as u64, duration.as_nanos() as u64))
                .collect();
            let node_kind = if data
                .subgraphs
                .iter()
                .any(|((_, op_addr), _)| op_addr == addr)
            {
                NodeKind::Subgraph
            } else {
                NodeKind::Node
            };
            let per_worker = data
                .operator_stats
                .iter()
                .filter(|&&((_, op_id), _)| op_id == id)
                .map(|&((worker, _), ref stats)| {
                    let activation_durations = stats
                        .activation_durations
                        .iter()
                        .map(|(start, duration)| {
                            (start.as_nanos() as u64, duration.as_nanos() as u64)
                        })
                        .collect();

                    let stats = VegaWorkerNode {
                        activations: stats.activations,
                        total_runtime: stats.total.as_nanos() as u64,
                        average_activation_time: stats.average.as_nanos() as u64,
                        max_activation_time: stats.max.as_nanos() as u64,
                        min_activation_time: stats.min.as_nanos() as u64,
                        activation_durations,
                        max_arrangement_size: stats
                            .arrangement_size
                            .as_ref()
                            .map(|arr| arr.max_size),
                        min_arrangement_size: stats
                            .arrangement_size
                            .as_ref()
                            .map(|arr| arr.min_size),
                        arrangement_batches: stats.arrangement_size.as_ref().map(|arr| arr.batches),
                        spline_levels: stats.arrangement_size.as_ref().map(|arr| {
                            arr.spline_levels
                                .iter()
                                .map(|&(time, complete, scale)| {
                                    (time.as_nanos() as u64, complete, scale)
                                })
                                .collect()
                        }),
                    };

                    (worker, stats)
                })
                .collect();

            Some(VegaNode {
                id,
                name,
                addr,
                activations: stats.activations,
                total_runtime: stats.total.as_nanos() as u64,
                average_activation_time: stats.average.as_nanos() as u64,
                max_activation_time: stats.max.as_nanos() as u64,
                min_activation_time: stats.min.as_nanos() as u64,
                activation_durations,
                max_arrangement_size: stats.arrangement_size.as_ref().map(|arr| arr.max_size),
                min_arrangement_size: stats.arrangement_size.as_ref().map(|arr| arr.min_size),
                arrangement_batches: stats.arrangement_size.as_ref().map(|arr| arr.batches),
                node_kind,
                per_worker,
            })
        })
        .collect();

    if let Err(err) = fs::create_dir_all(&args.output_dir) {
        tracing::warn!(
            output_dir = ?args.output_dir,
            "failed to create directory for dashboard file: {:?}",
            err,
        );
    }

    let file = BufWriter::new(
        File::create(args.output_dir.join("dashboard.html")).with_context(|| {
            anyhow::format_err!(
                "failed to create dashboard file at '{}'",
                args.output_dir.join("dashboard.html").display(),
            )
        })?,
    );

    let mut context = Context::new();
    context.insert("aggregated_stats", &nodes);

    let mut tera = Tera::default();
    tera.add_raw_template("dashboard", DASHBOARD)
        .context("internal error: failed to add dashboard template to tera")?;

    tera.render_to("dashboard", &context, file)
        .with_context(|| {
            anyhow::format_err!(
                "failed to render dashboard to {}",
                args.output_dir.join("dashboard.html").display(),
            )
        })?;

    Ok(())
}
