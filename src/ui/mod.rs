use crate::{
    args::Args,
    dataflow::{
        utils::OpKey, ArrangementStats as DataflowArrangementStats, DataflowData, OperatorProgress,
        OperatorShape, SplineLevel, Summation, TimelineEvent as RawTimelineEvent,
    },
};
use abomonation_derive::Abomonation;
use anyhow::{Context as _, Result};
use bytecheck::CheckBytes;
use ddshow_types::{ChannelId, OperatorAddr, OperatorId, PortId, WorkerId};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs::{self, File},
    io::BufWriter,
    time::Duration,
};
use tera::{Context, Tera};

const GRAPH_HTML: &str = include_str!("graph.html");
const GRAPH_CSS: &str = include_str!("graph.css");
const GRAPH_JS: &str = include_str!("graph.js");
const D3_JS: &str = include_str!("d3.v5.js");
const DAGRE_JS: &str = include_str!("dagre-d3.js");

#[allow(clippy::too_many_arguments)]
pub fn render(
    args: &Args,
    data: &DataflowData,
    nodes: &[Node],
    subgraphs: &[Subgraph],
    edges: &[Edge],
    palette_colors: &[String],
    arrangement_map: &HashMap<OpKey, &DataflowArrangementStats>,
    activation_map: &HashMap<OpKey, Vec<(Duration, Duration)>>,
    agg_operator_stats: &HashMap<OperatorId, &Summation>,
    agg_arrangement_stats: &HashMap<OperatorId, &DataflowArrangementStats>,
    agg_activations: &HashMap<OperatorId, Vec<&Vec<(Duration, Duration)>>>,
    spline_levels: &HashMap<OpKey, Vec<SplineLevel>>,
) -> Result<()> {
    let output_dir = args.output_dir.canonicalize().with_context(|| {
        anyhow::anyhow!("failed to canonicalize '{}'", args.output_dir.display())
    })?;
    tracing::info!(output_dir = ?output_dir, "writing graph files to disk");

    fs::create_dir_all(&output_dir).context("failed to create output directory")?;

    fs::write(output_dir.join("d3.v5.js"), D3_JS)
        .context("failed to write output graph to file")?;
    fs::write(output_dir.join("dagre-d3.js"), DAGRE_JS)
        .context("failed to write output graph to file")?;

    let vega_data = vega_data(
        data,
        arrangement_map,
        activation_map,
        agg_operator_stats,
        agg_arrangement_stats,
        agg_activations,
        spline_levels,
    );
    let graph_data = GraphData {
        nodes,
        subgraphs,
        edges,
        palette_colors,
        timeline_events: &data.timeline_events,
        operator_shapes: &data.operator_shapes,
        operator_progress: &data.operator_progress,
        vega_data: &vega_data,
    };

    let mut context =
        Context::from_serialize(graph_data).context("failed to render graph context as json")?;

    let mut tera = Tera::default();
    tera.add_raw_template("graph_js", GRAPH_JS)
        .context("internal error: failed to add graph.js template to tera")?;
    tera.add_raw_template("graph_html", GRAPH_HTML)
        .context("internal error: failed to add graph.html template to tera")?;

    // Render the javascript file & write it to disk
    let js_file = File::create(args.output_dir.join("graph.js")).with_context(|| {
        anyhow::format_err!(
            "failed to create graph.js file at '{}'",
            args.output_dir.join("graph.js").display(),
        )
    })?;
    tera.render_to("graph_js", &context, BufWriter::new(js_file))
        .with_context(|| {
            anyhow::format_err!(
                "failed to render graph.js to {}",
                args.output_dir.join("graph.js").display(),
            )
        })?;

    // Add the stylesheet into the tera context
    context.insert("stylesheet", GRAPH_CSS);

    // Render the html file & write it to disk
    let html_file = File::create(args.output_dir.join("graph.html")).with_context(|| {
        anyhow::format_err!(
            "failed to create graph.html file at '{}'",
            args.output_dir.join("graph.html").display(),
        )
    })?;
    tera.render_to("graph_html", &context, BufWriter::new(html_file))
        .with_context(|| {
            anyhow::format_err!(
                "failed to render graph.html to {}",
                args.output_dir.join("graph.html").display(),
            )
        })?;

    Ok(())
}

// These types reference as much data as possible to try and preserve memory
#[derive(Debug, Serialize)]
pub struct VegaNode<'a> {
    pub id: OperatorId,
    pub name: &'a str,
    pub addr: &'a OperatorAddr,
    pub activations: usize,
    pub total_runtime: u64,
    pub average_activation_time: u64,
    pub max_activation_time: u64,
    pub min_activation_time: u64,
    pub activation_durations: Vec<(u64, u64)>,
    pub max_arrangement_size: Option<usize>,
    pub min_arrangement_size: Option<usize>,
    pub arrangement_batches: Option<usize>,
    pub node_kind: VegaNodeKind,
    pub per_worker: Vec<(WorkerId, VegaWorkerNode)>,
}

#[derive(Debug, Serialize)]
pub struct VegaWorkerNode {
    pub activations: usize,
    pub total_runtime: u64,
    pub average_activation_time: u64,
    pub max_activation_time: u64,
    pub min_activation_time: u64,
    pub activation_durations: Vec<(u64, u64)>,
    pub max_arrangement_size: Option<usize>,
    pub min_arrangement_size: Option<usize>,
    pub arrangement_batches: Option<usize>,
    pub spline_levels: Option<Vec<(u64, usize, usize)>>,
}

#[derive(Debug, Serialize)]
pub enum VegaNodeKind {
    Node,
    Subgraph,
}

fn vega_data<'a>(
    data: &'a DataflowData,
    arrangement_map: &'a HashMap<OpKey, &'a DataflowArrangementStats>,
    activation_map: &'a HashMap<OpKey, Vec<(Duration, Duration)>>,
    agg_operator_stats: &'a HashMap<OperatorId, &'a Summation>,
    agg_arrangement_stats: &'a HashMap<OperatorId, &'a DataflowArrangementStats>,
    agg_activations: &'a HashMap<OperatorId, Vec<&'a Vec<(Duration, Duration)>>>,
    spline_levels: &'a HashMap<OpKey, Vec<SplineLevel>>,
) -> Vec<VegaNode<'a>> {
    agg_operator_stats
        .iter()
        .filter_map(|(&id, &stats)| {
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

            let activation_durations = agg_activations
                .get(&id)
                .map(|activations| {
                    activations
                        .iter()
                        .flat_map(|activations| {
                            activations.iter().map(|(start, duration)| {
                                (start.as_nanos() as u64, duration.as_nanos() as u64)
                            })
                        })
                        .collect()
                })
                .unwrap_or_default();

            let arranged = agg_arrangement_stats.get(&id);

            let node_kind = if data
                .subgraphs
                .iter()
                .any(|((_, op_addr), _)| op_addr == addr)
            {
                VegaNodeKind::Subgraph
            } else {
                VegaNodeKind::Node
            };

            let per_worker = data
                .summarized
                .iter()
                .filter(|&&((_, op_id), _)| op_id == id)
                .map(|&((worker, _), ref stats)| {
                    let activation_durations = activation_map
                        .get(&(worker, id))
                        .map(|activations| {
                            activations
                                .iter()
                                .map(|(start, duration)| {
                                    (start.as_nanos() as u64, duration.as_nanos() as u64)
                                })
                                .collect()
                        })
                        .unwrap_or_default();

                    let arranged = arrangement_map.get(&(worker, id));

                    let spline_levels = spline_levels.get(&(worker, id)).map(|spline_levels| {
                        spline_levels
                            .iter()
                            .map(
                                |&SplineLevel {
                                     event_time,
                                     complete_size,
                                     scale,
                                 }| {
                                    (event_time.as_nanos() as u64, complete_size, scale)
                                },
                            )
                            .collect()
                    });

                    let stats = VegaWorkerNode {
                        activations: stats.count,
                        total_runtime: stats.total.as_nanos() as u64,
                        average_activation_time: stats.average.as_nanos() as u64,
                        max_activation_time: stats.max.as_nanos() as u64,
                        min_activation_time: stats.min.as_nanos() as u64,
                        activation_durations,
                        max_arrangement_size: arranged.map(|arr| arr.max_size),
                        min_arrangement_size: arranged.map(|arr| arr.min_size),
                        arrangement_batches: arranged.map(|arr| arr.batches),
                        spline_levels,
                    };

                    (worker, stats)
                })
                .collect();

            Some(VegaNode {
                id,
                name,
                addr,
                activations: stats.count,
                total_runtime: stats.total.as_nanos() as u64,
                average_activation_time: stats.average.as_nanos() as u64,
                max_activation_time: stats.max.as_nanos() as u64,
                min_activation_time: stats.min.as_nanos() as u64,
                activation_durations,
                max_arrangement_size: arranged.map(|arr| arr.max_size),
                min_arrangement_size: arranged.map(|arr| arr.min_size),
                arrangement_batches: arranged.map(|arr| arr.batches),
                node_kind,
                per_worker,
            })
        })
        .collect()
}

// TODO: Move this to another crate, make serde & abomonation feature-gated,
//       add wasm-bindgen under a feature gate

//  - whether differential logging was enabled
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Archive, RkyvSerialize,
)]
#[allow(clippy::upper_case_acronyms)]
#[archive_attr(derive(CheckBytes))]
pub struct DDShowStats<'a> {
    pub program: ProgramStats,
    // TODO: Should/would these be better as trees?
    pub workers: &'a [&'a WorkerStats],
    pub dataflows: &'a [DataflowStats],
    pub nodes: &'a [NodeStats],
    pub channels: &'a [ChannelStats],
    pub arrangements: &'a [ArrangementStats],
    pub events: &'a [TimelineEvent],
    pub differential_enabled: bool,
    pub progress_enabled: bool,
    pub ddshow_version: &'a str,
    // TODO: Lists of nodes, channels & arrangement ids (or addresses?) sorted
    //       by various metrics, e.g. runtime, size, # merges
    // TODO: Progress logging
}

// - Program stats
//  - # workers
//  - # dataflows
//  - # nodes
//  - # operators
//  - # subgraphs
//  - # channels
//  - # arrangements
//  - # events
//  - # missing nodes
//  - # missing edges
//  - total program runtime
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Default,
    Deserialize,
    Serialize,
    Abomonation,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[archive_attr(derive(CheckBytes))]
pub struct ProgramStats {
    pub workers: usize,
    pub dataflows: usize,
    pub operators: usize,
    pub subgraphs: usize,
    pub channels: usize,
    pub arrangements: usize,
    pub events: usize,
    pub runtime: Duration,
    // TODO: Missing nodes & edges
}

// - Worker stats
//   - total worker runtime
//  - # dataflows
//  - # nodes
//  - # operators
//  - # subgraphs
//  - # channels
//  - # events
//  - # arrangements
//  - # missing nodes
//  - # missing edges
//  - list of dataflow addresses
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Default,
    Deserialize,
    Serialize,
    Abomonation,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[archive_attr(derive(CheckBytes))]
pub struct WorkerStats {
    pub id: WorkerId,
    pub dataflows: usize,
    pub operators: usize,
    pub subgraphs: usize,
    pub channels: usize,
    pub arrangements: usize,
    pub events: usize,
    pub runtime: Duration,
    pub dataflow_addrs: Vec<OperatorAddr>,
    // TODO: Missing nodes & edges
}

// - Dataflow stats
//   - creation time
//   - drop time
//   - # of contained operators
//   - # of contained subgraphs
//   - # of contained channels
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Default,
    Deserialize,
    Serialize,
    Abomonation,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[archive_attr(derive(CheckBytes))]
pub struct DataflowStats {
    pub id: OperatorId,
    pub addr: OperatorAddr,
    pub worker: WorkerId,
    pub operators: usize,
    pub subgraphs: usize,
    pub channels: usize,
    pub lifespan: Lifespan,
    // TODO: Arrangements within the current dataflow
}

// - Nodes
//   - id
//   - worker
//   - address
//   - name
//   - inputs
//   - outputs
//   - whether it's a subgraph
//   - whether it's a root dataflow
//   - number of invocations
//   - max activation time
//   - min activation time
//   - average activation time
//   - all activation durations
//   - creation time
//   - drop time
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Default,
    Deserialize,
    Serialize,
    Abomonation,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[archive_attr(derive(CheckBytes))]
pub struct NodeStats {
    pub id: OperatorId,
    pub addr: OperatorAddr,
    pub worker: WorkerId,
    pub name: String,
    pub inputs: Vec<PortId>,
    pub outputs: Vec<PortId>,
    pub lifespan: Lifespan,
    pub kind: NodeKind,
    pub activations: AggregatedStats<Duration>,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Deserialize,
    Serialize,
    Abomonation,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[archive_attr(derive(CheckBytes))]
pub enum NodeKind {
    Operator,
    Subgraph,
    Dataflow,
}

impl Default for NodeKind {
    fn default() -> Self {
        Self::Operator
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Default,
    Deserialize,
    Serialize,
    Abomonation,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[archive_attr(derive(CheckBytes))]
pub struct Lifespan {
    pub birth: Duration,
    pub death: Duration,
}

impl Lifespan {
    pub const fn new(birth: Duration, death: Duration) -> Self {
        Self { birth, death }
    }

    pub fn duration(&self) -> Duration {
        self.death - self.birth
    }
}

// - Edges
//   - id
//   - worker
//   - address
//   - name
//   - edge kind
//   - edge id (is this even a real thing?)
//   - source node
//   - dest node
//   - creation time
//   - drop time
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Default,
    Deserialize,
    Serialize,
    Abomonation,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[archive_attr(derive(CheckBytes))]
pub struct ChannelStats {
    // TODO: Do these two actually even exist?
    pub id: ChannelId,
    // TODO: Make `ChannelAddr`
    pub addr: OperatorAddr,
    pub worker: WorkerId,
    pub source_node: OperatorId,
    pub dest_node: OperatorId,
    pub kind: ChannelKind,
    pub lifespan: Lifespan,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Deserialize,
    Serialize,
    Abomonation,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[archive_attr(derive(CheckBytes))]
pub enum ChannelKind {
    Ingress,
    Egress,
    Normal,
}

impl Default for ChannelKind {
    fn default() -> Self {
        Self::Normal
    }
}

// - Arrangement stats
//   - operator address
//   - max arrangement size
//   - min arrangement size
//   - average arrangement size
//   - all arrangement sizes
//   - number of merges
//   - merge timings
//   - number of batches received
//   - max/min/average batch sizes
//   - list of all batch sizes
//   - # of traces
//   - creation time
//   - drop time
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Default,
    Deserialize,
    Serialize,
    Abomonation,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[archive_attr(derive(CheckBytes))]
pub struct ArrangementStats {
    pub operator_addr: OperatorAddr,
    pub size_stats: AggregatedStats<usize>,
    pub merge_stats: AggregatedStats<Duration>,
    pub batch_stats: AggregatedStats<usize>,
    pub trace_shares: usize,
    pub lifespan: Lifespan,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Default,
    Deserialize,
    Serialize,
    Abomonation,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[archive_attr(derive(CheckBytes))]
pub struct AggregatedStats<T> {
    pub total: usize,
    pub max: T,
    pub min: T,
    pub average: T,
    pub data_points: Vec<T>,
    // TODO: Standard deviation, standard error
}

// - Timeline events
//   - event id (is this actually needed?)
//   - worker
//   - event
//   - when the event started
//   - when the event ended (unneeded?)
//   - event duration
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Default,
    Deserialize,
    Serialize,
    Abomonation,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[archive_attr(derive(CheckBytes))]
pub struct TimelineEvent {
    pub worker: WorkerId,
    // TODO: Events
    pub event: (),
    pub lifespan: Lifespan,
}

#[derive(Debug, Serialize)]
pub struct GraphData<'a> {
    pub nodes: &'a [Node<'a>],
    pub subgraphs: &'a [Subgraph<'a>],
    pub edges: &'a [Edge<'a>],
    pub palette_colors: &'a [String],
    pub timeline_events: &'a [RawTimelineEvent],
    pub operator_shapes: &'a [OperatorShape],
    pub operator_progress: &'a [OperatorProgress],
    pub vega_data: &'a [VegaNode<'a>],
}

#[derive(Debug, Serialize)]
pub struct Node<'a> {
    pub id: OperatorId,
    pub worker: WorkerId,
    pub addr: &'a OperatorAddr,
    pub name: &'a str,
    pub max_activation_time: String,
    pub min_activation_time: String,
    pub average_activation_time: String,
    pub total_activation_time: String,
    pub invocations: usize,
    pub fill_color: String,
    pub text_color: String,
    pub activation_durations: Vec<ActivationDuration>,
    pub max_arrangement_size: Option<usize>,
    pub min_arrangement_size: Option<usize>,
}

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Deserialize, Serialize)]
pub struct ActivationDuration {
    pub activation_time: u64,
    pub activated_at: u64,
}

#[derive(Debug, Serialize)]
pub struct Subgraph<'a> {
    pub id: OperatorId,
    pub worker: WorkerId,
    pub addr: &'a OperatorAddr,
    pub name: &'a str,
    pub max_activation_time: String,
    pub min_activation_time: String,
    pub average_activation_time: String,
    pub total_activation_time: String,
    pub invocations: usize,
    pub fill_color: String,
    pub text_color: String,
}

#[derive(Debug, Serialize)]
pub struct Edge<'a> {
    pub src: &'a OperatorAddr,
    pub dest: &'a OperatorAddr,
    pub worker: WorkerId,
    pub channel_id: ChannelId,
    pub edge_kind: EdgeKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
pub enum EdgeKind {
    Normal,
    Crossing,
}

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Deserialize, Serialize)]
pub struct ChannelMessageStats {
    pub channel: ChannelId,
    pub messages: usize,
    pub capability_updates: usize,
}
