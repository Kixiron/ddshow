#![allow(clippy::unused_unit)]

use crate::{
    args::Args,
    dataflow::{OperatorProgress, OperatorShape, TimelineEvent as RawTimelineEvent},
};
use abomonation_derive::Abomonation;
use anyhow::{Context as _, Result};
use bytecheck::CheckBytes;
use ddshow_types::{ChannelId, OperatorAddr, OperatorId, PortId, WorkerId};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use std::{fs, time::Duration};
use tera::{Context, Tera};

const GRAPH_HTML: &str = include_str!("graph.html");
const GRAPH_CSS: &str = include_str!("graph.css");
const GRAPH_JS: &str = include_str!("graph.js");
const D3_JS: &str = include_str!("d3.v5.js");
const DAGRE_JS: &str = include_str!("dagre-d3.js");
const ECHARTS_JS: &str = include_str!("echarts.min.js");

#[allow(clippy::too_many_arguments)]
pub fn render(
    args: &Args,
    nodes: Vec<Node>,
    subgraphs: Vec<Subgraph>,
    edges: Vec<Edge>,
    palette_colors: Vec<String>,
    timeline_events: Vec<RawTimelineEvent>,
    operator_shapes: Vec<OperatorShape>,
    operator_progress: Vec<OperatorProgress>,
) -> Result<()> {
    let output_dir = &args.output_dir;
    tracing::info!(output_dir = ?output_dir, "writing graph files to disk");

    fs::create_dir_all(output_dir).context("failed to create output directory")?;

    fs::write(output_dir.join("graph.html"), GRAPH_HTML)
        .context("failed to write output graph to file")?;

    fs::write(output_dir.join("graph.css"), GRAPH_CSS)
        .context("failed to write output graph to file")?;

    fs::write(output_dir.join("d3.v5.js"), D3_JS)
        .context("failed to write output graph to file")?;

    fs::write(output_dir.join("dagre-d3.js"), DAGRE_JS)
        .context("failed to write output graph to file")?;

    fs::write(output_dir.join("echarts.min.js"), ECHARTS_JS)
        .context("failed to write output graph to file")?;

    let graph_data = GraphData {
        nodes,
        subgraphs,
        edges,
        palette_colors,
        timeline_events,
        operator_shapes,
        operator_progress,
    };

    let context =
        Context::from_serialize(graph_data).context("failed to render graph context as json")?;

    let rendered_js =
        Tera::one_off(GRAPH_JS, &context, false).context("failed to render output graph")?;

    fs::write(output_dir.join("graph.js"), rendered_js)
        .context("failed to write output graph to file")?;

    Ok(())
}

// TODO: Move this to another crate, make serde & abomonation feature-gated,
//       add wasm-bindgen under a feature gate

//  - whether differential logging was enabled
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
#[allow(clippy::upper_case_acronyms)]
#[archive_attr(derive(CheckBytes))]
pub struct DDShowStats {
    pub program: ProgramStats,
    // TODO: Should/would these be better as trees?
    pub workers: Vec<WorkerStats>,
    pub dataflows: Vec<DataflowStats>,
    pub nodes: Vec<NodeStats>,
    pub channels: Vec<ChannelStats>,
    pub arrangements: Vec<ArrangementStats>,
    pub events: Vec<TimelineEvent>,
    pub differential_enabled: bool,
    pub progress_enabled: bool,
    pub ddshow_version: String,
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

#[derive(Debug, Clone, PartialEq, PartialOrd, Deserialize, Serialize)]
pub struct GraphData {
    pub nodes: Vec<Node>,
    pub subgraphs: Vec<Subgraph>,
    pub edges: Vec<Edge>,
    pub palette_colors: Vec<String>,
    pub timeline_events: Vec<RawTimelineEvent>,
    pub operator_shapes: Vec<OperatorShape>,
    pub operator_progress: Vec<OperatorProgress>,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Deserialize, Serialize)]
pub struct Node {
    pub id: OperatorId,
    pub worker: WorkerId,
    pub addr: OperatorAddr,
    pub name: String,
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

#[derive(Debug, Clone, PartialEq, PartialOrd, Deserialize, Serialize)]
pub struct Subgraph {
    pub id: OperatorId,
    pub worker: WorkerId,
    pub addr: OperatorAddr,
    pub name: String,
    pub max_activation_time: String,
    pub min_activation_time: String,
    pub average_activation_time: String,
    pub total_activation_time: String,
    pub invocations: usize,
    pub fill_color: String,
    pub text_color: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
pub struct Edge {
    pub src: OperatorAddr,
    pub dest: OperatorAddr,
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
