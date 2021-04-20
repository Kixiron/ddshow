use crate::{
    args::Args,
    dataflow::{OperatorAddr, WorkerId, WorkerTimelineEvent},
};
use anyhow::{Context as _, Result};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File},
    io::BufWriter,
};
use tera::{Context, Tera};

const GRAPH_HTML: &str = include_str!("graph.html");
const GRAPH_CSS: &str = include_str!("graph.css");
const GRAPH_JS: &str = include_str!("graph.js");
const D3_JS: &str = include_str!("d3.v5.js");
const DAGRE_JS: &str = include_str!("dagre-d3.js");
const ECHARTS_JS: &str = include_str!("echarts.min.js");

pub fn render(
    args: &Args,
    nodes: Vec<Node>,
    subgraphs: Vec<Subgraph>,
    edges: Vec<Edge>,
    palette_colors: Vec<String>,
    timeline_events: Vec<WorkerTimelineEvent>,
) -> Result<()> {
    let output_dir = &args.output_dir;

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
    };

    serde_json::to_writer(
        BufWriter::new(File::create(output_dir.join("data.json")).unwrap()),
        &graph_data,
    )
    .unwrap();

    let context =
        Context::from_serialize(graph_data).context("failed to render graph context as json")?;

    let rendered_js =
        Tera::one_off(GRAPH_JS, &context, false).context("failed to render output graph")?;

    fs::write(output_dir.join("graph.js"), rendered_js)
        .context("failed to write output graph to file")?;

    Ok(())
}

// TODO: A better representation for this to minimize size and maximize speed
//
// Required data:
//
// - Nodes
//   - id
//   - worker
//   - address
//   - name
//   - inputs
//   - outputs
//   - whether it's a subgraph
//   - whether it's a root dataflow
//
// - Edges
//   - id
//   - worker
//   - address
//   - name
//   - edge kind
//   - edge id (is this even a real thing?)
//   - source node
//   - dest node
//
// - Node stats
//   - operator address
//   - number of invocations
//   - max activation time
//   - min activation time
//   - average activation time
//   - all activation durations
//
// - Arrangement stats
//   - operator address
//   - max arrangement size
//   - min arrangement size
//   - average arrangement size
//   - all arrangement sizes
//   - number of merges
//   - merge timings
//   - number of batches received
//   - batch sizes
//
// - Timeline events
//  - event id (is this actually needed?)
//  - worker
//  - event
//  - when the event started
//  - when the event ended (unneeded?)
//  - event duration

#[derive(Debug, Clone, PartialEq, PartialOrd, Deserialize, Serialize)]
pub struct GraphData {
    nodes: Vec<Node>,
    subgraphs: Vec<Subgraph>,
    edges: Vec<Edge>,
    palette_colors: Vec<String>,
    timeline_events: Vec<WorkerTimelineEvent>,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Deserialize, Serialize)]
pub struct Node {
    pub id: usize,
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
    pub id: usize,
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
    pub channel_id: usize,
    pub edge_kind: EdgeKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
pub enum EdgeKind {
    Normal,
    Crossing,
}
