use anyhow::{Context as _, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use tera::{Context, Tera};

use crate::args::Args;

const GRAPH_HTML: &str = include_str!("graph.html");
const GRAPH_CSS: &str = include_str!("graph.css");
const GRAPH_JS: &str = include_str!("graph.js");
const D3_JS: &str = include_str!("d3.v5.js");
const DAGRE_JS: &str = include_str!("dagre-d3.js");

pub fn render(
    args: &Args,
    nodes: Vec<Node>,
    subgraphs: Vec<Subgraph>,
    edges: Vec<Edge>,
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

    let context = Context::from_serialize(GraphData {
        nodes,
        subgraphs,
        edges,
    })
    .context("failed to render graph context as json")?;

    let rendered_js =
        Tera::one_off(GRAPH_JS, &context, false).context("failed to render output graph")?;

    fs::write(output_dir.join("graph.js"), rendered_js)
        .context("failed to write output graph to file")?;

    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
pub struct GraphData {
    nodes: Vec<Node>,
    subgraphs: Vec<Subgraph>,
    edges: Vec<Edge>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
pub struct Node {
    pub id: usize,
    pub addr: Vec<usize>,
    pub name: String,
    pub max_activation_time: String,
    pub mix_activation_time: String,
    pub average_activation_time: String,
    pub total_activation_time: String,
    pub invocations: usize,
    pub fill_color: String,
    pub text_color: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
pub struct Subgraph {
    pub id: usize,
    pub addr: Vec<usize>,
    pub name: String,
    pub max_activation_time: String,
    pub mix_activation_time: String,
    pub average_activation_time: String,
    pub total_activation_time: String,
    pub invocations: usize,
    pub fill_color: String,
    pub text_color: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
pub struct Edge {
    pub src: Vec<usize>,
    pub dest: Vec<usize>,
    pub channel_id: usize,
    pub channel_addr: Vec<usize>,
    pub channel_name: String,
}
