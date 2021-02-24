use anyhow::{Context as _, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use tera::{Context, Tera};

const GRAPH_HTML: &str = include_str!("graph.html");

pub fn render(nodes: Vec<Node>, subgraphs: Vec<Subgraph>, edges: Vec<Edge>) -> Result<()> {
    let context = Context::from_serialize(GraphData {
        nodes,
        subgraphs,
        edges,
    })
    .context("failed to render graph context as json")?;

    let rendered =
        Tera::one_off(GRAPH_HTML, &context, false).context("failed to render output graph")?;
    fs::write("graph.html", rendered).context("failed to write output graph to file")?;

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
