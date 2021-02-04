use std::{
    collections::HashMap,
    fmt::Write as _,
    io::Write,
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

use crate::{args::Args, colormap::select_color, dataflow::OperatorStats};
use anyhow::{Context, Result};
use sequence_trie::SequenceTrie;
use timely::logging::OperatesEvent;

pub fn render_graphviz_svg(args: &Args) -> Result<PathBuf> {
    let out_file = Path::new(
        args.dot_file
            .file_stem()
            .ok_or_else(|| anyhow::anyhow!("`--dot-file` must be a file"))?,
    )
    .with_extension("svg");

    Command::new("dot")
        .args(&["-Tsvg", "-Gsplines=polyline", "-Granksep=1.5", "-o"])
        .arg(&out_file)
        .arg(&args.dot_file)
        .spawn()
        .context("failed to spawn dot")?
        .wait()
        .context("dot returned with an error")?;

    Ok(out_file)
}

pub fn render_graph<W>(
    dot_graph: &mut W,
    args: &Args,
    operator_statistics: &HashMap<usize, OperatorStats>,
    graph: &SequenceTrie<usize, Graph>,
    (max_time, min_time): (Duration, Duration),
    level: usize,
) -> Result<()>
where
    W: Write,
{
    if let Some(event) = graph.value() {
        let stats = operator_statistics.get(&event.id()).unwrap();
        let label = format!(
            "{} {:#?} total over {} invocations",
            event.name(),
            stats.total,
            stats.invocations,
        );
        let tooltip = format!(
            "{:#?} average, {:#?} max, {:#?} min",
            stats.average, stats.max, stats.min,
        );

        match event {
            Graph::Node(node) => {
                let fill_color = select_color(&args.palette, stats.total, (max_time, min_time));
                let font_color = fill_color.text_color();

                writeln!(
                    dot_graph,
                    "{indent}{node_id} [label = {label:?}, fillcolor = {fill_color}, fontcolor = {font_color}, \
                        tooltip = {tooltip:?}, color = black, shape = box, style = filled];",
                    indent = " ".repeat(level * 4),
                    node_id = node_id(node)?,
                    label = label,
                    fill_color = fill_color,
                    font_color = font_color,
                    tooltip = tooltip,
                )?;

                for child in graph.children() {
                    render_graph(
                        dot_graph,
                        args,
                        operator_statistics,
                        child,
                        (max_time, min_time),
                        level,
                    )?;
                }
            }

            Graph::Subgraph(subgraph) => {
                let subgraph_indent = " ".repeat((level + 1) * 4);

                writeln!(
                    dot_graph,
                    "{indent}subgraph {node_id} {{\n{subgraph_indent}\
                        label = {label:?};\n{subgraph_indent}\
                        tooltip = {tooltip:?};",
                    indent = " ".repeat(level * 4),
                    node_id = subgraph_id(subgraph)?,
                    label = label,
                    tooltip = tooltip,
                    subgraph_indent = subgraph_indent,
                )?;

                for child in graph.children() {
                    render_graph(
                        dot_graph,
                        args,
                        operator_statistics,
                        child,
                        (max_time, min_time),
                        level + 1,
                    )?;
                }

                writeln!(dot_graph, "{}}}", " ".repeat(level * 4))?;
            }
        }
    } else {
        for child in graph.children() {
            render_graph(
                dot_graph,
                args,
                operator_statistics,
                child,
                (max_time, min_time),
                level,
            )?;
        }
    }

    Ok(())
}

pub fn node_id(operator: &OperatesEvent) -> Result<String> {
    let mut node_id = String::from("dataflow_node");
    for id in operator.addr.iter() {
        write!(&mut node_id, "_{}", id)?;
    }

    Ok(node_id)
}

// Subgraph ids must be prefixed with `cluster_`
// https://stackoverflow.com/a/7586857/9885253
fn subgraph_id(operator: &OperatesEvent) -> Result<String> {
    let mut subgraph_id = String::from("cluster_dataflow_node");
    for id in operator.addr.iter() {
        write!(&mut subgraph_id, "_{}", id)?;
    }

    Ok(subgraph_id)
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Graph {
    Node(OperatesEvent),
    Subgraph(OperatesEvent),
}

impl Graph {
    pub fn id(&self) -> usize {
        match self {
            Self::Node(node) => node.id,
            Self::Subgraph(subgraph) => subgraph.id,
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Self::Node(node) => &node.name,
            Self::Subgraph(subgraph) => &subgraph.name,
        }
    }

    pub fn as_subgraph(&self) -> Option<&OperatesEvent> {
        if let Self::Subgraph(subgraph) = self {
            Some(subgraph)
        } else {
            None
        }
    }
}
