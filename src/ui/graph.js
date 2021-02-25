const raw_nodes = {{ nodes | json_encode() }};
const raw_subgraphs = {{ subgraphs | json_encode() }};
const raw_edges = {{ edges | json_encode() }};

const dataflow_svg = d3.select("#dataflow-graph");
const svg = dataflow_svg.append("g");

// Create the zoomable area for the graph
const zoom = d3.zoom().on("zoom", () => {
    svg.attr("transform", d3.event.transform);
});
dataflow_svg.call(zoom);

let graph = new dagreD3.graphlib.Graph({ compound: true });
graph.setGraph({ nodesep: 50, ranksep: 50 });

let render = new dagreD3.render();

for (const node of raw_nodes) {
    const node_id = node.addr.toString();
    graph.setNode(
        node_id,
        {
            label: node.name,
            style: `fill: ${node.fill_color}`,
            labelStyle: `fill: ${node.text_color}`,
        },
    );

    const parent_addr = node.addr.slice(0, node.addr.length - 1);
    graph.setParent(node_id, parent_addr.toString());
}

for (const subgraph of raw_subgraphs) {
    const subgraph_id = subgraph.addr.toString();
    graph.setNode(
        subgraph_id,
        {
            label: subgraph.name,
            style: "fill: #EEEEEE",
            clusterLabelPos: "top",
        },
    );

    if (subgraph.addr.length > 1) {
        const parent_addr = subgraph.addr.slice(0, subgraph.addr.length - 1);
        graph.setParent(subgraph_id, parent_addr.toString());
    }
}

for (const edge of raw_edges) {
    graph.setEdge(
        edge.src.toString(),
        edge.dest.toString(),
        {},
    );
}

// Render the graph
render(svg, graph);
