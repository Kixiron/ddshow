const raw_nodes = {{ nodes | json_encode() }};
const raw_subgraphs = {{ subgraphs | json_encode() }};
const raw_edges = {{ edges | json_encode() }};
const palette_colors = {{ palette_colors | json_encode() }};

const dataflow_svg = d3.select("#dataflow-graph");
const svg = dataflow_svg.append("g");

// Create the zoomable area for the graph
const zoom = d3.zoom().on("zoom", () => {
    svg.attr("transform", d3.event.transform);
});
dataflow_svg.call(zoom);

const graph = new dagreD3.graphlib.Graph({ compound: true });
graph.setGraph({ nodesep: 50, ranksep: 50 });

const render = new dagreD3.render();

for (const node of raw_nodes) {
    const node_id = node.addr.toString();
    graph.setNode(
        node_id,
        {
            label: node.name,
            style: `fill: ${node.fill_color}`,
            labelStyle: `fill: ${node.text_color}`,
            data: { kind: "Node", ...node },
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
            style: "fill: #EEEEEE; stroke-dasharray: 5, 2;",
            clusterLabelPos: "top",
            data: { kind: "Subgraph", ...subgraph },
        },
    );

    if (subgraph.addr.length > 1) {
        const parent_addr = subgraph.addr.slice(0, subgraph.addr.length - 1);
        graph.setParent(subgraph_id, parent_addr.toString());
    }
}

for (const edge of raw_edges) {
    let style = "";
    switch (edge.edge_kind) {
        case "Crossing":
            // Blue
            style = "stroke: #5d5de6; stroke-dasharray: 5, 2; fill: none;"
            break;

        case "Normal":
            break;

        default:
            throw "Invalid edge kind received";
    }

    graph.setEdge(
        edge.src.toString(),
        edge.dest.toString(),
        {
            style: style,
            data: { kind: "Edge", ...edge },
        },
    );
}

// Render the graph
render(svg, graph);

// Create the tooltip div
const tooltip = d3.select("#dataflow-graph-div")
    .append("div")
    .attr("id", "tooltip-template");

const margin = { top: 10, right: 30, bottom: 30, left: 60 };
const width = 1080 - margin.left - margin.right;
const height = 750 - margin.top - margin.bottom;

const scatter_plot = d3.select("#operator-timing-scatter")
    .append("svg")
    .attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)
    .append("g")
    .attr("transform", `translate(${margin.left}, ${margin.top})`);

const scatter_tooltip = d3.select("#operator-timing-scatter")
    .append("div")
    .style("opacity", 0)
    .attr("class", "tooltip")
    .style("background-color", "white")
    .style("border", "solid")
    .style("border-width", "1px")
    .style("border-radius", "5px")
    .style("padding", "10px")
    .style("z-index", 5);

const x_axis = scatter_plot.append("g");
const y_axis = scatter_plot.append("g");
const scatter_dots = scatter_plot.append("g");

const operator_scatter_plot = (node) => {
    const x = d3.scaleLinear()
        .domain([
            Math.min(...node.activation_durations.map(duration => duration.activated_at)),
            Math.max(...node.activation_durations.map(duration => duration.activated_at)),
        ])
        .range([0, width]);

    x_axis
        .attr("transform", `translate(0, ${height})`)
        .call(d3.axisBottom(x));

    const y = d3.scaleLinear()
        .domain([
            Math.min(...node.activation_durations.map(duration => duration.activation_time)),
            Math.max(...node.activation_durations.map(duration => duration.activation_time)),
        ])
        .range([height, 0]);

    y_axis.call(d3.axisLeft(y));

    const mouseover = duration => {
        scatter_tooltip
            .text(`activated for ${duration.activation_time}ns`)
            // It is important to put the +90: otherwise the tooltip
            // is exactly where the point is an it creates a weird effect
            .style("left", (d3.event.pageX + 90) + "px")
            .style("top", d3.event.pageY + "px")
            .style("opacity", 1);
    };

    const mouseleave = _duration => {
        scatter_tooltip
            .transition()
            .duration(200)
            .style("opacity", 0);
    };

    scatter_dots
        .selectAll(".scatter-circle")
        .remove();

    console.log(node.activation_durations);
    scatter_dots
        .selectAll("dot")
        .data(node.activation_durations)
        .enter()
        .append("circle")
        .attr("class", "scatter-circle")
        .attr("cx", activation => x(activation.activated_at))
        .attr("cy", activation => y(activation.activation_time))
        .attr("r", 7)
        .style("fill", "#69b3a2")
        .style("opacity", 1)
        .style("stroke", "white")
        .on("mouseover", mouseover)
        .on("mousemove", mouseover)
        .on("mouseleave", mouseleave);
};

// Node tooltips
svg.selectAll("g.node")
    // Reveal the tooltip on hover
    .on("mouseover", () => tooltip.style("visibility", "visible"))
    .on("mousemove", node_id => {
        const node = graph.node(node_id).data;
        let text = `ran for ${node.total_activation_time} over ${node.invocations} invocations<br>\
            average runtime of ${node.average_activation_time} \
            (max: ${node.max_activation_time}, min: ${node.min_activation_time})`;

        if (node.kind === "Node"
            && node.max_arrangement_size !== null
            && node.min_arrangement_size !== null
        ) {
            text += `<br>max arrangement size: ${node.max_arrangement_size}, \
                min arrangement size: ${node.min_arrangement_size}`;
        }

        tooltip
            .html(text)
            .style("top", (d3.event.pageY - 40) + "px")
            .style("left", (d3.event.pageX + 40) + "px");
    })
    // Hide the tooltip on mouseout
    .on("mouseout", () => tooltip.style("visibility", "hidden"))
    .on("click", node_id => {
        const node = graph.node(node_id).data;
        operator_scatter_plot(node);
    });

// Edge tooltips
svg.selectAll("g.edgePath")
    // Reveal the tooltip on hover
    .on("mouseover", () => tooltip.style("visibility", "visible"))
    .on("mousemove", edge_id => {
        const edge = graph.edge(edge_id).data;
        const src = graph.node(edge.src).data,
            dest = graph.node(edge.dest).data;

        tooltip
            .text(`channel from ${src.name} to ${dest.name}`)
            .style("top", (d3.event.pageY - 40) + "px")
            .style("left", (d3.event.pageX + 40) + "px");
    })
    // Hide the tooltip on mouseout
    .on("mouseout", () => tooltip.style("visibility", "hidden"));

// Add the palette legend
const palette_legend = d3.select("body")
    .append("div")
    .attr("id", "palette-legend");

// Make the gradient's css
let palette_gradient = "",
    gradient_counter = 0;
for (color of palette_colors) {
    palette_gradient += `${color} ${gradient_counter}%, `;
    gradient_counter += 100 / palette_colors.length;
}
if (palette_gradient.endsWith(", ")) {
    palette_gradient = palette_gradient.substring(0, palette_gradient.length - 2);
}

// Top text
palette_legend
    .append("div")
    .attr("class", "palette-text")
    .attr("id", "palette-top-text")
    .text("slower");

// Heatgraph gradient
palette_legend
    .append("div")
    .attr("id", "palette-gradient")
    .style("background", `linear-gradient(to top, ${palette_gradient})`);

// Bottom text
palette_legend
    .append("div")
    .attr("class", "palette-text")
    .attr("id", "palette-bottom-text")
    .text("faster");

// Center & scale the graph
const initial_scale = 1.00;
d3.zoomIdentity
    .translate([(svg.attr("width") - graph.graph().width * initial_scale) / 2, 20])
    .scale(initial_scale);
dataflow_svg.attr("height", graph.graph().height * initial_scale + 40);
