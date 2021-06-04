const raw_nodes = {{ nodes | json_encode() }};
const raw_subgraphs = {{ subgraphs | json_encode() }};
const raw_edges = {{ edges | json_encode() }};
const palette_colors = {{ palette_colors | json_encode() }};
const timeline_events = {{ timeline_events | json_encode() }};

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

let error_nodes = {};

const node_id_exists = target_addr => {
    return raw_nodes.some(node => node.addr.toString() === target_addr)
        || raw_subgraphs.some(subgraph => subgraph.addr.toString() === target_addr)
        || Object.prototype.hasOwnProperty(error_nodes, target_addr);
};

const create_error_node = target_addr => {
    console.error(`created error node ${target_addr}`);
    error_nodes[target_addr] = 0;

    graph.setNode(
        target_addr,
        {
            label: `Error: ${target_addr}`,
            style: "",
            labelStyle: "",
            data: { kind: "Error" },
        },
    );
};

for (const node of raw_nodes) {
    const node_id = node.addr.toString();
    graph.setNode(
        node_id,
        {
            label: node.name.replaceAll("\\", "\\\\"),
            style: `fill: ${node.fill_color}`,
            labelStyle: `fill: ${node.text_color}`,
            data: { kind: "Node", ...node },
        },
    );

    const parent_addr = node.addr.slice(0, node.addr.length - 1).toString();
    if (!node_id_exists(parent_addr)) {
        create_error_node(parent_addr);
    }

    graph.setParent(node_id, parent_addr);
}

for (const subgraph of raw_subgraphs) {
    const subgraph_id = subgraph.addr.toString();
    graph.setNode(
        subgraph_id,
        {
            label: subgraph.name.replaceAll("\\", "\\\\"),
            style: "fill: #EEEEEE; stroke-dasharray: 5, 2;",
            clusterLabelPos: "top",
            data: { kind: "Subgraph", ...subgraph },
        },
    );

    if (subgraph.addr.length > 1) {
        const parent_addr = subgraph.addr.slice(0, subgraph.addr.length - 1).toString();
        if (!node_id_exists(parent_addr)) {
            create_error_node(parent_addr);
        }

        graph.setParent(subgraph_id, parent_addr);
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
            console.error(`invalid edge kind received: ${edge.edge_kind}"`);
            break;
    }

    const src_id = edge.src.toString();
    const dest_id = edge.dest.toString();

    if (!node_id_exists(src_id)) {
        create_error_node(src_id);
    }
    if (!node_id_exists(dest_id)) {
        create_error_node(dest_id);
    }

    graph.setEdge(
        src_id,
        dest_id,
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

// Node tooltips
svg.selectAll("g.node")
    // Reveal the tooltip on hover
    .on("mouseover", () => tooltip.style("visibility", "visible"))
    .on("mousemove", node_id => {
        const unsafe_node = graph.node(node_id);
        if (!unsafe_node || !unsafe_node.data || !unsafe_node.data.name || unsafe_node.data.kind === "Error") {
            tooltip.style("visibility", "hidden");
            return;
        }

        const node = unsafe_node.data;
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
    .on("mouseout", () => tooltip.style("visibility", "hidden"));

// Edge tooltips
svg.selectAll("g.edgePath")
    // Reveal the tooltip on hover
    .on("mouseover", () => tooltip.style("visibility", "visible"))
    .on("mousemove", edge_id => {
        const unsafe_edge = graph.edge(edge_id);
        if (!unsafe_edge || !unsafe_edge.data || !unsafe_edge.data.kind || unsafe_edge.data.kind === "Error") {
            tooltip.style("visibility", "hidden");
            return;
        }

        const edge = unsafe_edge.data;

        const get_node_name = node_addr => {
            const node = graph.node(node_addr);

            let node_name = "";
            if (!node || !node.data || !node.data.name || !node.data.kind || node.data.kind === "Error") {
                node_name = "Error";
            } else {
                node_name = node.data.name;
            }

            return node_name;
        };

        const src_name = get_node_name(edge.src);
        const dest_name = get_node_name(edge.dest);

        tooltip
            .text(`channel from ${src_name} to ${dest_name}`)
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

function worker_timeline(timeline_events) {
    let data = [];

    for (const event of timeline_events) {
        let group = data.find(group => group.group === `Worker ${event.worker}`);
        if (!group) {
            data.push({
                group: `Worker ${event.worker}`,
                data: [],
            });
            group = data.find(group => group.group === `Worker ${event.worker}`);
        }

        // TODO: Calculate this in timely
        let label = "";
        if (event.event === "Parked"
            || event.event === "Application"
            || event.event === "Input"
            || event.event === "Message"
            || event.event === "Progress"
        ) {
            label = event.event;

        } else if (event.event.OperatorActivation) {
            label = `Operator ${event.event.OperatorActivation.operator_id}: ${event.event.OperatorActivation.operator_name}`;

        } else if (event.event.Merge) {
            label = `Merge ${event.event.Merge.operator_id}: ${event.event.Merge.operator_name}`;

        } else {
            console.log("created invalid timeline event", event.event);
            continue;
        }

        let group_data = group.data.find(item => item.label === label);
        if (!group_data) {
            group.data.push({
                label: label,
                data: [],
            });
            group_data = group.data.find(item => item.label === label);
        }

        group_data.data.push({
            timeRange: [new Date(event.start_time), new Date(event.start_time + event.duration)],
            val: event.duration,
        });
    }

    TimelinesChart()(document.getElementById("worker-timeline"))
        .xTickFormat(n => +n)
        .timeFormat("%Q")
        .rightMargin(500)
        .topMargin(100)
        .bottomMargin(50)
        .zQualitative(true)
        .xTickFormat(format_duration)
        .sortAlpha(true)
        .maxHeight(4096)
        .data(data);
}

worker_timeline(timeline_events);

function format_id(id) {
    let buf = "[";
    let started = false;

    for (const segment of id) {
        if (started) {
            buf += `, ${segment}`;
        } else {
            buf += `${segment}`;
        }
    }
    buf += "]";

    return buf;
}

function format_duration(input_nanos) {
    function item_plural(buf, started, name, value) {
        if (value > 0) {
            buf += `${value}${name}`;
            if (value > 1) {
                buf += "s";
            }

            return [true, buf];
        } else {
            return [started, buf];
        }
    }

    function item(buf, started, name, value) {
        if (value > 0) {
            if (started) {
                buf += " ";
            }
            buf += `${value}${name}`;

            return [true, buf];
        } else {
            return [started, buf];
        }
    }

    let buf = "";

    let secs = Math.trunc(input_nanos / 1_000_000_000);
    let nanos = input_nanos % 1_000_000_000;

    if (secs === 0 && nanos === 0) {
        return "0s";
    }

    let years = Math.trunc(secs / 31_557_600);  // 365.25d
    let ydays = secs % 31_557_600;
    let months = Math.trunc(ydays / 2_630_016);  // 30.44d
    let mdays = ydays % 2_630_016;
    let days = Math.trunc(mdays / 86400);
    let day_secs = mdays % 86400;
    let hours = Math.trunc(day_secs / 3600);
    let minutes = Math.trunc(day_secs % 3600 / 60);
    let seconds = day_secs % 60;

    let millis = Math.trunc(nanos / 1_000_000);
    let micros = Math.trunc(nanos / 1000 % 1000);
    let nanosec = nanos % 1000;

    let started = false;
    [started, buf] = item_plural(buf, started, "year", years);
    [started, buf] = item_plural(buf, started, "month", months);
    [started, buf] = item_plural(buf, started, "day", days);

    [started, buf] = item(buf, started, "h", hours);
    [started, buf] = item(buf, started, "m", minutes);
    [started, buf] = item(buf, started, "s", seconds);
    [started, buf] = item(buf, started, "ms", millis);
    [started, buf] = item(buf, started, "us", micros);
    [started, buf] = item(buf, started, "ns", nanosec);

    return buf;
}

function time_sink_radar(events) {
    const operator_graph_div = document.getElementById("operator-graph");
    const chart = echarts.init(operator_graph_div);

    let workers = [];
    for (const event of events) {
        if (!workers.includes(event.worker)) {
            workers.push(event.worker);
        }
    }

    let time_sink_data = {};
    for (const worker of workers) {
        time_sink_data[worker] = {
            "OperatorActivation": 0,
            "Message": 0,
            "Parked": 0,
            "Merge": 0,
            "Progress": 0,
            "Input": 0,
            "Application": 0,
        };
    }

    for (const event of events) {
        let event_name = "";
        if (typeof event.event === "string") {
            event_name = event.event;
        } else {
            event_name = Object.keys(event.event)[0];
        }

        time_sink_data[event.worker][event_name] += event.duration;
    }

    let max_event_stats = [
        { name: "OperatorActivation", max: 0 },
        { name: "Message", max: 0 },
        { name: "Parked", max: 0 },
        { name: "Merge", max: 0 },
        { name: "Progress", max: 0 },
        { name: "Input", max: 0 },
        { name: "Application", max: 0 },
    ];
    let event_data = [];

    for (const worker in time_sink_data) {
        let worker_event = event_data.find(event => event.name === `Worker ${worker}`);
        if (!worker_event) {
            event_data.push({
                name: `Worker ${worker}`,
                value: [],
            });
            worker_event = event_data.find(event => event.name === `Worker ${worker}`);
        }

        for (const event_kind in time_sink_data[worker]) {
            let event_stats = max_event_stats.find(stats => stats.name === event_kind);
            if (event_stats.max < time_sink_data[worker][event_kind]) {
                event_stats.max = time_sink_data[worker][event_kind];
            }

            worker_event.value.push(time_sink_data[worker][event_kind]);
        }
    }

    // Add 10% onto each maximum to make the charts look a little better
    for (let event of max_event_stats) {
        if (event.max === 0) {
            event.max = 1000;
        } else {
            event.max += event.max * 0.10;
        }
    }

    // TODO: Formatting for tooltips
    chart.setOption({
        title: {
            text: "Program Time Allocation",
        },
        legend: {
            data: workers.map(worker => `Worker ${worker}`),
        },
        radar: {
            axisName: {
                color: "#fff",
                backgroundColor: "#999",
                borderRadius: 3,
                padding: [3, 5],
            },
            indicator: max_event_stats,
        },
        series: [{
            name: "Worker Time Comparison",
            type: "radar",
            data: event_data,
        }],
        tooltip: {
            trigger: "item",
            renderMode: "richText",
            triggerOn: "mousemove",
        },
    });
}

time_sink_radar(timeline_events);
