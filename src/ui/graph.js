// @ts-check
"use-strict";

/**
 * {#
 * TODO: Replace dagre.js/d3.js with cytoscape.js, dagre is now
 *       unmaintained. See what hierarchical layout algorithms
 *       that it offers, maybe it'll help to speed things up.
 * TODO: Graph node collapsing, collapse sub-regions and allow the
 *       user to expand them to see the nodes and subgraphs within
 *       them.
 * TODO: Maybe look into egraph-rs for replacing the current JS-based
 *       graph rendering libraries, maybe it'll make things faster to
 *       load and render up. May need a more advanced hierarchical layout
 *       algorithm, maybe try to get into touch with the author about
 *       that and for general documentation.
 *       https://github.com/likr/egraph-rs
 * TODO: Replace the vega scatter plot with a custom wasm one,
 *       use a quadtree to support streaming updates and incremental
 *       re-drawing (only invalidate & redraw the squares actually
 *       touched by the new data) and a gigatrace-derived index
 *       to downsample data at zoomed scales. Maybe look into the
 *       newly-stabilized `core::arch::wasm` simd intrinsics to speed
 *       things up where possible within index querying.
 *       https://github.com/trishume/gigatrace
 * TODO: Straight up write this in typescript and then compile & bundle it,
 *       this weak & painful typing really sucks. Can probably use wasm_bindgen
 *       to generate typings for the input data as well, that'll make life so
 *       much easier.
 * TODO: Figure out a better method of getting the data into the page, pasting
 *       it in directly is sub-optimal. Ideally you'd be able to load things
 *       and maybe even compare different runs.
 * 
 * @global {any} d3
 * @global {any} dagreD3
 * 
 * @typedef {{
 *     id: number;
 *     addr: number[];
 *     name: string;
 *     max_activation_time: string;
 *     min_activation_time: string;
 *     average_activation_time: string;
 *     total_activation_time: string;
 *     invocations: number;
 *     fill_color: string;
 *     text_color: string;
 *     activation_durations: ActivationDuration[];
 *     max_arrangement_size: number | null;
 *     min_arrangement_size: number | null;
 * }} RawNode
 *
 * @typedef {{ activation_time: number, activated_at: number }} ActivationDuration
 * 
 * @typedef {{
 *    id: number;
 *    addr: number[];
 *    name: string;
 *    max_activation_time: string;
 *    min_activation_time: string;
 *    average_activation_time: string;
 *    total_activation_time: string;
 *    invocations: number;
 *    fill_color: string;
 *    text_color: string;
 * }} Subgraph
 * 
 * @typedef {{
 *     src: number[];
 *     dest: number[];
 *     channel_id: number;
 *     edge_kind: EdgeKind;
 * }} Edge
 * 
 * @typedef {"Normal" | "Crossing"} EdgeKind
 * 
 * @typedef {{
 *     worker: number;
 *     event: EventKind;
 *     start_time: number;
 *     duration: number;
 *     collapsed_events: number;
 * }} TimelineEvent
 * 
 * @typedef {Activation | Application | "Parked" | "Input" | "Message" | "Progress" | Merge} EventKind
 * 
 * @typedef {{ OperatorActivation: { operator_id: number } }} Activation
 * @typedef {{ Application: { id: number } }} Application
 * @typedef {{ Merge: { operator_id: number } }} Merge
 * 
 * @typedef {{
 *     id: number;
 *     addr: number[];
 *     inputs: number[];
 *     outputs: number[];
 * }} OperatorShape
 * 
 * @typedef {{
 *     operator: number;
 *     worker: number;
 *     input_messages: [number, [number, number]][];
 *     output_messages: [number, [number, number]][];
 * }} OperatorProgress
 * #}
 */

// {#
/** @type {any} */
const d3 = undefined;
/** @type {any} */
const dagreD3 = undefined;
/** @type {any} */
const vega = undefined;
/** @type {any} */
const vegaEmbed = undefined;
// #}

/** @type {RawNode[]} */
const raw_nodes = {{ nodes | json_encode() }};

/** @type {Subgraph[]} */
const raw_subgraphs = {{ subgraphs | json_encode() }};

/** @type {Edge[]} */
const raw_edges = {{ edges | json_encode() }};

/** @type {string[]} */
const palette_colors = {{ palette_colors | json_encode() }};

/** @type {TimelineEvent[]} */
const timeline_events = {{ timeline_events | json_encode() }};

/** @type {OperatorShape[]} */
const operator_shapes = {{ operator_shapes | json_encode() }};

const vega_data = {{ vega_data | json_encode() }};


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

/** @type {Set<string>} */
let error_nodes = new Set();
/** @type {Set<string>} */
let operator_addrs = new Set();
/** @type {Map<number, string>} */
let operator_names = new Map();
/** @type {Set<string>} */
let subgraph_addrs = new Set();

/**
 * Check if the given node address exists
 * @param {string} target_addr The address to check the existence of
 * @returns {boolean} Whether or not the address exists
 */
const node_id_exists = target_addr => {
    return error_nodes.has(target_addr)
        || operator_addrs.has(target_addr)
        || raw_nodes.some(node => format_addr(node.addr) === target_addr)
        || raw_subgraphs.some(subgraph => format_addr(subgraph.addr) === target_addr);
};

/**
 * Creates an error node for the given address
 * @param {string} target_addr The given address to create an error node for
 */
const create_error_node = target_addr => {
    console.error(`created error node ${target_addr}`);
    error_nodes.add(target_addr);

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

const slash_regexp = new RegExp("\\\\", "g");

for (const subgraph of raw_subgraphs) {
    const subgraph_name = subgraph.name;
    operator_names.set(subgraph.id, subgraph_name);

    const subgraph_addr = format_addr(subgraph.addr);
    operator_addrs.add(subgraph_addr);
    subgraph_addrs.add(subgraph_addr);

    graph.setNode(
        subgraph_addr,
        {
            label: `${subgraph_name.replace(slash_regexp, "\\\\")} @ ${subgraph.id}, ${subgraph_addr}`,
            style: "fill: #EEEEEE; stroke-dasharray: 5, 2;",
            clusterLabelPos: "top",
            data: { kind: "Subgraph", ...subgraph },
        },
    );

    if (subgraph.addr.length > 1) {
        const parent_addr = format_addr(subgraph.addr.slice(0, subgraph.addr.length - 1));

        if (!node_id_exists(parent_addr)) {
            create_error_node(parent_addr);
        }

        graph.setParent(subgraph_addr, parent_addr);
    }
}

for (const node of raw_nodes) {
    const node_name = node.name;
    operator_names.set(node.id, node_name);

    const node_addr = format_addr(node.addr);
    operator_addrs.add(node_addr);

    graph.setNode(
        node_addr,
        {
            label: `${node_name.replace(slash_regexp, "\\\\")} @ ${node.id}, ${node_addr}`,
            style: `fill: ${node.fill_color}`,
            labelStyle: `fill: ${node.text_color}`,
            data: { kind: "Node", ...node },
        },
    );

    const parent_addr = format_addr(node.addr.slice(0, node.addr.length - 1));
    if (!node_id_exists(parent_addr)) {
        create_error_node(parent_addr);
    }

    graph.setParent(node_addr, parent_addr);
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

    const src_id = format_addr(edge.src);
    const dest_id = format_addr(edge.dest);

    if (!subgraph_addrs.has(src_id) && !subgraph_addrs.has(dest_id)) {
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
    } else {
        console.warn(
            `skipped edge from ${src_id} to ${dest_id}, ${subgraph_addrs.has(src_id) ? src_id : dest_id} is a subgraph and dagre is stupid`,
        );
    }
}

// Render the graph
try {
    render(svg, graph);
} catch (err) {
    console.error(`failed to render dataflow graph: ${err} `);
}

// Create the tooltip div
const tooltip = d3.select("#dataflow-graph-div")
    .append("div")
    .attr("id", "tooltip-template");

// Node tooltips
svg.selectAll("g.node")
    // Reveal the tooltip on hover
    .on("mouseover", () => tooltip.style("visibility", "visible"))
    .on(
        "mousemove",
        /** @param {string} node_id */
        node_id => {
            const unsafe_node = graph.node(node_id);
            if (!unsafe_node || !unsafe_node.data || !unsafe_node.data.name || unsafe_node.data.kind === "Error") {
                tooltip.style("visibility", "hidden");
                return;
            }

            const node = unsafe_node.data;
            let html = `ran for ${node.total_activation_time} over ${node.invocations} invocations < br >\
                average runtime of ${node.average_activation_time} \
    (max: ${node.max_activation_time}, min: ${node.min_activation_time})`;

            if (node.kind === "Node"
                && node.max_arrangement_size !== null
                && node.min_arrangement_size !== null
            ) {
                html += `< br > max arrangement size: ${node.max_arrangement_size}, \
                    min arrangement size: ${node.min_arrangement_size} `;
            }

            tooltip
                .html(html)
                .style("top", (d3.event.pageY - 40) + "px")
                .style("left", (d3.event.pageX + 40) + "px");
        },
    )
    // Hide the tooltip on mouseout
    .on("mouseout", () => tooltip.style("visibility", "hidden"));

// Edge tooltips
svg.selectAll("g.edgePath")
    // Reveal the tooltip on hover
    .on("mouseover", () => tooltip.style("visibility", "visible"))
    .on(
        "mousemove",
        /** @param {string} edge_id */
        edge_id => {
            const unsafe_edge = graph.edge(edge_id);
            if (!unsafe_edge || !unsafe_edge.data || !unsafe_edge.data.kind || unsafe_edge.data.kind === "Error") {
                tooltip.style("visibility", "hidden");
                return;
            }

            /** @type {Edge} */
            const edge = unsafe_edge.data;

            /**
             * @param {number[]} node_addr The node address to get the name of
             * @returns {string} The node's name
             */
            const get_node_name = node_addr => {
                const node = graph.node(format_addr(node_addr));

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

            let html = `channel from ${src_name} to ${dest_name} `;

            tooltip
                .html(html)
                .style("top", (d3.event.pageY - 40) + "px")
                .style("left", (d3.event.pageX + 40) + "px");
        },
    )
    // Hide the tooltip on mouseout
    .on("mouseout", () => tooltip.style("visibility", "hidden"));

// Add the palette legend
const palette_legend = d3.select("body")
    .append("div")
    .attr("id", "palette-legend");

// Make the gradient's css
let palette_gradient = "";
let gradient_counter = 0;
for (const color of palette_colors) {
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
    .style("background", `linear - gradient(to top, ${palette_gradient})`);

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

/**
 * Formats an operator address into a human-readable string
 * @param {number[]} addr The operator address to format
 * @returns string
 */
function format_addr(addr) {
    let buf = "[";
    let started = false;

    for (const segment of addr) {
        if (started) {
            buf += `, ${segment} `;
        } else {
            buf += `${segment} `;
            started = true;
        }
    }
    buf += "]";

    return buf;
}

// TODO: Make this operate off of the `Duration` type
/**
 * Formats a duration into a human-readable string
 * @param {number} input_nanos The input time to format
 * @returns string The formatted duration
 */
function format_duration(input_nanos) {
    /**
     * @param {string} buf The buffer to write info
     * @param {boolean} started Whether or not this is the first in the series
     * @param {string} name The name of the time unit
     * @param {number} value The value of the time unit
     * @return {[boolean, string]} The new value of `started` and the filled buffer
     */
    function item_plural(buf, started, name, value) {
        if (value > 0) {
            buf += `${value}${name} `;
            if (value > 1) {
                buf += "s";
            }

            return [true, buf];
        } else {
            return [started, buf];
        }
    }

    /**
     * @param {string} buf The buffer to write info
     * @param {boolean} started Whether or not this is the first in the series
     * @param {string} name The name of the time unit
     * @param {number} value The value of the time unit
     * @return {[boolean, string]} The new value of `started` and the filled buffer
     */
    function item(buf, started, name, value) {
        if (value > 0) {
            if (started) {
                buf += " ";
            }
            buf += `${value}${name} `;

            return [true, buf];
        } else {
            return [started, buf];
        }
    }

    const YEAR = 31_557_600;
    const MONTH = 2_630_016;
    const DAY = 86_400;
    const HOUR = 3_600;
    const MINUTE = 60;
    const MILLISECOND = 1_000_000;
    const MICRO_IN_MILLIS = 1_000;
    const NANOSECOND = 1_000_000_000;

    let buf = "";

    let secs = Math.trunc(input_nanos / NANOSECOND);
    let nanos = input_nanos % NANOSECOND;

    if (secs === 0 && nanos === 0) {
        return "0s";
    }

    let years = Math.trunc(secs / YEAR); // 365.25d
    let ydays = secs % YEAR;
    let months = Math.trunc(ydays / MONTH); // 30.44d
    let mdays = ydays % MONTH;
    let days = Math.trunc(mdays / DAY);
    let day_secs = mdays % DAY;
    let hours = Math.trunc(day_secs / HOUR);
    let minutes = Math.trunc(day_secs % HOUR / MINUTE);
    let seconds = day_secs % MINUTE;

    let millis = Math.trunc(nanos / MILLISECOND);
    let micros = Math.trunc(nanos / MICRO_IN_MILLIS % MICRO_IN_MILLIS);
    let nanosec = nanos % MICRO_IN_MILLIS;

    let started = false;
    [started, buf] = item_plural(buf, started, "year", years);
    [started, buf] = item_plural(buf, started, "month", months);
    [started, buf] = item_plural(buf, started, "day", days);

    [started, buf] = item(buf, started, "h", hours);
    [started, buf] = item(buf, started, "m", minutes);
    [started, buf] = item(buf, started, "s", seconds);
    [started, buf] = item(buf, started, "ms", millis);
    [started, buf] = item(buf, started, "µs", micros);
    [started, buf] = item(buf, started, "ns", nanosec);

    return buf;
}

// TODO: Type this with `VisualizationSpec`
const ddshow_spec = {
    $schema: "https://vega.github.io/schema/vega-lite/v5.json",
    data: {
        name: "aggregated_stats",
        values: vega_data,
    },
    config: {
        customFormatTypes: true,
    },
    background: "#EEEEEE",
    params: [
        {
            name: "filter_subgraphs",
            value: false,
            bind: {
                input: "checkbox"
            }
        },
        {
            name: "enable_activation_graphs",
            value: true,
            bind: {
                input: "checkbox"
            }
        },
        {
            name: "enable_compaction_graphs",
            value: true,
            bind: {
                input: "checkbox"
            }
        },
        {
            name: "operators_to_display",
            value: 10,
            bind: {
                input: "range",
                min: 1,
                // TODO: Get a dynamic max
                max: 1000,
                step: 1,
            }
        }
    ],
    transform: [
        {
            calculate: "[datum.name, datum.id]",
            as: "name_and_id"
        },
        {
            filter: "!filter_subgraphs || datum.node_kind != \"Subgraph\""
        }
    ],
    vconcat: [
        {
            title: "Total Operator Runtime",
            mark: "bar",
            width: 1000,
            transform: [
                {
                    window: [{
                        op: "rank",
                        as: "rank"
                    }],
                    sort: [{ field: "total_runtime", order: "descending" }]
                },
                {
                    filter: "datum.rank <= operators_to_display"
                }
            ],
            encoding: {
                y: {
                    field: "name_and_id",
                    title: "Operator",
                    type: "nominal",
                    sort: "-x",
                    axis: {
                        labelExpr: "join([datum.value[0], datum.value[1]], \", \")"
                    }
                },
                x: {
                    field: "total_runtime",
                    title: "Total Runtime",
                    type: "quantitative",
                    axis: {
                        formatType: "format_duration",
                    },
                },
                color: {
                    field: "name_and_id",
                    title: null,
                    legend: {
                        labelExpr: "join([datum.value[0], datum.value[1]], \", \")",
                    },
                },
                tooltip: [
                    {
                        field: "name_and_id[0]",
                        title: "Operator",
                    },
                    {
                        field: "name_and_id[1]",
                        title: "Operator ID",
                    },
                    {
                        field: "total_runtime",
                        title: "Total Runtime",
                        formatType: "format_duration",
                    },
                ],
            }
        },
        // TODO: hconcat these two
        {
            title: "Maximum Arrangement Size",
            transform: [
                {
                    "filter": "datum.max_arrangement_size != null && datum.max_arrangement_size > 0",
                },
                {
                    window: [{
                        op: "rank",
                        as: "rank"
                    }],
                    sort: [{ field: "max_arrangement_size", order: "descending" }],
                },
                {
                    filter: "datum.rank <= operators_to_display",
                },
                {
                    calculate: "[datum.max_arrangement_size, datum.min_arrangement_size || 0, datum.arrangement_batches || 0]",
                    as: "arrangement_stats",
                },
            ],
            mark: "bar",
            width: 900,
            encoding: {
                y: {
                    field: "name_and_id",
                    title: "Operator",
                    type: "nominal",
                    sort: "-x",
                    axis: {
                        labelExpr: "join([datum.value[0], datum.value[1]], \", \")",
                    },
                },
                x: {
                    field: "max_arrangement_size",
                    title: "Arrangement Size",
                    type: "quantitative",
                },
                tooltip: [
                    {
                        field: "name_and_id[0]",
                        title: "Operator",
                    },
                    {
                        field: "name_and_id[1]",
                        title: "Operator ID",
                    },
                    {
                        field: "max_arrangement_size",
                        title: "Max Size",
                    },
                    {
                        field: "arrangement_stats[1]",
                        title: "Min Size",
                    },
                    {
                        field: "arrangement_stats[2]",
                        title: "Batches",
                    },
                ]
            }
        },
        {
            title: "Total Arrangement Batches",
            transform: [
                {
                    filter: "datum.arrangement_batches != null && datum.arrangement_batches > 0",
                },
                {
                    calculate: "[datum.max_arrangement_size, datum.min_arrangement_size || 0, datum.arrangement_batches || 0]",
                    as: "arrangement_stats",
                },
                {
                    window: [{
                        op: "rank",
                        as: "rank",
                    }],
                    sort: [{ "field": "arrangement_batches", "order": "descending" }],
                },
                {
                    filter: "datum.rank <= operators_to_display",
                },
            ],
            mark: "bar",
            width: 900,
            encoding: {
                y: {
                    field: "name_and_id",
                    title: "Operator",
                    type: "nominal",
                    sort: "-x",
                    axis: {
                        labelExpr: "join([datum.value[0], datum.value[1]], \", \")"
                    }
                },
                x: {
                    field: "arrangement_batches",
                    title: "Batches",
                    aggregate: "sum",
                    type: "quantitative"
                },
                tooltip: [
                    {
                        field: "name_and_id[0]",
                        title: "Operator"
                    },
                    {
                        field: "name_and_id[1]",
                        title: "Operator ID"
                    },
                    {
                        field: "arrangement_batches",
                        title: "Batches"
                    },
                    {
                        field: "max_arrangement_size",
                        title: "Max Size"
                    },
                    {
                        field: "arrangement_stats[1]",
                        title: "Min Size"
                    }
                ]
            }
        },
        {
            title: "Operator Activation Durations",
            transform: [
                {
                    flatten: [
                        "activation_durations"
                    ]
                },
                {
                    calculate: "datum.activation_durations[0]",
                    as: "start_time"
                },
                {
                    calculate: "datum.activation_durations[1]",
                    as: "duration"
                }
            ],
            mark: "point",
            width: 1000,
            height: 1000,
            params: [
                {
                    name: "brush",
                    select: {
                        type: "interval",
                        resolve: "union",
                        on: "[mousedown[event.shiftKey], window:mouseup] > window:mousemove!",
                        translate: "[mousedown[event.shiftKey], window:mouseup] > window:mousemove!",
                        zoom: "wheel![event.shiftKey]"
                    }
                },
                {
                    name: "grid",
                    select: {
                        type: "interval",
                        resolve: "global",
                        translate: "[mousedown[!event.shiftKey], window:mouseup] > window:mousemove!",
                        zoom: "wheel![!event.shiftKey]"
                    },
                    bind: "scales"
                }
            ],
            encoding: {
                x: {
                    field: "start_time",
                    title: "Time Activated",
                    type: "quantitative",
                    axis: { formatType: "format_duration" },
                },
                y: {
                    field: "duration",
                    title: "Activation Duration",
                    type: "quantitative",
                    axis: { formatType: "format_duration" },
                },
                color: {
                    field: "name_and_id",
                    type: "nominal",
                },
                tooltip: [
                    {
                        field: "name_and_id[0]",
                        title: "Operator",
                    },
                    {
                        field: "name_and_id[1]",
                        title: "Operator ID",
                    },
                    {
                        field: "start_time",
                        title: "Activated at",
                        formatType: "format_duration",
                    },
                    {
                        field: "duration",
                        title: "Activated for",
                        formatType: "format_duration",
                    }
                ]
            }
        },
        {
            title: "Arrangement Compaction",
            transform: [
                {
                    filter: "enable_compaction_graphs",
                },
                {
                    flatten: ["per_worker"],
                },
                {
                    filter: "datum.per_worker[1].spline_levels != null && datum.per_worker[1].spline_levels.length > 0",
                },
                {
                    calculate: "datum.per_worker[1].spline_levels",
                    as: "spline_levels",
                },
                {
                    flatten: ["spline_levels"],
                },
                {
                    calculate: "datum.spline_levels[0]",
                    as: "merge_time",
                },
                {
                    calculate: "datum.spline_levels[1]",
                    as: "merge_scale",
                },
                {
                    calculate: "datum.spline_levels[2]",
                    as: "merge_complete_size",
                }
            ],
            mark: "bar",
            encoding: {
                x: {
                    field: "merge_scale",
                    title: "Merge Scale",
                    type: "quantitative",
                },
                y: {
                    field: "merge_complete_size",
                    title: "Completed Merge Size",
                    type: "quantitative",
                    aggregate: "count",
                },
                tooltip: [
                    {
                        field: "name_and_id[0]",
                        title: "Operator",
                    },
                    {
                        field: "name_and_id[1]",
                        title: "Operator ID",
                    },
                    {
                        field: "merge_scale",
                        title: "Merge Scale",
                    },
                    {
                        field: "merge_complete_size",
                        title: "Completed Merge Size",
                    },
                    {
                        field: "merge_time",
                        title: "Merge Occurred At",
                        formatType: "format_duration",
                    },
                ],
            },
        },
    ],
};

vega.expressionFunction(
    "format_duration",
    /**
     * @param {number} datum The input nanosecond timestamp
     * @param {any} _params Unused
     */
    (datum, _params) => format_duration(datum),
);

vegaEmbed(
    "#stats-graphs",
    ddshow_spec,
    {
        // logLevel: vega.Debug,
        actions: {
            export: true,
            source: false,
            compiled: false,
            editor: false,
        },
    },
);
