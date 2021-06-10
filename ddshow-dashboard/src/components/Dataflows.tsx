import * as d3 from "d3";
import dagreD3 from "dagre-d3";
import React from "react";
import {
    ArrangementStats,
    ChannelStats,
    NodeStats,
    OperatorAddr,
    OperatorId,
} from "../DDShowData";
import "./Dataflows.css";

type DataflowProps = {
    root_dataflows: OperatorId[];
    nodes: NodeStats[];
    channels: ChannelStats[];
    arrangements: ArrangementStats[];
};

type DataflowState = {
    current_dataflow: OperatorId;
    dataflow_svg: React.RefObject<HTMLDivElement>;
    graph: dagreD3.graphlib.Graph<{ data: NodeData }>;
    error_nodes: Set<string>;
};

interface NodeOptions extends NodeData {
    label: string;
    style?: string;
    labelStyle?: string;
    clusterLabelPos?: string;
}

interface NodeData {
    data: { kind: "Error" } | NodeStats;
}

interface EdgeData extends ChannelStats {
    src_addr: OperatorAddr;
    dest_addr: OperatorAddr;
}

export default class Dataflows extends React.Component<
    DataflowProps,
    DataflowState
> {
    constructor(props: DataflowProps) {
        super(props);

        let graph = new dagreD3.graphlib.Graph<{ data: NodeData }>({
            compound: true,
        });
        graph.setGraph({ nodesep: 50, ranksep: 50 });

        this.state = {
            current_dataflow: this.props.root_dataflows[0],
            dataflow_svg: React.createRef<HTMLDivElement>(),
            graph: graph,
            error_nodes: new Set(),
        };
    }

    render() {
        return <div id="dataflow-graph" ref={this.state.dataflow_svg}></div>;
    }

    // TODO: Filling the graph probably shouldn't happen in the on mount method
    componentDidMount() {
        let current_dataflow = this.state.current_dataflow;

        for (const node of this.props.nodes) {
            // TODO: Put dataflow membership into nodes/channels/arrangements
            if (node.addr[0] === current_dataflow) {
                let node_id = pretty_addr(node.addr);

                let options: NodeOptions = {
                    label: pretty_name(node.name),
                    data: node,
                };

                if (node.kind === "Subgraph" || node.kind === "Dataflow") {
                    options.clusterLabelPos = "top";
                    options.style = "fill: #EEEEEE; stroke-dasharray: 5, 2;";
                } else if (node.kind === "Operator") {
                    // TODO: Do the color stuff somewhere (timely?)
                    // options.style = `fill: ${node.fill_color}`;
                    // options.labelStyle = `fill: ${node.text_color}`;
                }

                this.state.graph.setNode(node_id, options);

                if (node.addr.length > 1) {
                    const parent_addr = pretty_addr(
                        node.addr.slice(0, node.addr.length - 1),
                    );

                    if (!this.node_id_exists(parent_addr)) {
                        this.create_error_node(parent_addr);
                    }

                    this.state.graph.setParent(node_id, parent_addr);
                }
            }
        }

        for (const channel of this.props.channels) {
            let style = "";
            switch (channel.kind) {
                case "Ingress":
                case "Egress":
                    // Blue
                    style =
                        "stroke: #5d5de6; stroke-dasharray: 5, 2; fill: none;";
                    break;

                case "Normal":
                    break;
            }

            // FIXME: Oh god this is horrible
            const src_node = this.props.nodes.find(
                node => node.id === channel.source_node,
            );

            let src_id;
            if (src_node) {
                src_id = pretty_addr(src_node.addr);
            } else {
                console.error(
                    `channel attempted to connect to source node that doesn't exist. \
                    channel: ${channel}, source: ${channel.source_node}`,
                );
                continue;
            }

            // FIXME: Oh god this is also horrible
            const dest_node = this.props.nodes.find(
                node => node.id === channel.dest_node,
            );

            let dest_id;
            if (dest_node) {
                dest_id = pretty_addr(dest_node.addr);
            } else {
                console.error(
                    `channel attempted to connect to dest node that doesn't exist. \
                    channel: ${channel}, dest: ${channel.dest_node}`,
                );
                continue;
            }

            let data: EdgeData = {
                ...channel,
                src_addr: src_node.addr,
                dest_addr: dest_node.addr,
            };

            this.state.graph.setEdge(src_id, dest_id, {
                style: style,
                data: data,
            });
        }

        // FIXME: Donno if this will work
        const dataflow_svg = d3.select("#dataflow-graph");
        const svg = dataflow_svg.append<SVGGElement>("g");
        console.error("here", dataflow_svg, svg, this.state.graph);

        // Create the zoomable area for the graph
        const zoom = d3.zoom().on("zoom", () => {
            svg.attr("transform", (d3 as any).event.transform);
        });
        dataflow_svg.call(zoom as any);

        const render = new dagreD3.render();
        // FIXME: This is hacky but idk how else to work around
        //        dagre's weirdness other than vendoring it
        render(svg as any, this.state.graph as any);

        // Center & scale the graph
        const initial_scale = 1.0;
        console.log(
            Number(svg.attr("width")),
            svg.attr("width"),
            this.state.graph.graph().width,
            this.state.graph.graph().height,
            this.state.graph.graph().width! * initial_scale,
        );

        d3.zoomIdentity
            .translate(
                (Number(svg.attr("width")) -
                    this.state.graph.graph().width! * initial_scale) /
                    2,
                20,
            )
            .scale(initial_scale);

        dataflow_svg.attr(
            "height",
            this.state.graph.graph().height! * initial_scale + 40,
        );
    }

    node_id_exists(target_addr: string): boolean {
        return (
            this.props.nodes.some(
                node => pretty_addr(node.addr) === target_addr,
            ) || this.state.error_nodes.has(target_addr)
        );
    }

    create_error_node(target_addr: string) {
        console.error(`created error node ${target_addr}`);

        this.state.error_nodes.add(target_addr);
        this.state.graph.setNode(target_addr, {
            label: `Error: ${target_addr}`,
            style: "",
            labelStyle: "",
            data: { kind: "Error" },
        });
    }
}

function pretty_addr(addr: OperatorAddr): string {
    return `[${addr.join(", ")}]`;
}

function pretty_name(name: string): string {
    return name.replaceAll("\\", "\\\\");
}
