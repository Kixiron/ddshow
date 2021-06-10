export {};

/*import React from "react";
import * as d3 from "d3";
import { Tooltip } from "@visx/tooltip";
import { AxisDomain } from "d3";

export interface GanttData<Id> {
    start: number;
    end: number;
    name: string;
    group: Id;
}

export type GanttProps<T extends GanttData<Id>, Id> =
    typeof Gantt.defaultProps & {
        width: number;
        height: number;
        bar_height: number;
        margin: { top: number; bottom: number; left: number; right: number };
        data: T[];
        style: {
            bar_color: string;
            tooltip_color: string;
        };
        axis_font_size: string;
    };

let GANTT_ID_COUNTER = 0;
type GanttState<T> = {
    id: string;
    max: number;
    min: number;
    tooltip_data?: { top: number; left: number; data: T };
};

export default class Gantt<
    T extends GanttData<Id>,
    Id extends AxisDomain,
> extends React.Component<GanttProps<T, Id>, GanttState<T>> {
    static defaultProps = {
        bar_height: 15,
        margin: {
            top: 20,
            bottom: 20,
            left: 50,
            right: 20,
        },
        axis_font_size: "11",
    };

    state: GanttState<T> = {
        // Create a unique css id for each instance of the gantt chart
        id: `gantt-svg-container-${GANTT_ID_COUNTER++}`,
        max: Math.max(...this.props.data.map(datum => datum.end)),
        min: Math.min(...this.props.data.map(datum => datum.start)),
    };

    // TODO: Enable/disable showing the following:
    //       - Operator/Arrangement creation
    //       - Operator/Arrangement destruction
    //       - Progress propagation (And other PDG stuff)
    //       - Event kinds
    //       - Individual operators/dataflows/workers
    //       - Operator/channel addresses & ids

    render() {
        return (
            <div id={this.state.id}>
                {this.state.tooltip_data && (
                    <Tooltip
                        top={this.state.tooltip_data.top}
                        left={this.state.tooltip_data.left}
                    >
                        <div>
                            <strong>{this.state.tooltip_data.data.name}</strong>
                        </div>

                        <div>
                            {/ * FIXME: Pretty durations & more info * /}
                            {this.state.tooltip_data.data.end -
                                this.state.tooltip_data.data.start}
                        </div>
                    </Tooltip>
                )}
            </div>
        );
    }

    componentDidMount() {
        // FIXME: Put each event kind into one lane and make lanes expand to
        //        accommodate overlapping events
        const group_names = new Map(
            this.props.data.map(datum => [datum.group, datum.name]),
        );

        const [width, svg_height, graph_height, top, bottom, left, right] = [
            this.props.width,
            this.props.height,
            this.props.data.length * this.props.bar_height,
            this.props.margin.top,
            this.props.margin.bottom,
            this.props.margin.left,
            this.props.margin.right,
        ];

        //const [x_max, x_min, y_max, y_min] = [
        //    this.state.max * 1.05,
        //    this.state.min > 0 ? 0 : this.state.min,
        //    this.props.data.length * this.props.bar_height * 1.05,
        //    0,
        //];

        const container = d3
            .select<HTMLDivElement, void>(`#${this.state.id}`)
            .style("overflow", "scroll")
            .style("width", width)
            .style("height", svg_height)
            .append("svg")
            .attr("height", svg_height + top + bottom)
            .attr("width", width + left + right)
            .attr("shape-rendering", "crispEdges");

        // Make a container for the body of the graph that's shifted
        // to account for margins
        const graph = container
            .append<SVGElement>("g")
            .attr("transform", `translate(${left}, ${top})`);

        const x_axis = d3
            .scaleLinear()
            .range([0, width])
            .domain([this.state.min, this.state.max]);

        const x_axis_svg = d3.axisBottom(x_axis);

        graph
            .append<SVGGElement>("g")
            .attr("transform", `translate(0, ${graph_height})`)
            .call(x_axis_svg)
            .selectAll(".tick text")
            .attr("font-size", `${this.props.axis_font_size}`)
            .attr("class", "x axis");

        d3.zoom().scaleExtent();

        const y_axis = d3
            .scaleBand<Id>()
            .range([0, graph_height])
            .domain([...group_names.keys()])
            .round(true)
            .align(0.5);

        const y_axis_svg = d3
            .axisLeft(y_axis)
            // Display the group name on the y axis display
            .tickFormat((group, _idx) => group_names.get(group)!);

        graph
            .append<SVGGElement>("g")
            .call(y_axis_svg)
            .selectAll(".tick text")
            .attr("font-size", this.props.axis_font_size)
            .attr("class", "y axis");

        const event_bars = graph
            .selectAll("rect")
            .data(this.props.data)
            .enter()
            // For each data point, create and style a rectangle
            .append("rect")
            .attr("x", datum => x_axis(datum.start))
            .attr("y", datum => y_axis(datum.group)!)
            .attr("width", datum => x_axis(datum.end) - x_axis(datum.start))
            // Make each event as tall as the y axis says they should be
            .attr("height", y_axis.bandwidth())
            // Round the corners of the rectangle
            .attr("rx", 6)
            .attr("ry", 6)
            .attr("class", this.props.style.bar_color);

        const mouseover = (event: MouseEvent, datum: T) => {
            this.setState({
                tooltip_data: {
                    top: event.pageY,
                    left: event.pageX,
                    data: datum,
                },
            });
        };

        const mouseleave = (_event: MouseEvent, _datum: T) => {
            this.setState({ tooltip_data: undefined });
        };

        const mousemove = (event: MouseEvent, datum: T) => {
            this.setState({
                tooltip_data: {
                    top: event.pageY,
                    left: event.pageX,
                    data: datum,
                },
            });
        };

        event_bars
            .on("mouseover", mouseover)
            .on("mousemove", mousemove)
            .on("mouseleave", mouseleave);

        graph.call(
            d3.zoom<SVGElement, void>().on("zoom", event => {
                console.log(event);
                //container.attr("transform", event.transform);

                graph
                    .select<SVGGElement>(".x.axis")
                    .call(event.transform.rescaleX(x_axis));

                // console.log(y_axis);
                // graph
                //     .select<SVGGElement>(".y.axis")
                //     .call(event.transform.rescaleY(y_axis));

                // graph
                //     .selectAll(".gantt-bar")
                //     .attr("transform", event.transform);
            }),
        );
    }
}*/
