import ParentSize from "@visx/responsive/lib/components/ParentSize";
import { AxisBottom, AxisLeft } from "@visx/axis";
import { Bar } from "@visx/shape";
import { Group } from "@visx/group";
import { scaleBand, scaleSqrt } from "@visx/scale";
import {
    Card,
    CardContent,
    Container,
    createStyles,
    Grid,
    makeStyles,
    Typography,
} from "@material-ui/core";
import React from "react";
import { Tooltip } from "@visx/tooltip";
import Gantt from "./Gantt";
import { Duration, OperatorAddr, WorkerStats, TimelineEvent } from "../App";

const useStyles = makeStyles(theme =>
    createStyles({
        root: {
            flexGrow: 1,
            width: "100%",
            height: "100%",
        },
        heading: {
            fontSize: theme.typography.pxToRem(15),
            fontWeight: theme.typography.fontWeightRegular,
        },
        pos: {
            marginBottom: 12,
        },
        bar: {
            fill: theme.palette.primary.light,
        },
        ticks: {
            transform: "rotate(90)",
        },
        bar_tooltip: {
            color: theme.palette.background.paper,
        },
        bar_color: {
            fill: theme.palette.info.dark,
        },
        gantt_tooltip: {
            color: theme.palette.getContrastText(
                theme.palette.background.paper,
            ),
            backgroundColor: theme.palette.background.paper,
            borderRadius: "5px",
            border: `2px ${theme.palette.divider}`,
        },
    }),
);

type WorkerOverviewProps = {
    data: WorkerData[];
};

type WorkerData = {
    stats: WorkerStats;
    events: TimelineEvent[];
};

export default function WorkerOverview(props: WorkerOverviewProps) {
    const classes = useStyles();

    let worker_cards = [];
    let workers = chunked(props.data, 2);

    let row_workers = workers.next();
    while (!row_workers.done) {
        const row_data = row_workers.value;
        const row_key = row_data.map(worker => worker.stats.id).join("-");

        const row = (
            <Grid
                container
                spacing={3}
                style={{ width: "100%", height: "100%" }}
                key={`worker-grid-row-${row_key}`}
            >
                {row_data.map(worker => {
                    const stats = worker.stats;
                    const worker_key = `worker-grid-card-${stats.id}`;

                    return (
                        <Grid
                            item
                            xs={6}
                            style={{ width: "100%", height: "100%" }}
                            key={worker_key}
                        >
                            <WorkerCard worker={worker} />
                        </Grid>
                    );
                })}
            </Grid>
        );

        worker_cards.push(row);
        row_workers = workers.next();
    }

    const gantt_events =
        props.data.length >= 1
            ? props.data[0].events.map(event => {
                  return {
                      start: event.start_time,
                      end: event.start_time + event.duration,
                      // FIXME: This is bad
                      name: "Eeee",
                      group: event.worker,
                  };
              })
            : [];

    return (
        <Container maxWidth="lg" style={{ width: "100%", height: "100%" }}>
            <Grid
                container
                spacing={1}
                style={{ width: "100%", height: "100%" }}
                key="worker-card-grid-outer"
            >
                {worker_cards}

                <Grid
                    item
                    style={{ width: "100%", height: "100%" }}
                    xs={12}
                    key="worker-card-grid-gantt-overview"
                >
                    <Gantt
                        width={1000}
                        height={1000}
                        data={gantt_events}
                        style={{
                            bar_color: classes.bar_color,
                            tooltip_color: classes.gantt_tooltip,
                        }}
                    />
                </Grid>
            </Grid>
        </Container>
    );
}

function WorkerCard(props: { worker: WorkerData }) {
    const classes = useStyles();
    const stats = props.worker.stats;

    let overview_data = [
        { name: "Total Dataflows", value: stats.dataflows },
        { name: "Total Operators", value: stats.operators },
        { name: "Total Subgraphs", value: stats.subgraphs },
        { name: "Total Channels", value: stats.channels },
    ];
    if (stats.arrangements) {
        overview_data.push({
            name: "Total Arrangements",
            value: stats.arrangements,
        });
    }

    return (
        <Card
            className={classes.root}
            style={{ width: "100%", height: "100%" }}
        >
            <CardContent style={{ width: "100%", height: "100%" }}>
                <Typography gutterBottom variant="h5" component="h2">
                    Worker {stats.id}
                </Typography>

                {/* TODO: Pretty print the duration */}
                <Typography className={classes.pos} color="textSecondary">
                    Ran for a total of {stats.runtime.secs} seconds and{" "}
                    {stats.runtime.nanos} nanoseconds
                </Typography>

                <ParentSize
                    parentSizeStyles={{ width: "100%", height: "100%" }}
                    style={{ width: "100%", height: "100%" }}
                    enableDebounceLeadingCall={true}
                    debounceTime={1000}
                >
                    {parent => (
                        <BarChart
                            width={Math.max(parent.width, 500)}
                            height={Math.max(
                                Math.min(parent.height, parent.width),
                                500,
                            )}
                            data={overview_data}
                        />
                    )}
                </ParentSize>
            </CardContent>
        </Card>
    );
}

type BarChartProps = {
    width: number;
    height: number;
    data: { name: string; value: number }[];
};

function BarChart({ width, height, data: raw_data }: BarChartProps) {
    const data = raw_data.filter(data => data.value > 0);
    const [tooltip_data, set_tooltip_data] =
        React.useState<{
            data: { name: string; value: number };
            top: number;
            left: number;
        } | null>(null);

    const classes = useStyles();
    const margin = { top: 40, left: 50, right: 40, bottom: 100 };
    const x_max = width - margin.left - margin.right;
    const y_max = height - margin.top - margin.bottom;

    const scale_x = scaleBand<string>({
        range: [0, x_max],
        round: true,
        domain: data.map(data => data.name),
        padding: 0.4,
    });
    const scale_y = scaleSqrt<number>({
        range: [y_max, 0],
        round: true,
        domain: [0, Math.max(...data.map(data => data.value))],
    });

    return (
        <>
            <svg width={width} height={height}>
                <Group
                    top={margin.top}
                    left={margin.left}
                    width={width}
                    height={height}
                >
                    {data.map(data => {
                        const name = data.name;
                        const normalized_name = name
                            .replace(" ", "-")
                            .toLowerCase();
                        const bar_width = scale_x.bandwidth();
                        const bar_height = y_max - (scale_y(data.value) ?? 0);
                        const bar_x = scale_x(name);
                        if (!bar_x) {
                            return <></>;
                        }
                        const bar_y = y_max - bar_height;

                        return (
                            <Bar
                                key={`worker-bar-${normalized_name}`}
                                x={bar_x}
                                y={bar_y}
                                width={bar_width}
                                height={bar_height}
                                className={classes.bar}
                                onMouseLeave={() => {
                                    set_tooltip_data(null);
                                }}
                                onMouseMove={event => {
                                    set_tooltip_data({
                                        data: data,
                                        top: event.pageY,
                                        left: event.pageX,
                                    });
                                }}
                            />
                        );
                    })}

                    <AxisLeft
                        hideAxisLine
                        hideTicks
                        scale={scale_y}
                        stroke="#fff"
                        tickStroke="#fff"
                        tickLabelProps={() => ({
                            fill: "#fff",
                            fontSize: 11,
                            textAnchor: "end",
                            dy: "0.33em",
                        })}
                    />

                    <AxisBottom
                        scale={scale_x}
                        top={y_max}
                        tickFormat={name => name}
                        stroke="#fff"
                        tickStroke="#fff"
                        tickLabelProps={() => ({
                            fill: "#fff",
                            fontSize: 11,
                            textAnchor: "middle",
                            dy: "0.33em",
                        })}
                    />
                </Group>
            </svg>

            {tooltip_data && (
                <Tooltip top={tooltip_data.top} left={tooltip_data.left}>
                    <div>
                        <strong>{tooltip_data.data.name}</strong>
                    </div>

                    <div>{tooltip_data.data.value}</div>
                </Tooltip>
            )}
        </>
    );
}

function* chunked<T>(array: T[], size: number): Generator<T[], null, void> {
    if (size === 0) {
        throw new Error("cannot chunk arrays into chunks of length zero");
    }

    if (array.length === 0) {
        return null;
    }

    for (let i = 0; i < array.length; i += size) {
        yield array.slice(i, i + size);
    }

    return null;
}
