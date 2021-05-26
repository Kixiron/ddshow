import BubbleChartIcon from "@material-ui/icons/BubbleChart";
import DashboardIcon from "@material-ui/icons/Dashboard";
import DataLoader, { LoaderStatus, LoadStatus } from "./components/DataLoader";
import EventNoteIcon from "@material-ui/icons/EventNote";
import InfoIcon from "@material-ui/icons/Info";
import ListIcon from "@material-ui/icons/List";
import ProgramOverviewCards from "./components/ProgramOverviewCards";
import React, { Key } from "react";
import SettingsEthernetIcon from "@material-ui/icons/SettingsEthernet";
import StorageIcon from "@material-ui/icons/Storage";
import WorkerOverview from "./components/WorkerOverview";
import { Alert } from "@material-ui/lab";
import { ReactComponent as Engineering } from "./icons/engineering.svg";
import "./App.css";
import "@fontsource/roboto";
import {
    createMuiTheme,
    makeStyles,
    ThemeProvider,
} from "@material-ui/core/styles";
import {
    Toolbar,
    Typography,
    useMediaQuery,
    AppBar,
    CssBaseline,
    Snackbar,
    Tooltip,
    Drawer,
    List,
    ListItem,
    ListItemIcon,
    ListItemText,
    SvgIcon,
} from "@material-ui/core";
import {
    BrowserRouter as Router,
    Switch,
    Route,
    useHistory,
    Link,
} from "react-router-dom";
import { ClassNameMap } from "@material-ui/styles";

const menu_width = 240;
const useStyles = makeStyles(theme => ({
    root: {
        minWidth: 275,
    },
    bullet: {
        display: "inline-block",
        margin: "0px 2px",
        transform: "scale(0.8)",
    },
    title: {
        flexGrow: 1,
    },
    pos: {
        marginBottom: 12,
    },
    app_bar: {
        width: `calc(100% - ${menu_width}px)`,
        marginLeft: menu_width,
    },
    app_bar_spacer: theme.mixins.toolbar,
    program_card: {
        width: "100%",
    },
    grow: {
        flexGrow: 1,
    },
    menu_button: {
        marginRight: theme.spacing(2),
    },
    menu_list: {
        width: menu_width,
    },
}));

export type DDShowStats = {
    program: ProgramStats;
    workers: WorkerStats[];
    dataflows: DataflowStats[];
    nodes: NodeStats[];
    channels: ChannelStats[];
    arrangements: ArrangementStats[];
    events: TimelineEvent[];
    differential_enabled: boolean;
    ddshow_version: string;
};

function get_totals(
    data: DDShowStats | null,
): [number, number, number, number, number, number, number, number] {
    const workers = total_workers(data);
    const nodes = total_nodes(data);
    const subgraphs = total_subgraphs(data);
    const operators = total_operators(data);
    const dataflows = total_dataflows(data);
    const channels = total_channels(data);
    const arrangements = total_arrangements(data);
    const events = total_events(data);

    return [
        workers,
        nodes,
        subgraphs,
        operators,
        dataflows,
        channels,
        arrangements,
        events,
    ];
}

function partition_events_by_worker(
    data: DDShowStats | null,
): Map<WorkerId, TimelineEvent[]> {
    let partitioned = new Map<WorkerId, TimelineEvent[]>();
    if (!data) {
        return partitioned;
    }

    for (const event of data.events) {
        const entry = partitioned.get(event.worker);

        if (entry) {
            entry.push(event);
        } else {
            partitioned.set(event.worker, [event]);
        }
    }

    return partitioned;
}

function total_workers(data: DDShowStats | null): number {
    return data ? data.workers.length : 0;
}

function total_nodes(data: DDShowStats | null): number {
    return data ? data.nodes.length : 0;
}

function total_subgraphs(data: DDShowStats | null): number {
    return data
        ? data.nodes.filter(node => node.kind === "Subgraph").length
        : 0;
}

function total_operators(data: DDShowStats | null): number {
    return data
        ? data.nodes.filter(node => node.kind === "Operator").length
        : 0;
}

function total_dataflows(data: DDShowStats | null): number {
    return data ? data.dataflows.length : 0;
}

function total_channels(data: DDShowStats | null): number {
    return data ? data.channels.length : 0;
}

function total_arrangements(data: DDShowStats | null): number {
    return data ? data.arrangements.length : 0;
}

function total_events(data: DDShowStats | null): number {
    return data ? data.events.length : 0;
}

export type ProgramStats = {
    workers: number;
    dataflows: number;
    operators: number;
    subgraphs: number;
    channels: number;
    arrangements: number;
    events: number;
    runtime: Duration;
};

export type WorkerStats = {
    id: WorkerId;
    dataflows: number;
    operators: number;
    subgraphs: number;
    channels: number;
    arrangements: number;
    events: number;
    runtime: Duration;
    dataflow_addrs: OperatorAddr[];
};

export type DataflowStats = {
    id: OperatorId;
    addr: OperatorAddr[];
    worker: WorkerId;
    operators: number;
    subgraphs: number;
    channels: number;
    lifespan: Lifespan;
};

export type NodeStats = {
    id: OperatorId;
    addr: OperatorAddr;
    worker: WorkerId;
    name: string;
    inputs: PortId[];
    outputs: PortId[];
    lifespan: Lifespan;
    kind: NodeKind;
    activations: AggregatedStats<Duration>;
};

export type NodeKind = "Operator" | "Subgraph" | "Dataflow";

export type ChannelStats = {
    id: ChannelId;
    addr: ChannelAddr;
    worker: WorkerId;
    source_node: OperatorId;
    dest_node: OperatorId;
    kind: ChannelKind;
    lifespan: Lifespan;
};

export type ChannelKind = "Ingress" | "Egress" | "Normal";

export type ArrangementStats = {
    operator_addr: OperatorAddr;
    size_stats: AggregatedStats<number>;
    merge_stats: AggregatedStats<Duration>;
    batch_stats: AggregatedStats<number>;
    trace_shares: number;
    lifespan: Lifespan;
};

export type TimelineEvent = {
    event_id: number;
    worker: number;
    start_time: number;
    duration: number;
    event: any;
};

export type WorkerId = number;
export type OperatorId = number;
export type OperatorAddr = number[];
export type PortId = number;
export type ChannelId = number;
export type ChannelAddr = number[];

export type Duration = {
    secs: number;
    nanos: number;
};

export type Lifespan = {
    birth: Duration;
    death: Duration;
};

export type AggregatedStats<T> = {
    total: number;
    max: T;
    min: T;
    average: T;
    data_points: T;
};

function format_event(event: any): string {
    if (event.OperatorActivation) {
        return event.OperatorActivation.operator_name;
    } else if (event.Merge) {
        return event.Merge.operator_name;
    } else {
        return event.toString();
    }
}

function event_id(event: any): string {
    if (event.OperatorActivation) {
        return `${event.OperatorActivation.operator_name}-${event.OperatorActivation.operator_id}`;
    } else if (event.Merge) {
        return `${event.Merge.operator_name}-${event.Merge.operator_id}`;
    } else {
        return event.toString();
    }
}

export default function App() {
    const prefersDarkMode = useMediaQuery("(prefers-color-scheme: dark)");
    const theme = React.useMemo(
        () =>
            createMuiTheme({
                palette: {
                    type: prefersDarkMode ? "dark" : "light",
                },
            }),
        [prefersDarkMode],
    );
    const [data, set_data] = React.useState<DDShowStats | null>(null);

    const [
        workers,
        nodes,
        subgraphs,
        operators,
        dataflows,
        channels,
        arrangements,
        events,
    ] = get_totals(data);
    let worker_stats = data ? data.workers : [];
    let worker_events = partition_events_by_worker(data);

    return (
        <ThemeProvider theme={theme}>
            <CssBaseline />

            <Router>
                <Header classes={useStyles()} />

                <Switch>
                    <Route exact path="/">
                        <DataLoader
                            on_file_load={text => {
                                console.log("started parsing data");
                                const data = JSON.parse(text);
                                console.log("finished parsing data");

                                set_data(data);
                            }}
                        />

                        <ProgramOverviewCards kind="loading" />
                    </Route>

                    <Route path="/overview">
                        {/* TODO: Display a message if there's no events/stats */}
                        <ProgramOverviewCards
                            kind="loaded"
                            workers={workers}
                            dataflows={dataflows}
                            operators={operators}
                            channels={channels}
                            arrangements={arrangements}
                            total_events={events}
                        />

                        <LoadStatusAlert />
                    </Route>

                    <Route path="/workers">
                        <WorkerOverview
                            data={worker_stats.map(stats => {
                                const events = worker_events.get(stats.id)!;

                                return {
                                    stats: stats,
                                    events: events,
                                };
                            })}
                        />
                    </Route>

                    <Route path="/dataflows"></Route>

                    <Route path="/operators"></Route>

                    <Route path="/channels"></Route>

                    <Route path="/arrangements"></Route>

                    <Route path="/events"></Route>
                </Switch>
            </Router>
        </ThemeProvider>
    );
}

interface HeaderProps {
    classes: ClassNameMap<
        "root" | "app_bar" | "title" | "app_bar_spacer" | "menu_list" | "grow"
    >;
}

type HeaderState = {
    container_ref: React.RefObject<HTMLDivElement>;
    grow_ref: React.RefObject<HTMLDivElement>;
    bar_spacer_ref: React.RefObject<HTMLDivElement>;
    menu_ref: React.RefObject<HTMLDivElement>;
    selected_index: number;
};

class Header extends React.Component<HeaderProps, HeaderState> {
    state = {
        container_ref: React.createRef<HTMLDivElement>(),
        grow_ref: React.createRef<HTMLDivElement>(),
        bar_spacer_ref: React.createRef<HTMLDivElement>(),
        menu_ref: React.createRef<HTMLDivElement>(),
        selected_index: 0,
    };

    render() {
        const sidebar_items = [
            ["Overview", "/overview", <DashboardIcon />],
            [
                "Workers",
                "/workers",
                <SvgIcon>
                    <Engineering />
                </SvgIcon>,
            ],
            ["Dataflows", "/dataflows", <StorageIcon />],
            ["Operators", "/operators", <BubbleChartIcon />],
            ["Channels", "/channels", <SettingsEthernetIcon />],
            ["Arrangements", "/arrangements", <ListIcon />],
            ["Events", "/events", <EventNoteIcon />],
        ];

        return (
            <>
                <div
                    className={this.props.classes.root}
                    ref={this.state.container_ref}
                >
                    <AppBar
                        position="static"
                        className={this.props.classes.app_bar}
                    >
                        <Toolbar>
                            <Typography
                                variant="h6"
                                className={this.props.classes.title}
                            >
                                DDShow
                            </Typography>

                            <div
                                className={this.props.classes.grow}
                                ref={this.state.grow_ref}
                            />

                            <Tooltip
                                title={
                                    <React.Fragment>
                                        <Typography align="center">
                                            DDShow is made with &#10084;&#65039;
                                            by Chase Wilson
                                        </Typography>
                                    </React.Fragment>
                                }
                                aria-label="DDShow is made with love by Chase Wilson"
                            >
                                <InfoIcon />
                            </Tooltip>
                        </Toolbar>
                    </AppBar>

                    <div
                        className={this.props.classes.app_bar_spacer}
                        ref={this.state.bar_spacer_ref}
                    ></div>
                </div>

                <Drawer anchor="left" variant="permanent">
                    <div
                        className={this.props.classes.menu_list}
                        role="presentation"
                        ref={this.state.menu_ref}
                    >
                        <List>
                            {sidebar_items.map(([title, route, icon], idx) => (
                                <ListItem
                                    button
                                    key={title as Key}
                                    component={props => (
                                        <Link {...props} to={route} />
                                    )}
                                    selected={this.state.selected_index === idx}
                                    onClick={() =>
                                        this.setState({ selected_index: idx })
                                    }
                                >
                                    <ListItemIcon>{icon}</ListItemIcon>

                                    <ListItemText primary={title} />
                                </ListItem>
                            ))}
                        </List>
                    </div>
                </Drawer>
            </>
        );
    }
}

function LoadStatusAlert() {
    const [open, setOpen] = React.useState(true);
    const history = useHistory();

    const handleClose = (_event?: React.SyntheticEvent, reason?: string) => {
        if (reason === "clickaway") {
            return;
        }

        setOpen(false);
    };

    if (history && history.location && history.location.state) {
        const loader_state = history.location.state as LoadStatus;

        if (
            loader_state.data_load_status &&
            loader_state.data_load_status.status === LoaderStatus.Success
        ) {
            const message = loader_state.data_load_status.message
                ? loader_state.data_load_status.message
                : "Data file loaded";

            return (
                <Snackbar
                    open={open}
                    autoHideDuration={6000}
                    onClose={handleClose}
                >
                    <Alert
                        severity="success"
                        onClose={handleClose}
                        variant="filled"
                    >
                        Success — {message}
                    </Alert>
                </Snackbar>
            );
        }
    }

    return <></>;
}
