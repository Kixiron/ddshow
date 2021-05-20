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

type GraphData = {
    nodes: Node[];
    subgraphs: Subgraph[];
    edges: Edge[];
    timeline_events: TimelineEvent[];
};
type Node = {
    id: number;
    worker: number;
    addr: number[];
    name: string;
    max_activation_time: string;
    min_activation_time: string;
    average_activation_time: string;
    total_activation_time: string;
    invocations: number;
    fill_color: string;
    text_color: string;
    activation_durations: ActivationDuration[];
    max_arrangement_size: number | null;
    min_arrangement_size: number | null;
};
type ActivationDuration = {};
type Subgraph = {
    id: number;
    worker: number;
    addr: number[];
    name: string;
    max_activation_time: string;
    min_activation_time: string;
    average_activation_time: string;
    total_activation_time: string;
    invocations: number;
    fill_color: string;
    text_color: string;
};
type Edge = {};
type TimelineEvent = {
    event_id: number;
    worker: number;
    start_time: number;
    duration: number;
    event: any;
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
    const [data, set_data] = React.useState<GraphData | null>(null);

    let total_arrangements = undefined;
    const total = data
        ? data.nodes.filter(
              node => node.min_arrangement_size || node.max_activation_time,
          ).length
        : 0;
    if (total === 0) {
        total_arrangements = undefined;
    } else {
        total_arrangements = total;
    }

    const total_workers = data
        ? new Set(data.timeline_events.map(event => event.worker)).size
        : 0;
    const total_operators = data
        ? data.subgraphs.length + data.nodes.length
        : 0;
    const dataflows = data
        ? data.subgraphs
              .filter(subgraph => subgraph.addr.length === 1)
              .map(subgraph => subgraph.addr)
        : [];
    const total_dataflows = dataflows.length;
    const total_channels = data ? data.edges.length : 0;
    const total_events = data ? data.timeline_events.length : 0;
    const total_subgraphs = data ? data.subgraphs.length : 0;
    const total_nodes = total_operators + total_subgraphs;

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
                            workers={total_workers}
                            dataflows={total_dataflows}
                            operators={total_operators}
                            channels={total_channels}
                            arrangements={total_arrangements}
                            total_events={total_events}
                        />

                        <LoadStatusAlert />
                    </Route>

                    <Route path="/workers">
                        <WorkerOverview
                            data={[
                                {
                                    worker_id: 0,
                                    total_runtime: { secs: 1000, nanos: 5322 },
                                    total_dataflows: total_dataflows,
                                    total_nodes: total_nodes,
                                    total_operators: total_operators,
                                    total_subgraphs: total_subgraphs,
                                    total_channels: total_channels,
                                    total_events: total_events,
                                    total_arrangements: total_arrangements,
                                    dataflow_addrs: dataflows,
                                    events: data
                                        ? data.timeline_events
                                              .filter(
                                                  event => event.worker === 0,
                                              )
                                              .map(event => ({
                                                  start: event.start_time,
                                                  end:
                                                      event.start_time +
                                                      event.duration,
                                                  name: format_event(
                                                      event.event,
                                                  ),
                                                  group: event_id(event.event),
                                              }))
                                        : [],
                                },
                            ]}
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
