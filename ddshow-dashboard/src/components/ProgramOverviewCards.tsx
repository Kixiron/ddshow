import {
    Card,
    CardContent,
    Container,
    Grid,
    makeStyles,
    Typography,
} from "@material-ui/core";
import { Skeleton } from "@material-ui/lab";
import React from "react";

const useStyles = makeStyles({
    pos: {
        marginBottom: 12,
    },
    program_card: {
        width: "100%",
        transition: "transform 0.15s ease-in-out",
    },
    card_hovered: {
        transform: "scale3d(1.05, 1.05, 1)",
    },
    title: {
        font_size: 14,
    },
    skeleton_container: {
        height: 0,
        overflow: "hidden",
        paddingTop: "100%",
        position: "relative",
    },
    skeleton_loader: {
        position: "absolute",
        top: 0,
        left: 0,
        width: "100%",
        height: "100%",
    },
});

type ProgramOverviewProps = Loading | Loaded;

interface Loading {
    kind: "loading";
}

interface Loaded {
    kind: "loaded";
    workers: number;
    dataflows: number;
    operators: number;
    channels: number;
    arrangements?: number;
    total_events: number;
}

export default function ProgramOverviewCards(props: ProgramOverviewProps) {
    return (
        <Container maxWidth="md">
            <Grid container spacing={1}>
                <Grid container item xs={12} spacing={3}>
                    <ProgramCard
                        name="Workers"
                        description="The number of timely workers created by the program"
                        props={props}
                        prop_key="workers"
                    />

                    <ProgramCard
                        name="Dataflows"
                        description="The number of dataflows created by the program"
                        props={props}
                        prop_key="dataflows"
                    />
                </Grid>

                <Grid container item xs={12} spacing={3}>
                    <ProgramCard
                        name="Operators"
                        description="The number of operators created by the program"
                        props={props}
                        prop_key="operators"
                    />

                    <ProgramCard
                        name="Channels"
                        description="The number of channels connecting operators together"
                        props={props}
                        prop_key="channels"
                    />
                </Grid>

                <Grid container item xs={12} spacing={3}>
                    <ProgramCard
                        name="Arrangements"
                        description="The number of arrangements created by the program"
                        props={props}
                        prop_key="arrangements"
                    />

                    <ProgramCard
                        name="Total Events"
                        description="The number of events received from the program"
                        props={props}
                        prop_key="total_events"
                    />
                </Grid>
            </Grid>
        </Container>
    );
}

// Type fuckery because TS doesn't have proper fucking sum types
function get_property<T>(object: T, property_name: keyof T): T[keyof T] {
    return object[property_name];
}

export type ProgramCardProps = {
    name: string;
    description: string;
    props: ProgramOverviewProps;
    prop_key: keyof Loaded;
};

export function ProgramCard(props: ProgramCardProps) {
    const skeleton = <Skeleton animation="wave" />;
    const [hovered, setHovered] = React.useState(false);
    const classes = useStyles();

    return (
        <Grid container item xs={6}>
            <Card
                className={classes.program_card}
                classes={{ root: hovered ? classes.card_hovered : "" }}
                onMouseOver={() => setHovered(true)}
                onMouseOut={() => setHovered(false)}
            >
                <CardContent>
                    <Typography
                        className={classes.title}
                        color="textSecondary"
                        gutterBottom
                    >
                        {props.props.kind === "loading" ? skeleton : props.name}
                    </Typography>

                    <Typography variant="h5">
                        {props.props.kind === "loading"
                            ? skeleton
                            : get_property<Loaded>(props.props, props.prop_key)}
                    </Typography>

                    <Typography className={classes.pos} color="textSecondary">
                        {props.props.kind === "loading"
                            ? skeleton
                            : props.description}
                    </Typography>
                </CardContent>
            </Card>
        </Grid>
    );
}
