export type DDShowStats = {
    program: ProgramStats;
    workers: WorkerStats[];
    dataflows: DataflowStats[];
    nodes: NodeStats[];
    channels: ChannelStats[];
    arrangements: ArrangementStats[];
    events: TimelineEvent[];
    differential_enabled: boolean;
    progress_enabled: boolean;
    ddshow_version: string;
};

export function get_totals(
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

export function partition_events_by_worker(
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

export function collect_root_dataflows(data: DDShowStats | null): OperatorId[] {
    return data
        ? data.nodes
              .filter(node => node.kind === "Dataflow")
              .map(node => node.id)
        : [];
}

export function total_workers(data: DDShowStats | null): number {
    return data ? data.program.workers : 0;
}

export function total_nodes(data: DDShowStats | null): number {
    return data ? data.program.operators + data.program.subgraphs : 0;
}

export function total_subgraphs(data: DDShowStats | null): number {
    return data ? data.program.subgraphs : 0;
}

export function total_operators(data: DDShowStats | null): number {
    return data ? data.program.operators : 0;
}

export function total_dataflows(data: DDShowStats | null): number {
    return data ? data.program.dataflows : 0;
}

export function total_channels(data: DDShowStats | null): number {
    return data ? data.program.channels : 0;
}

export function total_arrangements(data: DDShowStats | null): number {
    return data ? data.program.arrangements : 0;
}

export function total_events(data: DDShowStats | null): number {
    return data ? data.program.events : 0;
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
