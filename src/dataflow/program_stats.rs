use crate::dataflow::{
    operators::{
        DiffDuration, FilterMap, FilterMapTimed, JoinArranged, MapExt, MapTimed, Max, Min,
    },
    send_recv::ChannelAddrs,
    utils::{ArrangedKey, DifferentialLogBundle, Time, TimelyLogBundle},
    Channel, Diff, OperatorAddr,
};
use ddshow_types::{
    differential_logging::{
        BatchEvent, DifferentialEvent, DropEvent, MergeEvent, MergeShortfall, TraceShare,
    },
    ChannelId, OperatorId, WorkerId,
};
use differential_dataflow::{
    difference::{DiffPair, Present},
    operators::{CountTotal, ThresholdTotal},
    AsCollection, Collection, Data,
};
use std::time::Duration;
use timely::dataflow::{operators::Concat, Scope, Stream};

pub struct GraphStats<S>
where
    S: Scope<Timestamp = Time>,
{
    pub workers: Collection<S, WorkerId, Diff>,
    pub operators: Collection<S, OperatorAddr, Diff>,
    pub dataflows: Collection<S, OperatorAddr, Diff>,
    pub channels: Collection<S, ChannelId, Diff>,
    pub arrangements: Option<Collection<S, (WorkerId, OperatorId), Diff>>,
    /// The worker which this timespan is for and the `(start, end)` interval
    /// of their events
    pub total_runtime: Collection<S, (WorkerId, (Duration, Duration)), Diff>,
}

// TODO: Overhaul this, we shouldn't be using operator addresses everywhere since that's
//       *really* wasteful, we don't really need to be cloning as much data as this
//       inevitably will be
pub fn aggregate_program_stats<S>(
    timely: &Stream<S, TimelyLogBundle>,
    differential: Option<&Stream<S, DifferentialLogBundle>>,
    channels: &Collection<S, Channel, Diff>,
    subgraph_addresses: &ChannelAddrs<S, Diff>,
    operator_addrs_by_self: &ArrangedKey<S, OperatorAddr, Diff>,
) -> GraphStats<S>
where
    S: Scope<Timestamp = Time>,
{
    let workers = timely
        .map_ref_timed_named("Map: Worker Ids", |&time, &(_, worker, _)| {
            (worker, time, Present)
        })
        .as_collection()
        .distinct_total_core::<Diff>();

    let operators = operator_addrs_by_self
        .antijoin_arranged(subgraph_addresses)
        .map_named("Map: Reverse Operators", |(addr, _)| addr);

    let dataflows = operator_addrs_by_self
        .semijoin_arranged(subgraph_addresses)
        .filter_map_named("FilterMap: Top-level Dataflows", |(addr, _)| {
            addr.is_top_level().then(move || addr)
        });

    let channels = channels
        .map(|channel| channel.channel_id())
        .distinct_total_core::<Diff>();

    let arrangements = differential.map(|differential| {
        differential
            .filter_map_timed(|&time, (_event_time, worker, event)| {
                let operator = match event {
                    DifferentialEvent::Batch(BatchEvent { operator, .. })
                    | DifferentialEvent::Merge(MergeEvent { operator, .. })
                    | DifferentialEvent::MergeShortfall(MergeShortfall { operator, .. })
                    | DifferentialEvent::Drop(DropEvent { operator, .. })
                    | DifferentialEvent::TraceShare(TraceShare { operator, .. }) => Some(operator),
                };

                operator.map(|operator| ((worker, operator), time, Present))
            })
            .as_collection()
            .distinct_total_core::<Diff>()
    });

    let create_timestamps = |time| {
        DiffPair::new(
            Max::new(DiffDuration::new(time)),
            Min::new(DiffDuration::new(time)),
        )
    };

    let total_runtime = combine_events(
        timely,
        move |&time, (event_time, worker, _)| (worker, time, create_timestamps(event_time)),
        differential,
        move |&time, (event_time, worker, _)| (worker, time, create_timestamps(event_time)),
    )
    .as_collection()
    .count_total()
    .map_named(
        "Map: Total Runtime",
        |(
            worker,
            DiffPair {
                element1: Max { value: end },
                element2: Min { value: start },
            },
        )| (worker, (start.to_duration(), end.to_duration())),
    );

    GraphStats {
        workers,
        operators,
        dataflows,
        channels,
        arrangements,
        total_runtime,
    }
}

fn combine_events<S, D, TF, TD>(
    timely: &Stream<S, TimelyLogBundle>,
    map_timely: TF,
    differential: Option<&Stream<S, DifferentialLogBundle>>,
    map_differential: TD,
) -> Stream<S, D>
where
    S: Scope<Timestamp = Time>,
    D: Data,
    TF: Fn(&Time, TimelyLogBundle) -> D + 'static,
    TD: Fn(&Time, DifferentialLogBundle) -> D + 'static,
{
    let mut events = timely.map_timed(map_timely);
    if let Some(differential) = differential {
        events = events.concat(&differential.map_timed(map_differential));
    }

    events
}
