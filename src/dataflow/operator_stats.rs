use crate::{
    dataflow::{
        differential::ArrangementStats,
        granulate,
        summation::{summation, Summation},
        worker_timeline::{process_timely_event, EventProcessor, TimelineEventStream},
        ArrangedKey, ArrangedVal, ChannelId, Diff, OperatorAddr, OperatorId, TimelyLogBundle,
        WorkerId,
    },
    ui::Lifespan,
};
use abomonation_derive::Abomonation;
use ddshow_types::timely_logging::{ChannelsEvent, OperatesEvent, StartStop, TimelyEvent};
use differential_dataflow::{
    collection::AsCollection,
    lattice::Lattice,
    operators::{
        arrange::{Arrange, ArrangeByKey},
        Join, Reduce,
    },
    Collection,
};
use std::{collections::HashMap, time::Duration};
use timely::dataflow::{
    channels::pact::Exchange, operators::generic::builder_rc::OperatorBuilder, Scope, Stream,
};

type TimelyCollections<S> = (
    // Operator lifespans
    Collection<S, ((WorkerId, OperatorId), Lifespan), Diff>,
    // Operator activation times `(start, duration)`
    Collection<S, ((WorkerId, OperatorId), (Duration, Duration)), Diff>,
    // Operator creation times
    Collection<S, ((WorkerId, OperatorId), Duration), Diff>,
    // Channel creation times
    Collection<S, ((WorkerId, ChannelId), Duration), Diff>,
    // Raw channel events
    // TODO: Remove the need for this
    Collection<S, (WorkerId, ChannelsEvent), Diff>,
    // Raw operator events
    // TODO: Remove the need for this
    Collection<S, (WorkerId, OperatesEvent), Diff>,
    // Operator names
    ArrangedVal<S, (WorkerId, OperatorId), String>,
    // Operator ids to addresses
    ArrangedVal<S, (WorkerId, OperatorId), OperatorAddr>,
    // Operator addresses to ids
    ArrangedVal<S, (WorkerId, OperatorAddr), OperatorId>,
    // Operator addresses
    ArrangedKey<S, (WorkerId, OperatorAddr)>,
    // Channel scope addresses
    ArrangedVal<S, (WorkerId, ChannelId), OperatorAddr>,
    // Dataflow operator ids
    ArrangedKey<S, (WorkerId, OperatorId)>,
    // Timely event data
    TimelineEventStream<S>,
);

// TODO: These could all emit `Present` difference types since there's no retractions here
pub(super) fn extract_timely_info<S>(
    scope: &mut S,
    timely_stream: &Stream<S, TimelyLogBundle>,
) -> TimelyCollections<S>
where
    S: Scope<Timestamp = Duration>,
{
    let mut builder = OperatorBuilder::new("Extract Operator Info".to_owned(), scope.clone());
    builder.set_notify(false);

    let mut timely_stream = builder.new_input(
        // TODO: We may be able to get away with only granulating the input stream
        //       as long as we make sure no downstream consumers depend on either
        //       the timestamp of the timely stream or the differential collection
        //       for diagnostic data
        timely_stream,
        // Distribute events across all workers based on the worker they originated from
        // This should be done by default when the number of ddshow workers is the same
        // as the number of timely ones, but this ensures that it happens for disk replay
        // and unbalanced numbers of workers to ensure work is fairly divided
        Exchange::new(|&(_, id, _): &(_, WorkerId, _)| id.into_inner() as u64),
    );

    // Create all of the outputs
    let (mut lifespan_out, lifespan_stream) = builder.new_output();
    let (mut activation_duration_out, activation_duration_stream) = builder.new_output();
    let (mut operator_creation_out, operator_creation_stream) = builder.new_output();
    let (mut channel_creation_out, channel_creation_stream) = builder.new_output();
    let (mut raw_channels_out, raw_channels_stream) = builder.new_output();
    let (mut raw_operators_out, raw_operators_stream) = builder.new_output();
    let (mut operator_names_out, operator_names_stream) = builder.new_output();
    let (mut operator_ids_out, operator_ids_stream) = builder.new_output();
    let (mut operator_addrs_out, operator_addrs_stream) = builder.new_output();
    let (mut operator_addrs_by_self_out, operator_addrs_by_self_stream) = builder.new_output();
    let (mut channel_scope_addrs_out, channel_scope_addrs_stream) = builder.new_output();
    let (mut dataflow_ids_out, dataflow_ids_stream) = builder.new_output();
    let (mut worker_events_out, worker_events_stream) = builder.new_output();

    builder.build_reschedule(move |_capabilities| {
        move |_frontiers| {
            let mut buffer = Vec::new();
            let (
                mut lifespan_map,
                mut activation_map,
                mut event_map,
                mut map_buffer,
                mut stack_buffer,
            ) = (
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                Vec::new(),
            );

            // Activate all the outputs
            let mut lifespan_handle = lifespan_out.activate();
            let mut activation_duration_handle = activation_duration_out.activate();
            let mut operator_creation_handle = operator_creation_out.activate();
            let mut channel_creation_handle = channel_creation_out.activate();
            let mut raw_channels_handle = raw_channels_out.activate();
            let mut raw_operators_handle = raw_operators_out.activate();
            let mut operator_names_handle = operator_names_out.activate();
            let mut operator_ids_handle = operator_ids_out.activate();
            let mut operator_addrs_handle = operator_addrs_out.activate();
            let mut operator_addrs_by_self_handle = operator_addrs_by_self_out.activate();
            let mut channel_scope_addrs_handle = channel_scope_addrs_out.activate();
            let mut dataflow_ids_handle = dataflow_ids_out.activate();
            let mut worker_events_handle = worker_events_out.activate();

            timely_stream.for_each(|capability, data| {
                data.swap(&mut buffer);

                for (time, worker, event) in buffer.drain(..) {
                    // Get the timestamp for the current event
                    let session_time = capability.time().join(&time);

                    // Get capabilities for every individual output
                    let lifespan_cap = capability.delayed_for_output(&session_time, 0);
                    let activation_duration_cap = capability.delayed_for_output(&session_time, 1);
                    let operator_creation_cap = capability.delayed_for_output(&session_time, 2);
                    let channel_creation_cap = capability.delayed_for_output(&session_time, 3);
                    let raw_channels_cap = capability.delayed_for_output(&session_time, 4);
                    let raw_operators_cap = capability.delayed_for_output(&session_time, 5);
                    let operator_names_cap = capability.delayed_for_output(&session_time, 6);
                    let operator_ids_cap = capability.delayed_for_output(&session_time, 7);
                    let operator_addrs_cap = capability.delayed_for_output(&session_time, 8);
                    let operator_addrs_by_self_cap =
                        capability.delayed_for_output(&session_time, 9);
                    let channel_scope_addrs_cap = capability.delayed_for_output(&session_time, 10);
                    let dataflow_ids_cap = capability.delayed_for_output(&session_time, 11);
                    // Note: This has a different timestamp than the other capabilities,
                    //       this is because of the machinery within `EventProcessor`
                    let worker_events_cap = capability.delayed_for_output(capability.time(), 12);

                    let mut event_processor = EventProcessor::new(
                        &mut event_map,
                        &mut map_buffer,
                        &mut stack_buffer,
                        &mut worker_events_handle,
                        &worker_events_cap,
                        worker,
                        time,
                    );

                    process_timely_event(&mut event_processor, event.clone());

                    match event {
                        TimelyEvent::Operates(operates) => {
                            lifespan_map.insert((worker, operates.id), time);

                            // Emit raw operator events
                            raw_operators_handle.session(&raw_operators_cap).give((
                                (worker, operates.clone()),
                                session_time,
                                1,
                            ));

                            // Emit operator creation times
                            operator_creation_handle
                                .session(&operator_creation_cap)
                                .give((((worker, operates.id), time), session_time, 1));

                            // Emit operator names
                            operator_names_handle.session(&operator_names_cap).give((
                                ((worker, operates.id), operates.name),
                                session_time,
                                1,
                            ));

                            // Emit dataflow ids
                            if operates.addr.is_top_level() {
                                dataflow_ids_handle.session(&dataflow_ids_cap).give((
                                    ((worker, operates.id), ()),
                                    session_time,
                                    1,
                                ));
                            }

                            // Emit operator ids
                            operator_ids_handle.session(&operator_ids_cap).give((
                                ((worker, operates.id), operates.addr.clone()),
                                session_time,
                                1,
                            ));

                            // Emit operator addresses
                            operator_addrs_handle.session(&operator_addrs_cap).give((
                                ((worker, operates.addr.clone()), operates.id),
                                session_time,
                                1,
                            ));
                            operator_addrs_by_self_handle
                                .session(&operator_addrs_by_self_cap)
                                .give((((worker, operates.addr), ()), session_time, 1));
                        }

                        TimelyEvent::Shutdown(shutdown) => {
                            if let Some(start_time) = lifespan_map.remove(&(worker, shutdown.id)) {
                                lifespan_handle.session(&lifespan_cap).give((
                                    ((worker, shutdown.id), Lifespan::new(start_time, time)),
                                    session_time,
                                    1,
                                ));
                            }

                            // Remove any dangling activations
                            activation_map.remove(&(worker, shutdown.id));
                        }

                        TimelyEvent::Schedule(schedule) => {
                            let operator = schedule.id;

                            match schedule.start_stop {
                                StartStop::Start => {
                                    activation_map.insert((worker, operator), time);
                                }

                                StartStop::Stop => {
                                    if let Some(start_time) =
                                        activation_map.remove(&(worker, operator))
                                    {
                                        let duration = time - start_time;
                                        activation_duration_handle
                                            .session(&activation_duration_cap)
                                            .give((
                                                ((worker, operator), (start_time, duration)),
                                                session_time,
                                                1,
                                            ));
                                    }
                                }
                            }
                        }

                        TimelyEvent::Channels(channel) => {
                            // Emit raw channels
                            raw_channels_handle.session(&raw_channels_cap).give((
                                (worker, channel.clone()),
                                session_time,
                                1,
                            ));

                            // Emit channel creation times
                            channel_creation_handle
                                .session(&channel_creation_cap)
                                .give((((worker, channel.id), time), session_time, 1));

                            // Emit channel scope addresses
                            channel_scope_addrs_handle
                                .session(&channel_scope_addrs_cap)
                                .give((
                                    ((worker, channel.id), channel.scope_addr),
                                    session_time,
                                    1,
                                ));
                        }

                        TimelyEvent::PushProgress(_)
                        | TimelyEvent::Messages(_)
                        | TimelyEvent::Application(_)
                        | TimelyEvent::GuardedMessage(_)
                        | TimelyEvent::GuardedProgress(_)
                        | TimelyEvent::CommChannels(_)
                        | TimelyEvent::Input(_)
                        | TimelyEvent::Park(_)
                        | TimelyEvent::Text(_) => {}
                    }
                }
            });

            // FIXME: If every data source has completed, cut off any outstanding events to keep
            //        us from getting stuck in an infinite loop

            // Return our reactivation status, we want to be reactivated if we have any pending data
            !activation_map.is_empty() && !lifespan_map.is_empty()
        }
    });

    // TODO: Granulate the times within the operator
    let operator_names = operator_names_stream
        .as_collection()
        .delay(granulate)
        .arrange_by_key();
    let operator_ids = operator_ids_stream
        .as_collection()
        .delay(granulate)
        .arrange_by_key();
    let operator_addrs = operator_addrs_stream
        .as_collection()
        .delay(granulate)
        .arrange_by_key();
    let operator_addrs_by_self = operator_addrs_by_self_stream
        .as_collection()
        .delay(granulate)
        .arrange();
    let channel_scope_addrs = channel_scope_addrs_stream
        .as_collection()
        .delay(granulate)
        .arrange_by_key();
    let dataflow_ids = dataflow_ids_stream
        .as_collection()
        .delay(granulate)
        .arrange();

    // Granulate all streams and turn them into collections
    (
        lifespan_stream.as_collection().delay(granulate),
        activation_duration_stream.as_collection().delay(granulate),
        operator_creation_stream.as_collection().delay(granulate),
        channel_creation_stream.as_collection().delay(granulate),
        // FIXME: This isn't granulated since I have no idea what depends
        //       on the timestamp being the event time
        raw_channels_stream.as_collection(),
        // FIXME: This isn't granulated since I have no idea what depends
        //       on the timestamp being the event time
        raw_operators_stream.as_collection(),
        operator_names,
        operator_ids,
        operator_addrs,
        operator_addrs_by_self,
        channel_scope_addrs,
        dataflow_ids,
        // Note: Don't granulate this
        worker_events_stream,
    )
}

type ActivationTimes<S> = Collection<S, ((WorkerId, OperatorId), (Duration, Duration)), Diff>;

pub fn operator_stats<S>(
    scope: &mut S,
    activation_times: &ActivationTimes<S>,
) -> Collection<S, ((WorkerId, OperatorId), OperatorStats), Diff>
where
    S: Scope<Timestamp = Duration>,
{
    scope.region_named("Build Operator Stats", |region| {
        let activation_times = activation_times.enter(region);

        let aggregated_durations = activation_times.reduce(|_id, durations, output| {
            let durations: Vec<_> = durations
                .iter()
                .flat_map(|&(&duration, diff)| (0..diff).map(move |_| duration))
                .collect();

            output.push((durations, 1));
        });

        let execution_statistics =
            summation(&activation_times.map(|(operator, (_start, duration))| (operator, duration)));

        aggregated_durations
            .join_map(
                &execution_statistics,
                |&(worker, id),
                 activation_durations,
                 &Summation {
                     max,
                     min,
                     total,
                     average,
                     count: activations,
                 }| {
                    let stats = OperatorStats {
                        id,
                        worker,
                        max,
                        min,
                        average,
                        total,
                        activations,
                        activation_durations: activation_durations.to_owned(),
                        arrangement_size: None,
                    };

                    ((worker, id), stats)
                },
            )
            .leave_region()
    })
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Abomonation)]
pub struct OperatorStats {
    pub id: OperatorId,
    pub worker: WorkerId,
    pub max: Duration,
    pub min: Duration,
    pub average: Duration,
    pub total: Duration,
    pub activations: usize,
    /// Operator activation times `(start, duration)`
    pub activation_durations: Vec<(Duration, Duration)>,
    pub arrangement_size: Option<ArrangementStats>,
    // pub messages_sent: usize,
    // pub messages_received: usize,
}
