use crate::{
    dataflow::{
        constants::{DEFAULT_REACTIVATION_DELAY, IDLE_EXTRACTION_FUEL},
        operators::Fuel,
        utils::{granulate, Time},
        worker_timeline::{process_timely_event, EventProcessor, TimelineEventStream},
        ArrangedKey, ArrangedVal, ChannelId, Diff, OperatorAddr, OperatorId, TimelyLogBundle,
        WorkerId,
    },
    ui::Lifespan,
};
use ddshow_types::timely_logging::{ChannelsEvent, OperatesEvent, StartStop, TimelyEvent};
use differential_dataflow::{
    collection::AsCollection,
    lattice::Lattice,
    operators::arrange::{Arrange, ArrangeByKey},
    Collection,
};
use std::{
    collections::{HashMap, VecDeque},
    time::Duration,
};
use timely::{
    dataflow::{
        channels::{pact::Exchange, pushers::Tee},
        operators::{
            generic::{builder_rc::OperatorBuilder, OutputHandle, OutputWrapper},
            Capability,
        },
        Scope, ScopeParent, Stream,
    },
    progress::{frontier::MutableAntichain, Antichain},
    Data,
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
    // Timely event data, will be `None` if timeline analysis is disabled
    Option<TimelineEventStream<S>>,
);

// TODO: These could all emit `Present` difference types since there's no retractions here
pub(super) fn extract_timely_info<S>(
    scope: &mut S,
    timely_stream: &Stream<S, TimelyLogBundle>,
    disable_timeline: bool,
) -> TimelyCollections<S>
where
    S: Scope<Timestamp = Duration>,
{
    let (mut builder, mut output_counter) = (
        OperatorBuilder::new("Extract Operator Info".to_owned(), scope.clone()),
        0,
    );
    builder.set_notify(false);

    let operator_info = builder.operator_info();
    let activator = scope.activator_for(&operator_info.address);

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

    let builder = Builder::new(builder);
    let (mut outputs, streams) = Outputs::new(&mut builder);

    // Create all of the outputs
    let (mut lifespan_out, lifespan_stream, lifespan_idx) =
        new_output(&mut output_counter, &mut builder);
    let (mut activation_duration_out, activation_duration_stream, activation_duration_idx) =
        new_output(&mut output_counter, &mut builder);
    let (mut operator_creation_out, operator_creation_stream, operator_creation_idx) =
        new_output(&mut output_counter, &mut builder);
    let (mut channel_creation_out, channel_creation_stream, channel_creation_idx) =
        new_output(&mut output_counter, &mut builder);
    let (mut raw_channels_out, raw_channels_stream, raw_channels_idx) =
        new_output(&mut output_counter, &mut builder);
    let (mut raw_operators_out, raw_operators_stream, raw_operators_idx) =
        new_output(&mut output_counter, &mut builder);
    let (mut operator_names_out, operator_names_stream, operator_names_idx) =
        new_output(&mut output_counter, &mut builder);
    let (mut operator_ids_out, operator_ids_stream, operator_ids_idx) =
        new_output(&mut output_counter, &mut builder);
    let (mut operator_addrs_out, operator_addrs_stream, operator_addrs_idx) =
        new_output(&mut output_counter, &mut builder);
    let (mut operator_addrs_by_self_out, operator_addrs_by_self_stream, operator_addrs_by_self_idx) =
        new_output(&mut output_counter, &mut builder);
    let (mut channel_scope_addrs_out, channel_scope_addrs_stream, channel_scope_addrs_idx) =
        new_output(&mut output_counter, &mut builder);
    let (mut dataflow_ids_out, dataflow_ids_stream, dataflow_ids_idx) =
        new_output(&mut output_counter, &mut builder);
    let (mut worker_events_out, worker_events_stream, worker_events_idx) = if disable_timeline {
        (None, None, None)
    } else {
        let (out, stream, idx) = new_output(&mut output_counter, &mut builder);
        (Some(out), Some(stream), Some(idx))
    };

    builder.build(move |_capabilities| {
        let mut buffer = Vec::new();

        // TODO: Use stacks for these, migrate to something more like `EventProcessor`
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

        let mut work_list = VecDeque::new();
        let mut fuel = Fuel::limited(IDLE_EXTRACTION_FUEL);

        move |_frontiers| {
            // Activate all the outputs
            let handles = outputs.activate();

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
            let mut worker_events_handle = worker_events_out.as_mut().map(OutputWrapper::activate);

            timely_stream.for_each(|capability, data| {
                data.swap(&mut buffer);

                work_list.push_back((
                    buffer.clone(),
                    *capability.time(),
                    capability.delayed_for_output(capability.time(), lifespan_idx),
                ));

                for (time, worker, event) in buffer.drain(..) {

                    // Get the timestamp for the current event
                    let session_time = capability.time().join(&time);

                    // Get capabilities for every individual output
                    // let lifespan_cap = capability.delayed_for_output(&session_time, lifespan_idx);
                    let activation_duration_cap =
                        capability.delayed_for_output(&session_time, activation_duration_idx);
                    let operator_creation_cap =
                        capability.delayed_for_output(&session_time, operator_creation_idx);
                    let channel_creation_cap =
                        capability.delayed_for_output(&session_time, channel_creation_idx);
                    let raw_channels_cap =
                        capability.delayed_for_output(&session_time, raw_channels_idx);
                    let raw_operators_cap =
                        capability.delayed_for_output(&session_time, raw_operators_idx);
                    let operator_names_cap =
                        capability.delayed_for_output(&session_time, operator_names_idx);
                    let operator_ids_cap =
                        capability.delayed_for_output(&session_time, operator_ids_idx);
                    let operator_addrs_cap =
                        capability.delayed_for_output(&session_time, operator_addrs_idx);
                    let operator_addrs_by_self_cap =
                        capability.delayed_for_output(&session_time, operator_addrs_by_self_idx);
                    let channel_scope_addrs_cap =
                        capability.delayed_for_output(&session_time, channel_scope_addrs_idx);
                    let dataflow_ids_cap =
                        capability.delayed_for_output(&session_time, dataflow_ids_idx);

                    if let (Some(worker_events_handle), Some(worker_events_idx)) =
                        (worker_events_handle.as_mut(), worker_events_idx)
                    {
                        // Note: This has a different timestamp than the other capabilities
                        //       because of the machinery within `EventProcessor`
                        let worker_events_cap =
                            capability.delayed_for_output(capability.time(), worker_events_idx);

                        let mut event_processor = EventProcessor::new(
                            &mut event_map,
                            &mut map_buffer,
                            &mut stack_buffer,
                            worker_events_handle,
                            &worker_events_cap,
                            worker,
                            time,
                        );

                        process_timely_event(&mut event_processor, event.clone());
                    }

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
                            // if let Some(start_time) = lifespan_map.remove(&(worker, shutdown.id)) {
                            //     lifespan_handle.session(&lifespan_cap).give((
                            //         ((worker, shutdown.id), Lifespan::new(start_time, time)),
                            //         session_time,
                            //         1,
                            //     ));
                            // }
                            //
                            // // Remove any dangling activations
                            // activation_map.remove(&(worker, shutdown.id));
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

                activator.activate_after(DEFAULT_REACTIVATION_DELAY);
            });

            fuel.reset();

            'work_loop: while !fuel.is_exhausted() {
                if let Some((mut buffer, capability_time, mut lifespan_cap)) = work_list.pop_front()
                {
                    for (time, worker, event) in buffer.drain(..) {
                        let session_time = capability_time.join(&time);

                        lifespan_cap.downgrade(&session_time);

                        match event {
                            TimelyEvent::Operates(_) => {}
                            TimelyEvent::Channels(_) => {}
                            TimelyEvent::PushProgress(_) => {}
                            TimelyEvent::Messages(_) => {}
                            TimelyEvent::Schedule(_) => {}

                            TimelyEvent::Shutdown(shutdown) => {
                                if let Some(start_time) =
                                    lifespan_map.remove(&(worker, shutdown.id))
                                {
                                    lifespan_handle.session(&lifespan_cap).give((
                                        ((worker, shutdown.id), Lifespan::new(start_time, time)),
                                        session_time,
                                        1,
                                    ));
                                    fuel.exert(1);
                                }

                                // Remove any dangling activations
                                activation_map.remove(&(worker, shutdown.id));
                            }

                            TimelyEvent::Application(_) => {}
                            TimelyEvent::GuardedMessage(_) => {}
                            TimelyEvent::GuardedProgress(_) => {}
                            TimelyEvent::CommChannels(_) => {}
                            TimelyEvent::Input(_) => {}
                            TimelyEvent::Park(_) => {}
                            TimelyEvent::Text(_) => {}
                        }
                    }
                } else {
                    break 'work_loop;
                }
            }

            if !work_list.is_empty() {
                activator.activate_after(DEFAULT_REACTIVATION_DELAY);
            }

            // FIXME: If every data source has completed, cut off any outstanding events to keep
            //        us from getting stuck in an infinite loop

            // Return our reactivation status, we want to be reactivated if we have any pending data
            // dbg!(!has_been_activated && !work_list.is_empty() && !activation_map.is_empty() && !lifespan_map.is_empty())
        }
    });

    // TODO: Granulate the times within the operator
    let operator_names = operator_names_stream
        .as_collection()
        .delay(granulate)
        .arrange_by_key_named("ArrangeByKey: Operator Names");
    let operator_ids = operator_ids_stream
        .as_collection()
        .delay(granulate)
        .arrange_by_key_named("ArrangeByKey: Operator Ids");
    let operator_addrs = operator_addrs_stream
        .as_collection()
        .delay(granulate)
        .arrange_by_key_named("ArrangeByKey: Operator Addrs");
    let operator_addrs_by_self = operator_addrs_by_self_stream
        .as_collection()
        .delay(granulate)
        .arrange_named("Arrange: Operator Addrs by Self");
    let channel_scope_addrs = channel_scope_addrs_stream
        .as_collection()
        .delay(granulate)
        .arrange_by_key_named("ArrangeByKey: Channel Scope Addrs");
    let dataflow_ids = dataflow_ids_stream
        .as_collection()
        .delay(granulate)
        .arrange_named("Arrange: Dataflow Ids");

    // Granulate all streams and turn them into collections
    (
        lifespan_stream.as_collection().delay(granulate),
        activation_duration_stream.as_collection().delay(granulate),
        operator_creation_stream.as_collection().delay(granulate),
        channel_creation_stream.as_collection().delay(granulate),
        raw_channels_stream.as_collection().delay(granulate),
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

struct Builder<S>
where
    S: Scope,
{
    builder: OperatorBuilder<S>,
    output_idx: usize,
}

impl<S> Builder<S>
where
    S: Scope<Timestamp = Time>,
{
    fn new(builder: OperatorBuilder<S>) -> Self {
        Self {
            builder,
            output_idx: 0,
        }
    }

    fn new_output<T>(&mut self) -> (Output<T>, Stream<S, (T, Time, Diff)>)
    where
        T: Data,
    {
        let (wrapper, stream) = self.builder.new_output();

        let idx = self.output_idx;
        self.output_idx += 1;

        let output = Output::new(wrapper, idx);
        (output, stream)
    }

    fn build<B, L>(self, constructor: B)
    where
        B: FnOnce(Vec<Capability<Time>>) -> L,
        L: FnMut(&[MutableAntichain<Time>]) + 'static,
    {
        self.builder.build(constructor)
    }
}

struct Output<T>
where
    T: Data,
{
    /// The timely output wrapper
    wrapper: OutputWrapper<Time, (T, Time, Diff), Tee<Time, (T, Time, Diff)>>,
    /// The output's index
    idx: usize,
}

impl<T> Output<T>
where
    T: Data,
{
    fn new(
        wrapper: OutputWrapper<Time, (T, Time, Diff), Tee<Time, (T, Time, Diff)>>,
        idx: usize,
    ) -> Self {
        Self { wrapper, idx }
    }

    fn activate(&mut self) -> ActivatedOutput<'_, T> {
        ActivatedOutput::new(self.wrapper.activate(), self.idx)
    }
}

struct ActivatedOutput<'a, T>
where
    T: Data,
{
    handle: OutputHandle<'a, Time, (T, Time, Diff), Tee<Time, (T, Time, Diff)>>,
    idx: usize,
}

impl<'a, T> ActivatedOutput<'a, T>
where
    T: Data,
{
    fn new(
        handle: OutputHandle<'a, Time, (T, Time, Diff), Tee<Time, (T, Time, Diff)>>,
        idx: usize,
    ) -> Self {
        Self { handle, idx }
    }
}

macro_rules! timely_source_processor {
    ($($name:ident: $data:ty $(, if $cond:ident)?),* $(,)?) => {
        type Streams<S> = ($(Stream<S, ($data, Time, Diff)>,)*);

        struct Outputs {
            $($name: timely_source_processor!(@output_type $data, $($cond)?),)*
        }

        impl Outputs {
            fn new<S>(builder: &mut Builder<S>) -> (Self, Streams<S>)
            where
                S: Scope<Timestamp = Time>,
            {
                $(let $name = builder.new_output::<$data>();)*

                let streams = ($($name.1,)*);
                let outputs = Self {
                    $($name: $name.0,)*
                };

                (outputs, streams)
            }

            fn activate(&mut self) -> OutputHandles<'_> {
                OutputHandles::new($(self.$name.activate(),)*)
            }
        }

        struct OutputHandles<'a> {
            $($name: ActivatedOutput<'a, $data>,)*
        }

        impl<'a> OutputHandles<'a> {
            fn new($($name: ActivatedOutput<'a, $data>,)*) -> Self {
                Self {
                    $($name,)*
                }
            }
        }
    };

    (@output_type $data:ty, $cond:ident) => {
        Option<Output<$data>>
    };

    (@output_type $data:ty,) => {
        Output<$data>
    };
}

timely_source_processor! {
    lifespans: ((WorkerId, OperatorId), Lifespan),
    activation_durations: ((WorkerId, OperatorId), (Duration, Duration)),
    operator_creations: ((WorkerId, OperatorId), Duration),
    channel_creations: ((WorkerId, ChannelId), Duration),
    raw_channels: (WorkerId, ChannelsEvent),
    raw_operators: (WorkerId, OperatesEvent),
    operator_names: ((WorkerId, OperatorId), String),
    operator_ids: ((WorkerId, OperatorId), OperatorAddr),
    operator_addrs: ((WorkerId, OperatorAddr), OperatorId),
    operator_addrs_by_self: (WorkerId, ChannelId),
    channel_scope_addrs: ((WorkerId, ChannelId), OperatorAddr),
    dataflow_ids: (WorkerId, OperatorId),
}

type OutputTuple<S, D> = (
    OutputWrapper<<S as ScopeParent>::Timestamp, D, Tee<<S as ScopeParent>::Timestamp, D>>,
    Stream<S, D>,
    usize,
);

fn new_output<S, D>(id_counter: &mut usize, builder: &mut OperatorBuilder<S>) -> OutputTuple<S, D>
where
    S: Scope,
    D: Data,
{
    let output_id = *id_counter;
    *id_counter += 1;

    let (output, stream) = builder.new_output();

    (output, stream, output_id)
}
