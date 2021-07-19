use crate::{
    dataflow::{
        constants::IDLE_EXTRACTION_FUEL,
        operators::{DelayExt, Fuel},
        utils::{granulate, Time},
        worker_timeline::{process_timely_event, EventMap, EventProcessor},
        ArrangedKey, ArrangedVal, ChannelId, Diff, OperatorAddr, OperatorId, TimelineEvent,
        TimelyLogBundle, WorkerId,
    },
    ui::Lifespan,
};
use ddshow_types::timely_logging::{ChannelsEvent, OperatesEvent, StartStop, TimelyEvent};
use differential_dataflow::{
    collection::AsCollection,
    difference::{Present, Semigroup},
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
        channels::{
            pact::Exchange,
            pushers::{buffer::Session, counter::Counter as PushCounter, Tee},
        },
        operators::{
            generic::{builder_rc::OperatorBuilder, OutputHandle, OutputWrapper},
            Capability, CapabilityRef,
        },
        Scope, Stream,
    },
    progress::frontier::MutableAntichain,
    Data,
};

// TODO: So much refactoring

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
    Option<Collection<S, TimelineEvent, Present>>,
);

type WorkList = VecDeque<(Vec<TimelyLogBundle>, Duration, OutputCapabilities)>;

// TODO: These could all emit `Present` difference types since there's no retractions here
pub(super) fn extract_timely_info<S>(
    scope: &mut S,
    timely_stream: &Stream<S, TimelyLogBundle>,
    disable_timeline: bool,
) -> TimelyCollections<S>
where
    S: Scope<Timestamp = Duration>,
{
    let mut builder = OperatorBuilder::new("Extract Operator Info".to_owned(), scope.clone());
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
        // and unbalanced numbers of workers to ensure work is fairly divided.
        //
        // As a greater concern (and the reason why this continues to exist here), events
        // are keyed with their worker id, meaning that if an end event goes to a worker
        // where the input event wasn't previously received then the input event won't
        // be closed off properly
        Exchange::new(|(_, worker, _): &TimelyLogBundle| worker.into_inner() as u64),
    );

    let mut builder = Builder::new(builder);
    let (mut outputs, streams) = Outputs::new(&mut builder, !disable_timeline);

    builder.build(move |_capabilities| {
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
        let mut work_list_buffers = Vec::new();
        let mut fuel = Fuel::limited(IDLE_EXTRACTION_FUEL);

        move |_frontiers| {
            // Activate all the outputs
            let mut handles = outputs.activate();

            timely_stream.for_each(|capability, data| {
                let mut buffer = work_list_buffers.pop().unwrap_or_default();
                data.swap(&mut buffer);

                work_list.push_back((
                    // TODO: Keep some extra buffers around
                    buffer,
                    *capability.time(),
                    handles.retain(capability),
                ));
            });

            work_loop(
                &mut fuel,
                &mut handles,
                &mut lifespan_map,
                &mut activation_map,
                &mut work_list,
                &mut work_list_buffers,
                &mut event_map,
                &mut map_buffer,
                &mut stack_buffer,
            );

            // If we have any work left to do, reactivate ourselves
            if !work_list.is_empty() {
                activator.activate();
            }

            // FIXME: If every data source has completed, cut off any outstanding events to keep
            //        us from getting stuck in an infinite loop
        }
    });

    let Collections {
        lifespans,
        activation_durations,
        operator_creations,
        channel_creations,
        raw_channels,
        raw_operators,
        operator_names,
        operator_ids,
        operator_addrs,
        operator_addrs_by_self,
        channel_scope_addrs,
        dataflow_ids,
        worker_events,
    } = streams.into_collections();

    // TODO: Granulate the times within the operator
    let operator_names = operator_names.arrange_by_key_named("ArrangeByKey: Operator Names");
    let operator_ids = operator_ids.arrange_by_key_named("ArrangeByKey: Operator Ids");
    let operator_addrs = operator_addrs.arrange_by_key_named("ArrangeByKey: Operator Addrs");
    let operator_addrs_by_self =
        operator_addrs_by_self.arrange_named("Arrange: Operator Addrs by Self");
    let channel_scope_addrs =
        channel_scope_addrs.arrange_by_key_named("ArrangeByKey: Channel Scope Addrs");
    let dataflow_ids = dataflow_ids.arrange_named("Arrange: Dataflow Ids");

    // Granulate all streams and turn them into collections
    (
        lifespans,
        activation_durations,
        operator_creations,
        channel_creations,
        raw_channels,
        // FIXME: This isn't granulated since I have no idea what depends
        //       on the timestamp being the event time
        raw_operators,
        operator_names,
        operator_ids,
        operator_addrs,
        operator_addrs_by_self,
        channel_scope_addrs,
        dataflow_ids,
        // Note: Don't granulate this
        worker_events,
    )
}
#[allow(clippy::too_many_arguments)]
fn work_loop(
    fuel: &mut Fuel,
    handles: &mut OutputHandles,
    lifespan_map: &mut HashMap<(WorkerId, OperatorId), Duration>,
    activation_map: &mut HashMap<(WorkerId, OperatorId), Duration>,
    work_list: &mut WorkList,
    work_list_buffers: &mut Vec<Vec<TimelyLogBundle>>,
    event_map: &mut EventMap,
    map_buffer: &mut EventMap,
    stack_buffer: &mut Vec<Vec<Duration>>,
) {
    // Reset our fuel on each activation
    fuel.reset();

    'work_loop: while !fuel.is_exhausted() {
        if let Some((mut buffer, capability_time, mut capabilities)) = work_list.pop_front() {
            fuel.exert(buffer.len());

            for (time, worker, event) in buffer.drain(..) {
                if let (Some(worker_events), Some(capability)) = (
                    handles.worker_events.as_mut(),
                    capabilities.worker_events.as_ref(),
                ) {
                    let mut event_processor = EventProcessor::new(
                        event_map,
                        map_buffer,
                        stack_buffer,
                        &mut worker_events.handle,
                        capability,
                        worker,
                        time,
                    );

                    process_timely_event(&mut event_processor, event.clone());
                }

                // Get the timestamp for the current event
                let session_time = capability_time.join(&time);
                capabilities.downgrade(&session_time);

                ingest_event(
                    time,
                    worker,
                    event,
                    session_time,
                    handles,
                    &capabilities,
                    lifespan_map,
                    activation_map,
                );
            }

            // // If we didn't have enough fuel to fully deplete the buffer, push it back onto the work list
            // if !buffer.is_empty() {
            //     work_list.push_front((buffer, capability_time, capabilities));
            //
            // // Otherwise add it to the extra buffers we maintain
            // } else {
            //     work_list_buffers.push(buffer);
            // }
            work_list_buffers.push(buffer);
        } else {
            break 'work_loop;
        }
    }

    // Keep our memory usage somewhat under control
    // TODO: Factor out into a function or method
    {
        if lifespan_map.capacity() >= 128 && lifespan_map.capacity() > lifespan_map.len() * 4 {
            tracing::trace!(
                "shrank lifespan map from a capacity of {} to {}",
                lifespan_map.capacity(),
                lifespan_map.len(),
            );

            lifespan_map.shrink_to_fit();
        }

        if activation_map.capacity() >= 128 && activation_map.capacity() > activation_map.len() * 4
        {
            tracing::trace!(
                "shrank activation map from a capacity of {} to {}",
                activation_map.capacity(),
                activation_map.len(),
            );

            activation_map.shrink_to_fit();
        }

        if work_list.capacity() >= 128 && work_list.capacity() > work_list.len() * 4 {
            tracing::trace!(
                "shrank work list from a capacity of {} to {}",
                work_list.capacity(),
                work_list.len(),
            );

            work_list.shrink_to_fit();
        }

        if work_list_buffers.capacity() >= 128
            && work_list_buffers.capacity() > work_list_buffers.len() * 4
        {
            tracing::trace!(
                "shrank work list buffers from a capacity of {} to {}",
                work_list_buffers.capacity(),
                work_list_buffers.len(),
            );

            work_list_buffers.shrink_to_fit();
        }

        if event_map.capacity() >= 128 && event_map.capacity() > event_map.len() * 4 {
            tracing::trace!(
                "shrank event map from a capacity of {} to {}",
                event_map.capacity(),
                event_map.len(),
            );

            event_map.shrink_to_fit();
        }

        if map_buffer.capacity() >= 128 && map_buffer.capacity() > map_buffer.len() * 4 {
            tracing::trace!(
                "shrank map buffer from a capacity of {} to {}",
                map_buffer.capacity(),
                map_buffer.len(),
            );

            map_buffer.shrink_to_fit();
        }

        stack_buffer.retain(|buffer| buffer.capacity() >= 128);
        if stack_buffer.capacity() >= 128 && stack_buffer.capacity() > stack_buffer.len() * 4 {
            tracing::trace!(
                "shrank stack buffer from a capacity of {} to {}",
                stack_buffer.capacity(),
                stack_buffer.len(),
            );

            stack_buffer.shrink_to_fit();
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn ingest_event(
    time: Duration,
    worker: WorkerId,
    event: TimelyEvent,
    session_time: Duration,
    handles: &mut OutputHandles,
    capabilities: &OutputCapabilities,
    lifespan_map: &mut HashMap<(WorkerId, OperatorId), Duration>,
    activation_map: &mut HashMap<(WorkerId, OperatorId), Duration>,
) {
    match event {
        TimelyEvent::Operates(operates) => {
            lifespan_map.insert((worker, operates.id), time);

            // Emit raw operator events
            handles
                .raw_operators
                .session(&capabilities.raw_operators)
                .give(((worker, operates.clone()), session_time, 1));

            // Emit operator creation times
            handles
                .operator_creations
                .session(&capabilities.operator_creations)
                .give((((worker, operates.id), time), session_time, 1));

            // Emit operator names
            handles
                .operator_names
                .session(&capabilities.operator_names)
                .give((((worker, operates.id), operates.name), session_time, 1));

            // Emit dataflow ids
            if operates.addr.is_top_level() {
                handles
                    .dataflow_ids
                    .session(&capabilities.dataflow_ids)
                    .give((((worker, operates.id), ()), session_time, 1));
            }

            // Emit operator ids
            handles
                .operator_ids
                .session(&capabilities.operator_ids)
                .give((
                    ((worker, operates.id), operates.addr.clone()),
                    session_time,
                    1,
                ));

            // Emit operator addresses
            handles
                .operator_addrs
                .session(&capabilities.operator_addrs)
                .give((
                    ((worker, operates.addr.clone()), operates.id),
                    session_time,
                    1,
                ));
            handles
                .operator_addrs_by_self
                .session(&capabilities.operator_addrs_by_self)
                .give((((worker, operates.addr), ()), session_time, 1));
        }

        TimelyEvent::Shutdown(shutdown) => {
            if let Some(start_time) = lifespan_map.remove(&(worker, shutdown.id)) {
                handles.lifespans.session(&capabilities.lifespans).give((
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
                    if let Some(start_time) = activation_map.remove(&(worker, operator)) {
                        let duration = time - start_time;
                        handles
                            .activation_durations
                            .session(&capabilities.activation_durations)
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
            handles
                .raw_channels
                .session(&capabilities.raw_channels)
                .give(((worker, channel.clone()), session_time, 1));

            // Emit channel creation times
            handles
                .channel_creations
                .session(&capabilities.channel_creations)
                .give((((worker, channel.id), time), session_time, 1));

            // Emit channel scope addresses
            handles
                .channel_scope_addrs
                .session(&capabilities.channel_scope_addrs)
                .give((((worker, channel.id), channel.scope_addr), session_time, 1));
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

type Bundle<T, R = Diff> = (T, Time, R);
type SessionPusher<T, R = Diff> = PushCounter<Time, Bundle<T, R>, Tee<Time, Bundle<T, R>>>;
type OutWrap<T, R> = OutputWrapper<Time, Bundle<T, R>, Tee<Time, Bundle<T, R>>>;
type OutHandle<'a, T, R> = OutputHandle<'a, Time, Bundle<T, R>, Tee<Time, Bundle<T, R>>>;

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

    fn new_output<T, R>(&mut self) -> (Output<T, R>, Stream<S, Bundle<T, R>>)
    where
        T: Data,
        R: Semigroup,
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

struct Output<T, R = Diff>
where
    T: Data,
    R: Semigroup,
{
    /// The timely output wrapper
    wrapper: OutWrap<T, R>,
    /// The output's index
    idx: usize,
}

impl<T, R> Output<T, R>
where
    T: Data,
    R: Semigroup,
{
    fn new(wrapper: OutWrap<T, R>, idx: usize) -> Self {
        Self { wrapper, idx }
    }

    fn activate(&mut self) -> ActivatedOutput<'_, T, R> {
        ActivatedOutput::new(self.wrapper.activate(), self.idx)
    }
}

struct ActivatedOutput<'a, T, R = Diff>
where
    T: Data,
    R: Semigroup,
{
    handle: OutHandle<'a, T, R>,
    idx: usize,
}

impl<'a, T, R> ActivatedOutput<'a, T, R>
where
    T: Data,
    R: Semigroup,
{
    fn new(handle: OutHandle<'a, T, R>, idx: usize) -> Self {
        Self { handle, idx }
    }

    pub fn session<'b>(
        &'b mut self,
        capability: &'b Capability<Time>,
    ) -> Session<'b, Time, Bundle<T, R>, SessionPusher<T, R>>
    where
        'a: 'b,
    {
        self.handle.session(capability)
    }
}

macro_rules! timely_source_processor {
    ($($name:ident: $data:ty $(; if $cond:ident)? $(= $diff:ty)?),* $(,)?) => {
        struct Streams<S>
        where
            S: Scope<Timestamp = Time>,
        {
            $($name: timely_source_processor!(@stream $data, timely_source_processor!(@diff $($diff)?), $($cond)?),)*
        }

        impl<S> Streams<S>
        where
            S: Scope<Timestamp = Time>,
        {
            fn into_collections(self) -> Collections<S> {
                Collections {
                    $($name: timely_source_processor!(@as_collection self, $name, $($cond)?),)*
                }
            }
        }

        struct Collections<S>
        where
            S: Scope<Timestamp = Time>,
        {
            $($name: timely_source_processor!(@collection $data, timely_source_processor!(@diff $($diff)?), $($cond)?),)*
        }

        struct Outputs {
            $($name: timely_source_processor!(@output_type $data, timely_source_processor!(@diff $($diff)?), $($cond)?),)*
        }

        impl Outputs {
            fn new<S>(builder: &mut Builder<S>, $($($cond: bool,)?)*) -> (Self, Streams<S>)
            where
                S: Scope<Timestamp = Time>,
            {
                $(timely_source_processor!(@make_output builder, $name, $data, timely_source_processor!(@diff $($diff)?), $($cond)?);)*

                let streams = Streams {
                    $($name: $name.1,)*
                };
                let outputs = Self {
                    $($name: $name.0,)*
                };

                (outputs, streams)
            }

            fn activate(&mut self) -> OutputHandles<'_> {
                OutputHandles::new($(timely_source_processor!(@activate self, $name, $($cond)?),)*)
            }
        }

        struct OutputHandles<'a> {
            $($name: timely_source_processor!(@handle $data, timely_source_processor!(@diff $($diff)?), $($cond)?),)*
        }

        impl<'a> OutputHandles<'a> {
            #[allow(clippy::too_many_arguments)]
            fn new($($name: timely_source_processor!(@handle $data, timely_source_processor!(@diff $($diff)?), $($cond)?),)*) -> Self {
                Self {
                    $($name,)*
                }
            }

            fn retain(&self, capability: CapabilityRef<'_, Time>) -> OutputCapabilities {
                OutputCapabilities::new($(timely_source_processor!(@retain self, capability, $name, $($cond)?),)*)
            }
        }

        struct OutputCapabilities {
            $($name: timely_source_processor!(@capability $data, $($cond)?),)*
        }

        impl OutputCapabilities {
            #[allow(clippy::too_many_arguments)]
            fn new($($name: timely_source_processor!(@capability $data, $($cond)?),)*) -> Self {
                Self {
                    $($name,)*
                }
            }

            fn downgrade(&mut self, time: &Time) {
                $(timely_source_processor!(@downgrade self, time, $name, $($cond)?);)*
            }

            #[allow(unused_must_use)]
            fn time(&self) -> Time {
                let x = $(&self.$name;)*
                *x.time()
            }
        }

        impl Drop for OutputCapabilities {
            fn drop(&mut self) {
                tracing::trace!(
                    target: "timely_source_capabilities",
                    $($name = ?self.$name,)*
                    "dropped output capability for timestamp {:#?}",
                    self.time(),
                );
            }
        }
    };

    (@diff) => { Diff };
    (@diff $diff:ty) => { $diff };

    (@output_type $data:ty, $diff:ty, $cond:ident) => {
        Option<Output<$data, $diff>>
    };

    (@output_type $data:ty, $diff:ty,) => {
        Output<$data, $diff>
    };

    (@make_output $builder:ident, $name:ident, $data:ty, $diff:ty, $cond:ident) => {
        let $name = if $cond {
            let (output, stream) = $builder.new_output::<$data, $diff>();

            (Some(output), Some(stream))
        } else {
            (None, None)
        };
    };

    (@make_output $builder:ident, $name:ident, $data:ty, $diff:ty,) => {
        let $name = $builder.new_output::<$data, $diff>();
    };

    (@capability $data:ty, $cond:ident) => {
        Option<Capability<Time>>
    };

    (@capability $data:ty,) => {
        Capability<Time>
    };

    (@retain $self:ident, $capability:ident, $name:ident, $cond:ident) => {
        $self
            .$name
            .as_ref()
            .map(|$name| $capability.delayed_for_output($capability.time(), $name.idx))
    };

    (@retain $self:ident, $capability:ident, $name:ident,) => {
        $capability.delayed_for_output($capability.time(), $self.$name.idx)
    };

    (@downgrade $self:ident, $time:ident, $name:ident, $cond:ident) => {
        if let Some($name) = $self.$name.as_mut() {
            $name.downgrade($time);
        }
    };

    (@downgrade $self:ident, $time:ident, $name:ident,) => {
        $self.$name.downgrade($time);
    };

    (@handle $data:ty, $diff:ty, $cond:ident) => {
        Option<ActivatedOutput<'a, $data, $diff>>
    };

    (@handle $data:ty, $diff:ty,) => {
        ActivatedOutput<'a, $data, $diff>
    };

    (@activate $self:ident, $name:ident, $cond:ident) => {
        $self.$name.as_mut().map(|$name| $name.activate())
    };

    (@activate $self:ident, $name:ident,) => {
        $self.$name.activate()
    };

    (@stream $data:ty, $diff:ty, $cond:ident) => {
        Option<Stream<S, ($data, Time, $diff)>>
    };

    (@stream $data:ty, $diff:ty,) => {
        Stream<S, ($data, Time, $diff)>
    };

    (@collection $data:ty, $diff:ty, $cond:ident) => {
        Option<Collection<S, $data, $diff>>
    };

    (@collection $data:ty, $diff:ty,) => {
        Collection<S, $data, $diff>
    };

    (@as_collection $self:ident, $name:ident, $cond:ident) => {
        $self.$name.map(|$name| $name.as_collection().delay_fast(granulate))
    };

    (@as_collection $self:ident, $name:ident,) => {
        $self.$name.as_collection().delay_fast(granulate)
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
    operator_addrs_by_self: ((WorkerId, OperatorAddr), ()),
    channel_scope_addrs: ((WorkerId, ChannelId), OperatorAddr),
    dataflow_ids: ((WorkerId, OperatorId), ()),
    worker_events: TimelineEvent; if timeline_enabled = Present,
}
