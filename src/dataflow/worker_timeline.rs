use crate::dataflow::{
    constants::EVENT_NS_MARGIN,
    operators::{FilterSplit, Multiply, Split},
    Diff, DifferentialLogBundle,
};
use abomonation_derive::Abomonation;
use ddshow_types::{
    differential_logging::DifferentialEvent,
    timely_logging::{ParkEvent, StartStop, TimelyEvent},
    OperatorId, WorkerId,
};
use differential_dataflow::{
    algorithms::identifiers::Identifiers,
    difference::Abelian,
    lattice::Lattice,
    operators::{
        arrange::{ArrangeByKey, Arranged},
        JoinCore, Threshold,
    },
    trace::TraceReader,
    AsCollection, Collection, ExchangeData,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, iter, mem, time::Duration};
use timely::dataflow::{
    channels::{pact::Pipeline, pushers::Tee},
    operators::{
        aggregation::StateMachine, generic::OutputHandle, Capability, Concat, Delay, Enter, Map,
        Operator,
    },
    Scope, Stream,
};

pub(super) fn worker_timeline<S, Trace>(
    scope: &mut S,
    differential_stream: Option<&Stream<S, DifferentialLogBundle>>,
    operator_names: &Arranged<S, Trace>,
    timely_events: &Stream<S, (EventData, Duration, Diff)>,
) -> Collection<S, WorkerTimelineEvent, Diff>
where
    S: Scope<Timestamp = Duration>,
    Trace: TraceReader<Key = (WorkerId, OperatorId), Val = String, Time = Duration, R = Diff>
        + Clone
        + 'static,
{
    scope.region_named("Collect Worker Timelines", |region| {
        let (differential_stream, timely_events) = (
            differential_stream.map(|stream| stream.enter(region)),
            timely_events.enter(region),
        );

        // TODO: Emit trace drops & shares to a separate stream so that we can make markers
        //       with `timeline.setCustomTime()`
        // TODO: Emit the # of batches received
        let differential_events =
            differential_stream.map(|event_stream| collect_differential_events(&event_stream));

        let partial_events = differential_events
            .as_ref()
            .map(|differential_events| timely_events.concat(differential_events))
            .unwrap_or(timely_events)
            .as_collection()
            .distinct_core::<Diff>()
            .identifiers();

        let (needs_operators, finished) = partial_events.filter_split(
            |(
                EventData {
                    worker,
                    partial_event,
                    start_time,
                    duration,
                },
                event_id,
            )| {
                let timeline_event = WorkerTimelineEvent {
                    event_id,
                    worker,
                    event: partial_event.into(),
                    duration: duration.as_nanos() as u64,
                    start_time: start_time.as_nanos() as u64,
                    collapsed_events: 1,
                };

                if let Some(operator_id) = partial_event.operator_id() {
                    (Some(((worker, operator_id), timeline_event)), None)
                } else {
                    (None, Some(timeline_event))
                }
            },
        );

        // FIXME: Add errors for `needs_operators` that fail the join
        let events = needs_operators
            .arrange_by_key()
            .join_core(&operator_names.enter_region(region), |_id, event, name| {
                let mut event = event.to_owned();
                *event.event.operator_name_mut().unwrap() = name.to_owned();

                iter::once(event)
            })
            .concat(&finished);

        // collapse_events(&events)
        events.leave_region()
    })
}

type TimelineStreamEvent = (EventData, Duration, Diff);
pub(super) type TimelineEventStream<S> = Stream<S, TimelineStreamEvent>;
type EventMap = HashMap<(WorkerId, EventKind), Vec<(Duration, Capability<Duration>)>>;
type EventOutput<'a> =
    OutputHandle<'a, Duration, TimelineStreamEvent, Tee<Duration, TimelineStreamEvent>>;

pub(super) fn process_timely_event(
    event_processor: &mut EventProcessor<'_, '_>,
    event: TimelyEvent,
) {
    match event {
        TimelyEvent::Schedule(schedule) => {
            let event_kind = EventKind::activation(schedule.id);
            let partial_event = PartialTimelineEvent::activation(schedule.id);

            event_processor.start_stop(event_kind, partial_event, schedule.start_stop);
        }

        TimelyEvent::Application(app) => {
            let event_kind = EventKind::application(app.id);
            let partial_event = PartialTimelineEvent::Application;

            event_processor.is_start(event_kind, partial_event, app.is_start);
        }

        TimelyEvent::GuardedMessage(message) => {
            let event_kind = EventKind::Message;
            let partial_event = PartialTimelineEvent::Message;

            event_processor.is_start(event_kind, partial_event, message.is_start);
        }

        TimelyEvent::GuardedProgress(progress) => {
            let event_kind = EventKind::Progress;
            let partial_event = PartialTimelineEvent::Progress;

            event_processor.is_start(event_kind, partial_event, progress.is_start);
        }

        TimelyEvent::Input(input) => {
            let event_kind = EventKind::Input;
            let partial_event = PartialTimelineEvent::Input;

            event_processor.start_stop(event_kind, partial_event, input.start_stop);
        }

        TimelyEvent::Park(park) => {
            let event_kind = EventKind::Park;

            match park {
                ParkEvent::Park(_) => event_processor.insert(event_kind),
                ParkEvent::Unpark => {
                    event_processor.remove(event_kind, PartialTimelineEvent::Parked);
                }
            }
        }

        // When an operator shuts down, release all capabilities associated with it.
        // This works to counteract dataflow stalling
        TimelyEvent::Shutdown(shutdown) => event_processor.remove_referencing(shutdown.id),

        TimelyEvent::Operates(_)
        | TimelyEvent::Channels(_)
        | TimelyEvent::PushProgress(_)
        | TimelyEvent::Messages(_)
        | TimelyEvent::CommChannels(_)
        | TimelyEvent::Text(_) => {}
    }
}

// TODO: Wire operator shutdown events into this as well
pub(super) fn collect_differential_events<S>(
    event_stream: &Stream<S, DifferentialLogBundle>,
) -> TimelineEventStream<S>
where
    S: Scope<Timestamp = Duration>,
{
    event_stream.unary(
        Pipeline,
        "Gather Differential Event Durations",
        |_capability, _info| {
            let mut buffer = Vec::new();
            let (mut event_map, mut map_buffer, mut stack_buffer) =
                (HashMap::new(), HashMap::new(), Vec::new());

            move |input, output| {
                input.for_each(|capability, data| {
                    let capability = capability.retain();
                    data.swap(&mut buffer);

                    for (time, worker, event) in buffer.drain(..) {
                        let mut event_processor = EventProcessor::new(
                            &mut event_map,
                            &mut map_buffer,
                            &mut stack_buffer,
                            output,
                            &capability,
                            worker,
                            time,
                        );

                        process_differential_event(&mut event_processor, event);
                    }
                });
            }
        },
    )
}

fn process_differential_event(
    event_processor: &mut EventProcessor<'_, '_>,
    event: DifferentialEvent,
) {
    match event {
        DifferentialEvent::Merge(merge) => {
            let event_kind = EventKind::merge(merge.operator);
            let partial_event = PartialTimelineEvent::merge(merge.operator);
            let is_start = merge.complete.is_none();

            event_processor.is_start(event_kind, partial_event, is_start);
        }

        DifferentialEvent::MergeShortfall(shortfall) => {
            let event_kind = EventKind::merge(shortfall.operator);
            let partial_event = PartialTimelineEvent::merge(shortfall.operator);

            event_processor.remove(event_kind, partial_event);
        }

        // Sometimes merges don't complete since they're dropped part way through
        DifferentialEvent::Drop(drop) => event_processor.remove_referencing(drop.operator),

        DifferentialEvent::Batch(_) | DifferentialEvent::TraceShare(_) => {}
    }
}

pub(super) struct EventProcessor<'a, 'b> {
    event_map: &'a mut EventMap,
    map_buffer: &'a mut EventMap,
    stack_buffer: &'a mut Vec<Vec<(Duration, Capability<Duration>)>>,
    output: &'a mut EventOutput<'b>,
    capability: &'a Capability<Duration>,
    worker: WorkerId,
    time: Duration,
}

impl<'a, 'b> EventProcessor<'a, 'b> {
    pub(super) fn new(
        event_map: &'a mut EventMap,
        map_buffer: &'a mut EventMap,
        stack_buffer: &'a mut Vec<Vec<(Duration, Capability<Duration>)>>,
        output: &'a mut EventOutput<'b>,
        capability: &'a Capability<Duration>,
        worker: WorkerId,
        time: Duration,
    ) -> Self {
        Self {
            event_map,
            map_buffer,
            stack_buffer,
            output,
            capability,
            worker,
            time,
        }
    }

    fn insert(&mut self, event_kind: EventKind) {
        let Self {
            event_map,
            stack_buffer,
            worker,
            time,
            capability,
            ..
        } = self;

        event_map
            .entry((*worker, event_kind))
            .or_insert_with(|| stack_buffer.pop().unwrap_or_else(Vec::new))
            .push((*time, capability.clone()));
    }

    fn remove(&mut self, event_kind: EventKind, partial_event: PartialTimelineEvent) {
        if let Some((start_time, stored_capability)) = self
            .event_map
            .get_mut(&(self.worker, event_kind))
            .and_then(Vec::pop)
        {
            self.output_event(start_time, stored_capability, partial_event)
        } else {
            tracing::warn!("attempted to remove event that was never started");
        }
    }

    fn output_event(
        &mut self,
        start_time: Duration,
        stored_capability: Capability<Duration>,
        partial_event: PartialTimelineEvent,
    ) {
        Self::output_event_inner(
            self.output,
            self.time,
            start_time,
            &self.capability,
            stored_capability,
            partial_event,
            self.worker,
        )
    }

    /// The inner workings of `self.output_event()`, abstracted away so that it can be used within
    /// `self.remove_referencing()`
    fn output_event_inner(
        output: &mut EventOutput,
        current_time: Duration,
        start_time: Duration,
        current_capability: &Capability<Duration>,
        mut stored_capability: Capability<Duration>,
        partial_event: PartialTimelineEvent,
        worker: WorkerId,
    ) {
        let duration = current_time - start_time;
        stored_capability.downgrade(&stored_capability.time().join(current_capability.time()));

        output.session(&stored_capability).give((
            EventData::new(worker, partial_event, start_time, duration),
            *stored_capability.time(),
            1,
        ));
    }

    fn start_stop(
        &mut self,
        event_kind: EventKind,
        partial_event: PartialTimelineEvent,
        start_stop: StartStop,
    ) {
        match start_stop {
            StartStop::Start => self.insert(event_kind),
            StartStop::Stop => self.remove(event_kind, partial_event),
        }
    }

    fn is_start(
        &mut self,
        event_kind: EventKind,
        partial_event: PartialTimelineEvent,
        is_start: bool,
    ) {
        self.start_stop(
            event_kind,
            partial_event,
            if is_start {
                StartStop::Start
            } else {
                StartStop::Stop
            },
        )
    }

    /// Remove all events that reference the given operator id,
    /// releasing their associated capabilities
    fn remove_referencing(&mut self, operator: OperatorId) {
        mem::swap(self.event_map, self.map_buffer);

        let mut removed_refs = 0;
        for ((worker, event_kind), mut value_stack) in self.map_buffer.drain() {
            match event_kind {
                // If the event doesn't reference the operator id, release all associated capabilities
                EventKind::OperatorActivation { operator_id }
                | EventKind::Merge { operator_id }
                    if operator_id == operator =>
                {
                    let partial_event = match event_kind {
                        EventKind::OperatorActivation { operator_id } => {
                            PartialTimelineEvent::activation(operator_id)
                        }
                        EventKind::Merge { operator_id } => {
                            PartialTimelineEvent::merge(operator_id)
                        }

                        _ => unreachable!(),
                    };

                    // Drain the value stack, sending all dangling events
                    for (start_time, stored_capability) in value_stack.drain(..) {
                        Self::output_event_inner(
                            self.output,
                            self.time,
                            start_time,
                            &self.capability,
                            stored_capability,
                            partial_event,
                            self.worker,
                        )
                    }

                    // Save the value stack by stashing it into the stack buffer
                    self.stack_buffer.push(value_stack);

                    removed_refs += 1;
                }

                // If the event doesn't reference the operator id, insert it back into the event map
                EventKind::OperatorActivation { .. }
                | EventKind::Merge { .. }
                | EventKind::Message
                | EventKind::Progress
                | EventKind::Input
                | EventKind::Park
                | EventKind::Application { .. } => {
                    self.event_map.insert((worker, event_kind), value_stack);
                }
            }
        }

        if removed_refs != 0 {
            tracing::warn!(
                operator = %operator,
                removed_refs = removed_refs,
                "removed {} dangling event{} pointing to a dropped operator",
                removed_refs,
                if removed_refs == 1 { "" } else { "s" },
            );
        }
    }
}

// TODO: This may be slightly unreliable
#[allow(dead_code)]
fn collapse_events<S, R>(
    events: &Collection<S, WorkerTimelineEvent, R>,
) -> Collection<S, WorkerTimelineEvent, R>
where
    S: Scope<Timestamp = Duration>,
    S::Timestamp: Lattice,
    R: Abelian + ExchangeData + Multiply<Output = R> + From<i8>,
{
    fn fold_timeline_events(
        _key: &WorkerId,
        input: State,
        state: &mut Option<WorkerTimelineEvent>,
    ) -> (
        bool,
        impl IntoIterator<Item = WorkerTimelineEvent> + 'static,
    ) {
        match input {
            State::Event(input) => {
                (
                    false,
                    match state {
                        state @ None => {
                            *state = Some(input);
                            None
                        }

                        Some(old_state) => {
                            let (state_start, input_start) =
                                (old_state.start_time, input.start_time);
                            let (state_end, input_end) = (
                                old_state.start_time + old_state.duration,
                                input.start_time + input.duration,
                            );

                            // Make sure the events are the same and are also overlapping
                            // in their time windows (`event_start..event_end`) by using
                            // a simple bounding box. Note that the state's time window
                            // is expanded by `EVENT_NS_MARGIN` so that there's a small grace
                            // period that allows events not directly adjacent to be collapsed
                            if old_state.event == input.event
                                && state_start.saturating_sub(EVENT_NS_MARGIN) <= input_end
                                && (state_end + EVENT_NS_MARGIN) >= input_start
                            {
                                old_state.duration += input.duration;
                                old_state.collapsed_events += 1;

                                None
                            } else {
                                Some(mem::replace(old_state, input))
                            }
                        }
                    },
                )
            }

            State::Flush(input) => {
                if let Some(state_val) = state.clone() {
                    if state_val.start_time + state_val.duration
                        <= input.start_time + input.duration
                    {
                        return (true, state.take());
                    }
                }

                (false, None)
            }
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
    pub enum State {
        Event(WorkerTimelineEvent),
        Flush(WorkerTimelineEvent),
    }

    impl State {
        const fn worker(&self) -> WorkerId {
            match self {
                Self::Event(event) | Self::Flush(event) => event.worker,
            }
        }
    }

    let (normal, delayed) = events
        .inner
        .delay(|&(_, timestamp, _), _| timestamp)
        // Note: This code is kinda sketchy all-around, it takes the current *stream time* and uses it as
        //       the timestamp the flush messages will be delayed at. This means that instead of using
        //       `event_start_time + event_duration` as the delayed timestamp we're using
        //       `stream_time + event_duration`. The purpose of delaying the flush stream is so that the
        //       flush message arrives *after* any potentially collapsible messages, thereby making sure
        //       that there's actually an opportunity for events to be collapsed
        .split(|(event, time, _diff)| {
            let end_time = time + Duration::from_nanos(event.duration + EVENT_NS_MARGIN);

            (
                // Note: the time of this stream is entirely ignored
                (State::Event(event.clone()), time),
                (State::Flush(event), end_time),
            )
        });

    let collapsed = normal
        .concat(&delayed.delay(|&(_, end_time), _| end_time))
        .map(|(event, _)| (event.worker(), event))
        .state_machine(fold_timeline_events, move |&worker_id| {
            worker_id.into_inner() as u64
        })
        .map(|event| {
            let timestamp = Duration::from_nanos(event.start_time + event.duration);
            (event, timestamp, R::from(1))
        })
        .as_collection();

    if cfg!(debug_assertions) {
        collapsed
            .filter(|event| event.collapsed_events > 1)
            .inspect(|x| tracing::debug!("Collapsed timeline event: {:?}", x));
    }

    collapsed
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub(super) struct EventData {
    pub(super) worker: WorkerId,
    pub(super) partial_event: PartialTimelineEvent,
    pub(super) start_time: Duration,
    pub(super) duration: Duration,
}

impl EventData {
    pub const fn new(
        worker: WorkerId,
        partial_event: PartialTimelineEvent,
        start_time: Duration,
        duration: Duration,
    ) -> Self {
        Self {
            worker,
            partial_event,
            start_time,
            duration,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub(super) enum EventKind {
    OperatorActivation { operator_id: OperatorId },
    Message,
    Progress,
    Input,
    Park,
    Application { id: usize },
    Merge { operator_id: OperatorId },
}

impl EventKind {
    pub const fn activation(operator_id: OperatorId) -> Self {
        Self::OperatorActivation { operator_id }
    }

    pub const fn application(id: usize) -> Self {
        Self::Application { id }
    }

    pub const fn merge(operator_id: OperatorId) -> Self {
        Self::Merge { operator_id }
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Abomonation,
)]
pub(super) enum PartialTimelineEvent {
    OperatorActivation { operator_id: OperatorId },
    Application,
    Parked,
    Input,
    Message,
    Progress,
    Merge { operator_id: OperatorId },
}

impl PartialTimelineEvent {
    pub const fn activation(operator_id: OperatorId) -> Self {
        Self::OperatorActivation { operator_id }
    }

    pub const fn merge(operator_id: OperatorId) -> Self {
        Self::Merge { operator_id }
    }

    pub const fn operator_id(&self) -> Option<OperatorId> {
        match *self {
            Self::OperatorActivation { operator_id } | Self::Merge { operator_id } => {
                Some(operator_id)
            }
            _ => None,
        }
    }
}

#[allow(clippy::from_over_into)]
impl Into<TimelineEvent> for PartialTimelineEvent {
    fn into(self) -> TimelineEvent {
        match self {
            Self::OperatorActivation { operator_id } => TimelineEvent::OperatorActivation {
                operator_id,
                operator_name: String::new(),
            },
            Self::Application => TimelineEvent::Application,
            Self::Parked => TimelineEvent::Parked,
            Self::Input => TimelineEvent::Input,
            Self::Message => TimelineEvent::Message,
            Self::Progress => TimelineEvent::Progress,
            Self::Merge { operator_id } => TimelineEvent::Merge {
                operator_id,
                operator_name: String::new(),
            },
        }
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Abomonation,
)]
pub enum TimelineEvent {
    OperatorActivation {
        operator_id: OperatorId,
        operator_name: String,
    },
    Application,
    Parked,
    Input,
    Message,
    Progress,
    Merge {
        operator_id: OperatorId,
        operator_name: String,
    },
}

impl TimelineEvent {
    pub fn operator_name_mut(&mut self) -> Option<&mut String> {
        match self {
            Self::OperatorActivation { operator_name, .. } | Self::Merge { operator_name, .. } => {
                Some(operator_name)
            }

            _ => None,
        }
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize, Abomonation,
)]
pub struct WorkerTimelineEvent {
    pub event_id: u64,
    pub worker: WorkerId,
    pub event: TimelineEvent,
    pub start_time: u64,
    pub duration: u64,
    /// The number of events that have been collapsed within the current timeline event
    pub collapsed_events: usize,
}
