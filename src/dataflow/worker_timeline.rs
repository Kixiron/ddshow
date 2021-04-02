use crate::dataflow::{Diff, DifferentialLogBundle, FilterSplit, Split, TimelyLogBundle};
use abomonation_derive::Abomonation;
use differential_dataflow::{
    algorithms::identifiers::Identifiers,
    difference::Abelian,
    lattice::Lattice,
    logging::DifferentialEvent,
    operators::{
        arrange::{ArrangeByKey, Arranged},
        JoinCore,
    },
    trace::TraceReader,
    AsCollection, Collection, ExchangeData,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, iter, mem, ops::Mul, time::Duration};
use timely::{
    dataflow::{
        channels::pact::Pipeline,
        operators::{aggregation::StateMachine, Concat, Delay, Enter, Map, Operator},
        Scope, Stream,
    },
    logging::{ParkEvent, StartStop, TimelyEvent, WorkerIdentifier},
};

pub fn worker_timeline<S, Trace>(
    scope: &mut S,
    timely_stream: &Stream<S, TimelyLogBundle>,
    differential_stream: Option<&Stream<S, DifferentialLogBundle>>,
    operator_names: &Arranged<S, Trace>,
) -> Collection<S, WorkerTimelineEvent, Diff>
where
    S: Scope<Timestamp = Duration>,
    Trace: TraceReader<Key = usize, Val = String, Time = Duration, R = Diff> + Clone + 'static,
{
    scope.region_named("Collect Worker Timelines", |region| {
        let (timely_stream, differential_stream) = (
            timely_stream.enter(region),
            differential_stream.map(|stream| stream.enter(region)),
        );

        let timely_events = timely_stream.unary(
            Pipeline,
            "Associate Timely Start/Stop Events",
            |_capability, _info| {
                let mut buffer = Vec::new();
                let mut event_map = HashMap::new();

                move |input, output| {
                    input.for_each(|capability, data| {
                        let capability = capability.retain();
                        data.swap(&mut buffer);

                        for (time, worker, event) in buffer.drain(..) {
                            match event {
                                TimelyEvent::Schedule(schedule) => {
                                    let event = EventKind::OperatorActivation {
                                        operator_id: schedule.id,
                                    };

                                    match schedule.start_stop {
                                        StartStop::Start => {
                                            event_map.insert(
                                                (worker, event),
                                                (time, capability.clone()),
                                            );
                                        }

                                        StartStop::Stop => {
                                            if let Some((start_time, mut stored_capability)) =
                                                event_map.remove(&(worker, event))
                                            {
                                                let duration = time - start_time;
                                                stored_capability.downgrade(
                                                    &stored_capability
                                                        .time()
                                                        .join(capability.time()),
                                                );

                                                output.session(&stored_capability).give((
                                                    (
                                                        worker,
                                                        PartialTimelineEvent::OperatorActivation {
                                                            operator_id: schedule.id,
                                                        },
                                                        duration,
                                                    ),
                                                    time,
                                                    1,
                                                ));
                                            } else {
                                                tracing::error!("attempted to remove schedule event that was never started");
                                            }
                                        }
                                    }
                                }

                                TimelyEvent::Application(app) => {
                                    let event = EventKind::Application { id: app.id };

                                    if app.is_start {
                                        event_map
                                            .insert((worker, event), (time, capability.clone()));
                                    } else if let Some((start_time, mut stored_capability)) =
                                        event_map.remove(&(worker, event))
                                    {
                                        let duration = time - start_time;
                                        stored_capability.downgrade(
                                            &stored_capability.time().join(capability.time()),
                                        );

                                        output.session(&stored_capability).give((
                                            (worker, PartialTimelineEvent::Application, duration),
                                            time,
                                            1,
                                        ));
                                    } else {
                                        tracing::error!("attempted to remove application event that was never started");
                                    }
                                }

                                TimelyEvent::GuardedMessage(message) => {
                                    let event = EventKind::Message;

                                    if message.is_start {
                                        event_map
                                            .insert((worker, event), (time, capability.clone()));
                                    } else if let Some((start_time, mut stored_capability)) =
                                        event_map.remove(&(worker, event))
                                    {
                                        let duration = time - start_time;
                                        stored_capability.downgrade(
                                            &stored_capability.time().join(capability.time()),
                                        );

                                        output.session(&stored_capability).give((
                                            (worker, PartialTimelineEvent::Message, duration),
                                            time,
                                            1,
                                        ));
                                    } else {
                                        tracing::error!("attempted to remove guarded message event that was never started");
                                    }
                                }

                                TimelyEvent::GuardedProgress(progress) => {
                                    let event = EventKind::Progress;

                                    if progress.is_start {
                                        event_map
                                            .insert((worker, event), (time, capability.clone()));
                                    } else if let Some((start_time, mut stored_capability)) =
                                        event_map.remove(&(worker, event))
                                    {
                                        let duration = time - start_time;
                                        stored_capability.downgrade(
                                            &stored_capability.time().join(capability.time()),
                                        );

                                        output.session(&stored_capability).give((
                                            (worker, PartialTimelineEvent::Progress, duration),
                                            time,
                                            1,
                                        ));
                                    } else {
                                        tracing::error!("attempted to remove guarded progress event that was never started");
                                    }
                                }

                                TimelyEvent::Input(input) => {
                                    let event = EventKind::Input;

                                    match input.start_stop {
                                        StartStop::Start => {
                                            event_map.insert(
                                                (worker, event),
                                                (time, capability.clone()),
                                            );
                                        }

                                        StartStop::Stop => {
                                            if let Some((start_time, mut stored_capability)) =
                                                event_map.remove(&(worker, event))
                                            {
                                                let duration = time - start_time;
                                                stored_capability.downgrade(
                                                    &stored_capability
                                                        .time()
                                                        .join(capability.time()),
                                                );

                                                output.session(&stored_capability).give((
                                                    (worker, PartialTimelineEvent::Input, duration),
                                                    time,
                                                    1,
                                                ));
                                            } else {
                                                tracing::error!("attempted to remove input event that was never started");
                                            }
                                        }
                                    }
                                }

                                TimelyEvent::Park(park) => {
                                    let event = EventKind::Park;

                                    match park {
                                        ParkEvent::Park(_) => {
                                            event_map.insert(
                                                (worker, event),
                                                (time, capability.clone()),
                                            );
                                        }

                                        ParkEvent::Unpark => {
                                            if let Some((start_time, mut stored_capability)) =
                                                event_map.remove(&(worker, event))
                                            {
                                                let duration = time - start_time;
                                                stored_capability.downgrade(
                                                    &stored_capability
                                                        .time()
                                                        .join(capability.time()),
                                                );

                                                output.session(&stored_capability).give((
                                                    (worker, PartialTimelineEvent::Parked, duration),
                                                    time,
                                                    1,
                                                ));
                                            } else {
                                                tracing::error!("attempted to remove park event that was never started");
                                            }
                                        }
                                    }
                                }

                                TimelyEvent::Operates(_)
                                | TimelyEvent::Channels(_)
                                | TimelyEvent::PushProgress(_)
                                | TimelyEvent::Messages(_)
                                | TimelyEvent::Shutdown(_)
                                | TimelyEvent::CommChannels(_)
                                | TimelyEvent::Text(_) => {}
                            }
                        }
                    });
                }
            },
        );

        // TODO: Emit trace drops & shares to a separate stream so that we can make markers
        //       with `timeline.setCustomTime()`
        let differential_events = differential_stream.map(|stream| {
            stream.unary(
                Pipeline,
                "Associate Differential Start/Stop Events",
                |_capability, _info| {
                    let mut buffer = Vec::new();
                    let mut event_map = HashMap::new();

                    move |input, output| {
                        input.for_each(|capability, data| {
                            let capability = capability.retain();
                            data.swap(&mut buffer);

                            for (time, worker, event) in buffer.drain(..) {
                                match event {
                                    DifferentialEvent::Merge(merge) => {
                                        let event = EventKind::Merge {
                                            operator_id: merge.operator,
                                        };

                                        if merge.complete.is_none() {
                                            let result = event_map.insert(
                                                (worker, event),
                                                (time, capability.clone()),
                                            );

                                            // Sometimes nested(?) merges happen, so simply complete the previous
                                            // merge event
                                            if let Some((_start_time, mut _stored_capability)) = result {
                                                // TODO: Figure out how to handle this?
                                                // let duration = time - start_time;
                                                // stored_capability.downgrade(
                                                //     &stored_capability.time().join(capability.time()),
                                                // );
                                                // 
                                                // output.session(&stored_capability).give((
                                                //     (
                                                //         worker,
                                                //         PartialTimelineEvent::Merge {
                                                //             operator_id: merge.operator,
                                                //         },
                                                //         duration,
                                                //     ),
                                                //     time,
                                                //     1,
                                                // ));
                                            }
                                        } else if let Some((start_time, mut stored_capability)) =
                                            event_map.remove(&(worker, event))
                                        {
                                            let duration = time - start_time;
                                            stored_capability.downgrade(
                                                &stored_capability.time().join(capability.time()),
                                            );

                                            output.session(&stored_capability).give((
                                                (
                                                    worker,
                                                    PartialTimelineEvent::Merge {
                                                        operator_id: merge.operator,
                                                    },
                                                    duration,
                                                ),
                                                time,
                                                1,
                                            ));
                                        } else {
                                            tracing::error!("attempted to remove merge event that was never started");
                                        }
                                    }

                                    DifferentialEvent::MergeShortfall(shortfall) => {
                                        let event = EventKind::Merge {
                                            operator_id: shortfall.operator,
                                        };

                                        if let Some((start_time, mut stored_capability)) =
                                            event_map.remove(&(worker, event))
                                        {
                                            let duration = time - start_time;
                                            stored_capability.downgrade(
                                                &stored_capability.time().join(capability.time()),
                                            );

                                            output.session(&stored_capability).give((
                                                (
                                                    worker,
                                                    PartialTimelineEvent::Merge {
                                                        operator_id: shortfall.operator,
                                                    },
                                                    duration,
                                                ),
                                                time,
                                                1,
                                            ));
                                        } else {
                                            tracing::error!("attempted to remove a short merge event that was never started");
                                        }
                                    }

                                    // Sometimes merges don't complete since they're dropped part way through
                                    DifferentialEvent::Drop(drop) => {
                                        let event = EventKind::Merge {
                                            operator_id: drop.operator,
                                        };

                                        if let Some((start_time, mut stored_capability)) =
                                            event_map.remove(&(worker, event))
                                        {
                                            tracing::warn!("trace was dropped part way though a merge event");

                                            let duration = time - start_time;
                                            stored_capability.downgrade(
                                                &stored_capability.time().join(capability.time()),
                                            );

                                            output.session(&stored_capability).give((
                                                (
                                                    worker,
                                                    PartialTimelineEvent::Merge {
                                                        operator_id: drop.operator,
                                                    },
                                                    duration,
                                                ),
                                                time,
                                                1,
                                            ));
                                        }
                                    }

                                    DifferentialEvent::Batch(_)
                                    | DifferentialEvent::TraceShare(_) => {}
                                }
                            }
                        });
                    }
                },
            )
        });

        let partial_events = differential_events
            .as_ref()
            .map(|differential_events| timely_events.concat(differential_events))
            .unwrap_or(timely_events)
            .as_collection()
            .identifiers();

        let (needs_operators, finished) = partial_events
            .filter_split(|((worker_id, event, duration), event_id)| {
                let timeline_event = WorkerTimelineEvent {
                    event_id,
                    worker: worker_id,
                    event: event.into(),
                    duration: duration.as_nanos() as u64,
                    start_time: duration.as_nanos() as u64,
                    collapsed_events: 1,
                };

                if let Some(operator_id) = event.operator_id() {
                    (
                        Some((
                            operator_id,
                            timeline_event,
                        )),
                        None,
                    )
                } else {
                    (None, Some(timeline_event))
                }
            });

        let events = needs_operators
            .arrange_by_key()
            .join_core(&operator_names.enter_region(region), |_id, event, name| {
                let mut event = event.to_owned();
                *event.event.operator_name_mut().unwrap() = name.to_owned();

                iter::once(event)
            })
            .concat(&finished);

        collapse_events(&events)
            .leave_region()
    })
}

fn collapse_events<S, R>(
    events: &Collection<S, WorkerTimelineEvent, R>,
) -> Collection<S, WorkerTimelineEvent, R>
where
    S: Scope<Timestamp = Duration>,
    S::Timestamp: Lattice,
    R: Abelian + ExchangeData + Mul<Output = R> + From<i8>,
{
    const MARGIN_NS: u64 = 500_000;

    fn fold_timeline_events(
        _key: &usize,
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
                            // is expanded by `MARGIN_NS` so that there's a small grace
                            // period that allows events not directly adjacent to be collapsed
                            if old_state.event == input.event
                                && state_start.saturating_sub(MARGIN_NS) <= input_end
                                && (state_end + MARGIN_NS) >= input_start
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
        const fn worker(&self) -> usize {
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
            let end_time = time + Duration::from_nanos(event.duration + MARGIN_NS);

            (
                // Note: the time of this stream is entirely ignored
                (State::Event(event.clone()), time),
                (State::Flush(event), end_time),
            )
        });

    let collapsed = normal
        .concat(&delayed.delay(|&(_, end_time), _| end_time))
        .map(|(event, _)| (event.worker(), event))
        .state_machine(fold_timeline_events, move |&worker_id| worker_id as u64)
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
enum EventKind {
    OperatorActivation { operator_id: usize },
    Message,
    Progress,
    Input,
    Park,
    Application { id: usize },
    Merge { operator_id: usize },
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Abomonation,
)]
enum PartialTimelineEvent {
    OperatorActivation { operator_id: usize },
    Application,
    Parked,
    Input,
    Message,
    Progress,
    Merge { operator_id: usize },
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

impl PartialTimelineEvent {
    pub const fn operator_id(&self) -> Option<usize> {
        match *self {
            Self::OperatorActivation { operator_id } | Self::Merge { operator_id } => {
                Some(operator_id)
            }

            _ => None,
        }
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Abomonation,
)]
pub enum TimelineEvent {
    OperatorActivation {
        operator_id: usize,
        operator_name: String,
    },
    Application,
    Parked,
    Input,
    Message,
    Progress,
    Merge {
        operator_id: usize,
        operator_name: String,
    },
}

impl TimelineEvent {
    fn operator_name_mut(&mut self) -> Option<&mut String> {
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
    pub worker: WorkerIdentifier,
    pub event: TimelineEvent,
    pub start_time: u64,
    pub duration: u64,
    /// The number of events that have been collapsed within the current timeline event
    pub collapsed_events: usize,
}
