use crate::dataflow::{
    worker_timeline::{EventData, PartialTimelineEvent},
    Diff,
};
use proptest::{
    arbitrary::any,
    prelude::Rng,
    prop_oneof,
    strategy::{Just, Strategy},
};
use std::time::Duration;
use timely::logging::{ScheduleEvent, ShutdownEvent, StartStop, TimelyEvent, WorkerIdentifier};

#[derive(Debug, Clone)]
pub struct EventPair {
    pub start: Event,
    pub end: Event,
    pub worker: WorkerIdentifier,
}

#[derive(Debug, Clone)]
pub struct Event {
    pub recv_timestamp: Duration,
    pub timestamp: Duration,
    pub event: TimelyEvent,
}

impl EventPair {
    pub(super) fn expected(&self) -> (Duration, (EventData, Duration, Diff)) {
        let event = match &self.start.event {
            TimelyEvent::Schedule(schedule) => PartialTimelineEvent::OperatorActivation {
                operator_id: schedule.id,
            },

            TimelyEvent::Operates(_)
            | TimelyEvent::Channels(_)
            | TimelyEvent::PushProgress(_)
            | TimelyEvent::Messages(_)
            | TimelyEvent::Shutdown(_)
            | TimelyEvent::Application(_)
            | TimelyEvent::GuardedMessage(_)
            | TimelyEvent::GuardedProgress(_)
            | TimelyEvent::CommChannels(_)
            | TimelyEvent::Input(_)
            | TimelyEvent::Park(_)
            | TimelyEvent::Text(_) => unreachable!(),
        };

        (
            self.end.recv_timestamp,
            (
                EventData::new(
                    self.worker,
                    event,
                    self.start.timestamp,
                    self.end.timestamp - self.start.timestamp,
                ),
                self.end.recv_timestamp,
                1,
            ),
        )
    }
}

pub fn gen_timely_event_pair() -> impl Strategy<Value = EventPair> {
    (
        any::<WorkerIdentifier>(),
        any::<usize>(),
        any::<usize>(),
        any::<u64>(),
        any::<u64>(),
    )
        .prop_flat_map(
            |(worker, operator_id, unique_id, time_range_start, recv_time_range_start)| {
                (
                    Just((worker, operator_id, unique_id)),
                    gen_timely_event(
                        time_range_start,
                        recv_time_range_start,
                        operator_id,
                        unique_id,
                        false,
                    ),
                )
            },
        )
        .prop_flat_map(|((worker, operator_id, unique_id), start_event)| {
            (
                Just(worker),
                gen_timely_event(
                    start_event.timestamp.as_nanos() as u64,
                    start_event.recv_timestamp.as_nanos() as u64,
                    operator_id,
                    unique_id,
                    true,
                ),
                Just(start_event),
            )
        })
        .prop_map(|(worker, end_event, start_event)| EventPair {
            start: start_event,
            end: end_event,
            worker,
        })
}

fn gen_timely_event(
    time_range_start: u64,
    recv_time_range_start: u64,
    operator_id: usize,
    unique_id: usize,
    should_terminate: bool,
) -> impl Strategy<Value = Event> {
    if should_terminate {
        terminating_event_kind(operator_id, unique_id).boxed()
    } else {
        starting_event_kind(operator_id, unique_id).boxed()
    }
    .prop_perturb(move |event, mut rng| {
        (
            event,
            rng.gen_range(time_range_start..u64::max_value()),
            rng.gen_range(recv_time_range_start..u64::max_value()),
        )
    })
    .prop_map(|(event, timestamp, recv_timestamp)| Event {
        timestamp: Duration::from_nanos(timestamp),
        recv_timestamp: Duration::from_nanos(recv_timestamp),
        event,
    })
}

fn starting_event_kind(
    operator_id: usize,
    _unique_id: usize,
) -> impl Strategy<Value = TimelyEvent> {
    Just(TimelyEvent::Schedule(ScheduleEvent {
        id: operator_id,
        start_stop: StartStop::Start,
    }))
}

fn terminating_event_kind(
    operator_id: usize,
    _unique_id: usize,
) -> impl Strategy<Value = TimelyEvent> {
    prop_oneof![
        Just(TimelyEvent::Schedule(ScheduleEvent {
            id: operator_id,
            start_stop: StartStop::Stop,
        })),
        Just(TimelyEvent::Shutdown(ShutdownEvent { id: operator_id }))
    ]
}
