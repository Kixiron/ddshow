use crate::dataflow::{
    operators::{
        rkyv_capture::{RkyvScheduleEvent, RkyvShutdownEvent, RkyvStartStop},
        ActivateCapabilitySet, RkyvTimelyEvent,
    },
    worker_timeline::{EventData, PartialTimelineEvent},
    Diff, OperatorId, WorkerId,
};
use differential_dataflow::logging::{DifferentialEvent, DropEvent, MergeEvent, MergeShortfall};
use proptest::{
    arbitrary::any,
    prelude::Rng,
    prop_oneof,
    strategy::{BoxedStrategy, Just, Strategy},
    test_runner::{RngAlgorithm, TestRng},
};
use std::{fmt::Debug, time::Duration};
use timely::{
    dataflow::operators::{input::Handle as InputHandle, unordered_input::UnorderedHandle},
    logging::WorkerIdentifier,
};

type ExpectedEvent = (Duration, (EventData, Duration, Diff));

#[derive(Debug, Clone)]
pub struct EventPair<E> {
    pub start: Event<E>,
    pub end: Event<E>,
    pub worker: WorkerId,
}

impl<E> EventPair<E> {
    fn build_expected(&self, event: PartialTimelineEvent) -> ExpectedEvent {
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

    pub(super) fn give_to(&self, input: &mut InputHandle<Duration, (Duration, WorkerId, E)>)
    where
        E: Clone + 'static,
    {
        input.advance_to(self.start.recv_timestamp);
        input.send((self.start.timestamp, self.worker, self.start.event.clone()));

        input.advance_to(self.end.recv_timestamp);
        input.send((self.end.timestamp, self.worker, self.end.event.clone()));
    }

    pub(super) fn give_to_unordered(
        &self,
        input: &mut UnorderedHandle<Duration, (Duration, WorkerId, E)>,
        capabilities: &mut ActivateCapabilitySet<Duration>,
    ) where
        E: Clone + 'static,
    {
        let (start_capability, end_capability) = (
            capabilities.delayed(&self.start.recv_timestamp),
            capabilities.delayed(&self.end.recv_timestamp),
        );
        capabilities.insert(start_capability.clone());
        capabilities.insert(end_capability.clone());

        input.session(start_capability).give((
            self.start.timestamp,
            self.worker,
            self.start.event.clone(),
        ));
        input.session(end_capability).give((
            self.end.timestamp,
            self.worker,
            self.end.event.clone(),
        ));
    }
}

#[derive(Debug, Clone)]
pub struct Event<E> {
    pub recv_timestamp: Duration,
    pub timestamp: Duration,
    pub event: E,
}

pub(super) trait Expected {
    fn expected(&self) -> ExpectedEvent;
}

impl Expected for EventPair<RkyvTimelyEvent> {
    fn expected(&self) -> ExpectedEvent {
        let event = match &self.start.event {
            RkyvTimelyEvent::Schedule(schedule) => PartialTimelineEvent::OperatorActivation {
                operator_id: schedule.id,
            },

            RkyvTimelyEvent::Operates(_)
            | RkyvTimelyEvent::Channels(_)
            | RkyvTimelyEvent::PushProgress(_)
            | RkyvTimelyEvent::Messages(_)
            | RkyvTimelyEvent::Shutdown(_)
            | RkyvTimelyEvent::Application(_)
            | RkyvTimelyEvent::GuardedMessage(_)
            | RkyvTimelyEvent::GuardedProgress(_)
            | RkyvTimelyEvent::CommChannels(_)
            | RkyvTimelyEvent::Input(_)
            | RkyvTimelyEvent::Park(_)
            | RkyvTimelyEvent::Text(_) => unreachable!(),
        };

        self.build_expected(event)
    }
}

impl Expected for EventPair<DifferentialEvent> {
    fn expected(&self) -> ExpectedEvent {
        let event = match &self.start.event {
            DifferentialEvent::Merge(merge) => PartialTimelineEvent::Merge {
                operator_id: OperatorId::new(merge.operator),
            },

            DifferentialEvent::Batch(_)
            | DifferentialEvent::Drop(_)
            | DifferentialEvent::MergeShortfall(_)
            | DifferentialEvent::TraceShare(_) => unreachable!(),
        };

        self.build_expected(event)
    }
}

pub fn gen_event_pair<E>(allow_stops: bool) -> impl Strategy<Value = EventPair<E>>
where
    E: EventInner + Clone + Debug,
{
    (
        any::<WorkerIdentifier>(),
        any::<usize>(),
        any::<[u8; 32]>(),
        any::<u64>(),
        any::<u64>(),
    )
        .prop_flat_map(
            move |(worker, operator_id, rng_seed, time_range_start, recv_time_range_start)| {
                let mut rng = TestRng::from_seed(RngAlgorithm::ChaCha, &rng_seed);

                (
                    gen_event(
                        &mut rng,
                        time_range_start,
                        recv_time_range_start,
                        operator_id,
                        false,
                        allow_stops,
                    ),
                    Just((WorkerId::new(worker), operator_id, rng)),
                )
            },
        )
        .prop_flat_map(move |(start_event, (worker, operator_id, mut rng))| {
            (
                gen_event(
                    &mut rng,
                    start_event.timestamp.as_nanos() as u64,
                    start_event.recv_timestamp.as_nanos() as u64,
                    operator_id,
                    true,
                    allow_stops,
                ),
                Just((worker, start_event)),
            )
        })
        .prop_map(|(end_event, (worker, start_event))| EventPair {
            start: start_event,
            end: end_event,
            worker,
        })
}

fn gen_event<E>(
    rng: &mut TestRng,
    time_range_start: u64,
    recv_time_range_start: u64,
    operator_id: usize,
    should_terminate: bool,
    allow_stops: bool,
) -> impl Strategy<Value = Event<E>>
where
    E: EventInner + Debug,
{
    let operator_id = OperatorId::new(operator_id);

    if should_terminate {
        E::terminating_event(operator_id, allow_stops, rng)
    } else {
        E::starting_event(operator_id, rng)
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

pub trait EventInner: Sized {
    fn starting_event(operator_id: OperatorId, rng: &mut TestRng) -> BoxedStrategy<Self>;

    fn terminating_event(
        operator_id: OperatorId,
        allow_stops: bool,
        rng: &mut TestRng,
    ) -> BoxedStrategy<Self>;
}

impl EventInner for RkyvTimelyEvent {
    fn starting_event(operator_id: OperatorId, _rng: &mut TestRng) -> BoxedStrategy<Self> {
        Just(RkyvTimelyEvent::Schedule(RkyvScheduleEvent {
            id: operator_id,
            start_stop: RkyvStartStop::Start,
        }))
        .boxed()
    }

    fn terminating_event(
        operator_id: OperatorId,
        allow_stops: bool,
        _rng: &mut TestRng,
    ) -> BoxedStrategy<Self> {
        if allow_stops {
            prop_oneof![
                Just(RkyvTimelyEvent::Schedule(RkyvScheduleEvent {
                    id: operator_id,
                    start_stop: RkyvStartStop::Stop,
                })),
                Just(RkyvTimelyEvent::Shutdown(RkyvShutdownEvent {
                    id: operator_id
                }))
            ]
            .boxed()
        } else {
            Just(RkyvTimelyEvent::Schedule(RkyvScheduleEvent {
                id: operator_id,
                start_stop: RkyvStartStop::Stop,
            }))
            .boxed()
        }
    }
}

// FIXME: Make these numbers realistic
impl EventInner for DifferentialEvent {
    fn starting_event(operator_id: OperatorId, rng: &mut TestRng) -> BoxedStrategy<Self> {
        Just(DifferentialEvent::Merge(MergeEvent {
            operator: operator_id.into_inner(),
            scale: rng.gen(),
            length1: rng.gen(),
            length2: rng.gen(),
            complete: None,
        }))
        .boxed()
    }

    fn terminating_event(
        operator_id: OperatorId,
        allow_stops: bool,
        rng: &mut TestRng,
    ) -> BoxedStrategy<Self> {
        if allow_stops {
            prop_oneof![
                Just(DifferentialEvent::Merge(MergeEvent {
                    operator: operator_id.into_inner(),
                    scale: rng.gen(),
                    length1: rng.gen(),
                    length2: rng.gen(),
                    complete: Some(rng.gen()),
                })),
                Just(DifferentialEvent::MergeShortfall(MergeShortfall {
                    operator: operator_id.into_inner(),
                    scale: rng.gen(),
                    shortfall: rng.gen(),
                })),
                Just(DifferentialEvent::Drop(DropEvent {
                    operator: operator_id.into_inner(),
                    length: rng.gen(),
                })),
            ]
            .boxed()
        } else {
            prop_oneof![
                Just(DifferentialEvent::Merge(MergeEvent {
                    operator: operator_id.into_inner(),
                    scale: rng.gen(),
                    length1: rng.gen(),
                    length2: rng.gen(),
                    complete: Some(rng.gen()),
                })),
                Just(DifferentialEvent::MergeShortfall(MergeShortfall {
                    operator: operator_id.into_inner(),
                    scale: rng.gen(),
                    shortfall: rng.gen(),
                })),
            ]
            .boxed()
        }
    }
}
