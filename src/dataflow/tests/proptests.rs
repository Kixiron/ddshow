use crate::dataflow::{
    operators::ActivateCapabilitySet,
    tests::{
        init_test_logging,
        proptest_utils::{gen_event_pair, EventPair, Expected},
    },
    worker_timeline::{
        collect_differential_events, collect_timely_events, worker_timeline, EventData,
        WorkerTimelineEvent,
    },
    Diff,
};
use differential_dataflow::{
    logging::DifferentialEvent,
    operators::{arrange::ArrangeByKey, Consolidate},
    AsCollection, ExchangeData,
};
use proptest::{
    arbitrary::any,
    collection::vec as propvec,
    prelude::Rng,
    prop_assert_eq, proptest,
    test_runner::{RngAlgorithm, TestCaseError, TestRng},
};
use rand::distributions::Alphanumeric;
use std::{
    collections::HashMap,
    sync::{mpsc, Mutex},
    time::Duration,
};
use timely::{
    communication::allocator::Thread,
    dataflow::{
        operators::{
            capture::Extract, probe::Handle as ProbeHandle, Capture, Input, Probe, UnorderedInput,
        },
        scopes::Child,
        Stream,
    },
    logging::{TimelyEvent, WorkerIdentifier},
    worker::Worker,
};

type ExpectationVec<E> = Vec<(Duration, Vec<(E, Duration, Diff)>)>;

proptest! {
    #[test]
    fn timeline_events(
        name_seed in [any::<u8>(); 32],
        timely in propvec(gen_event_pair(false), 1..10),
        differential in propvec(gen_event_pair(false), 1..10),
    ) {
        let name_rng = TestRng::from_seed(RngAlgorithm::ChaCha, &name_seed);
        timeline_events_inner(name_rng, timely, differential)?;
    }

    #[test]
    fn timely_events(pair in gen_event_pair(true)) {
        events_inner::<TimelyEvent, _>(pair, |events| collect_timely_events(events))?;
    }

    #[test]
    fn timely_events_stress(pairs in propvec(gen_event_pair(true), 1..500)) {
        events_stress_inner::<TimelyEvent, _>(pairs, |events| collect_timely_events(events))?;
    }

    #[test]
    fn differential_events(pair in gen_event_pair(true)) {
        events_inner::<DifferentialEvent, _>(pair, |events| collect_differential_events(events))?;
    }

    #[test]
    fn differential_events_stress(pairs in propvec(gen_event_pair(true), 1..500)) {
        events_stress_inner::<DifferentialEvent, _>(pairs, |events| collect_differential_events(events))?;
    }
}

type EventStreamIn<'a, E> =
    Stream<Child<'a, Worker<Thread>, Duration>, (Duration, WorkerIdentifier, E)>;
type EventStreamOut<'a> = Stream<Child<'a, Worker<Thread>, Duration>, (EventData, Duration, Diff)>;

fn timeline_events_inner(
    mut name_rng: TestRng,
    timely: Vec<EventPair<TimelyEvent>>,
    differential: Vec<EventPair<DifferentialEvent>>,
) -> Result<(), TestCaseError> {
    init_test_logging();

    let mut expected: ExpectationVec<WorkerTimelineEvent> = Vec::new();
    let mut final_timestamp = Duration::from_nanos(0);
    let mut operator_names = HashMap::new();

    for pair in timely.iter() {
        let (timestamp, (event, _, diff)) = pair.expected();

        if timestamp > final_timestamp {
            final_timestamp = timestamp;
        }

        let name = operator_names
            .entry((event.worker, event.partial_event.operator_id().unwrap()))
            .or_insert_with(|| {
                let len = name_rng.gen::<u8>() as usize;
                (&mut name_rng)
                    .sample_iter(Alphanumeric)
                    .take(len)
                    .map(char::from)
                    .collect::<String>()
            })
            .clone();

        let mut event = WorkerTimelineEvent {
            event_id: 0,
            worker: event.worker,
            event: event.partial_event.into(),
            start_time: event.start_time.as_nanos() as u64,
            duration: event.duration.as_nanos() as u64,
            collapsed_events: 1,
        };
        *event.event.operator_name_mut().unwrap() = name;

        if let Some((_, events)) = expected
            .iter_mut()
            .find(|&&mut (time, _)| time == timestamp)
        {
            events.push((event, timestamp, diff));
        } else {
            expected.push((timestamp, vec![(event, timestamp, diff)]));
        }
    }

    for pair in differential.iter() {
        let (timestamp, (event, _, diff)) = pair.expected();

        if timestamp > final_timestamp {
            final_timestamp = timestamp;
        }

        let name = operator_names
            .entry((event.worker, event.partial_event.operator_id().unwrap()))
            .or_insert_with(|| {
                let len = name_rng.gen::<u8>() as usize;
                (&mut name_rng)
                    .sample_iter(Alphanumeric)
                    .take(len)
                    .map(char::from)
                    .collect::<String>()
            })
            .clone();

        let mut event = WorkerTimelineEvent {
            event_id: 0,
            worker: event.worker,
            event: event.partial_event.into(),
            start_time: event.start_time.as_nanos() as u64,
            duration: event.duration.as_nanos() as u64,
            collapsed_events: 1,
        };
        *event.event.operator_name_mut().unwrap() = name;

        dbg!(&event, &expected, timestamp);
        if let Some((_, events)) = expected
            .iter_mut()
            .find(|&&mut (time, _)| time == timestamp)
        {
            events.push((event, timestamp, diff));
        } else {
            expected.push((timestamp, vec![(event, timestamp, diff)]));
        }
        dbg!(&expected, timestamp);
    }

    expected.sort_unstable_by_key(|&(time, _)| time);
    for (_, data) in expected.iter_mut() {
        data.sort_unstable_by_key(|&(ref event, time, _)| {
            (time, event.worker, event.duration, event.event.clone())
        });
    }

    let (send, recv) = mpsc::channel();
    let send = Mutex::new(send);

    timely::execute_directly(move |worker| {
        let mut probe = ProbeHandle::new();

        let (
            (mut timely_input, timely_capability),
            (mut differential_input, differential_capability),
            (mut operator_input, operator_capability),
        ) = worker.dataflow(|scope| {
            let (timely_input, timely_stream) = scope.new_unordered_input();
            let (differential_input, differential_stream) = scope.new_unordered_input();

            let (operator_input, operator_stream) = scope.new_unordered_input();
            let operator_names = operator_stream.as_collection().arrange_by_key();

            let partial_events = worker_timeline(
                scope,
                &timely_stream,
                Some(&differential_stream),
                &operator_names,
            );

            partial_events
                .consolidate()
                .inner
                .probe_with(&mut probe)
                .capture_into(send.lock().unwrap().clone());

            (timely_input, differential_input, operator_input)
        });

        for ((worker, operator_id), operator_name) in operator_names {
            operator_input.session(operator_capability.clone()).give((
                ((worker, operator_id), operator_name),
                Duration::from_nanos(0),
                1,
            ));
        }

        let mut timely_capabilities = ActivateCapabilitySet::from_elem(timely_capability);
        for EventPair {
            ref start,
            ref end,
            worker: worker_id,
        } in timely
        {
            let (start_capability, end_capability) = (
                timely_capabilities.delayed(&start.recv_timestamp),
                timely_capabilities.delayed(&end.recv_timestamp),
            );
            timely_capabilities.insert(start_capability.clone());
            timely_capabilities.insert(end_capability.clone());

            timely_input.session(start_capability).give((
                start.timestamp,
                worker_id,
                start.event.clone(),
            ));
            timely_input.session(end_capability).give((
                end.timestamp,
                worker_id,
                end.event.clone(),
            ));
        }

        let mut differential_capabilities =
            ActivateCapabilitySet::from_elem(differential_capability);
        for EventPair {
            ref start,
            ref end,
            worker: worker_id,
        } in differential
        {
            let (start_capability, end_capability) = (
                differential_capabilities.delayed(&start.recv_timestamp),
                differential_capabilities.delayed(&end.recv_timestamp),
            );
            differential_capabilities.insert(start_capability.clone());
            differential_capabilities.insert(end_capability.clone());

            differential_input.session(start_capability).give((
                start.timestamp,
                worker_id,
                start.event.clone(),
            ));
            differential_input.session(end_capability).give((
                end.timestamp,
                worker_id,
                end.event.clone(),
            ));
        }

        drop(timely_capabilities);
        drop(differential_capabilities);
        drop(operator_capability);

        worker.step_or_park_while(None, || probe.less_than(&final_timestamp));
    });

    let mut data = recv.extract();
    data.sort_unstable_by_key(|&(time, _)| time);
    for (_, data) in data.iter_mut() {
        data.sort_unstable_by_key(|&(ref event, time, _)| {
            (time, event.worker, event.duration, event.event.clone())
        });

        for (event, _, diff) in data {
            event.event_id = 0;
            prop_assert_eq!(*diff, 1);
        }
    }

    prop_assert_eq!(data, expected);

    Ok(())
}

fn events_inner<E, F>(pair: EventPair<E>, event_collector: F) -> Result<(), TestCaseError>
where
    E: ExchangeData,
    EventPair<E>: Expected,
    F: for<'a, 'b> Fn(&'b EventStreamIn<'a, E>) -> EventStreamOut<'a> + Send + Sync + 'static,
{
    init_test_logging();

    let (time, data) = pair.expected();
    let expected = vec![(time, vec![data])];

    let (send, recv) = mpsc::channel();
    let send = Mutex::new(send);

    timely::execute_directly(move |worker| {
        let (mut input, probe) = worker.dataflow(|scope| {
            let (input, stream) = scope.new_input();
            let partial_events = event_collector(&stream);
            partial_events.capture_into(send.lock().unwrap().clone());

            (input, partial_events.probe())
        });

        let EventPair {
            start,
            end,
            worker: worker_id,
        } = pair;

        input.advance_to(start.recv_timestamp);
        input.send((start.timestamp, worker_id, start.event));

        input.advance_to(end.recv_timestamp);
        input.send((end.timestamp, worker_id, end.event));

        input.advance_to(end.recv_timestamp + Duration::from_nanos(1));
        worker.step_or_park_while(None, || probe.less_than(input.time()));
    });

    let data = recv.extract();
    prop_assert_eq!(data, expected);

    Ok(())
}

fn events_stress_inner<E, F>(
    pairs: Vec<EventPair<E>>,
    event_collector: F,
) -> Result<(), TestCaseError>
where
    E: ExchangeData,
    EventPair<E>: Expected,
    F: for<'a, 'b> Fn(&'b EventStreamIn<'a, E>) -> EventStreamOut<'a> + Send + Sync + 'static,
{
    init_test_logging();

    let mut expected: ExpectationVec<EventData> = Vec::new();
    let mut final_timestamp = Duration::from_nanos(0);

    for pair in pairs.iter() {
        let (timestamp, event) = pair.expected();

        if timestamp > final_timestamp {
            final_timestamp = timestamp;
        }

        if let Some((_, events)) = expected
            .iter_mut()
            .find(|&&mut (time, _)| time == timestamp)
        {
            events.push(event);
        } else {
            expected.push((timestamp, vec![event]));
        }
    }
    expected.sort_unstable_by_key(|&(time, _)| time);

    let (send, recv) = mpsc::channel();
    let send = Mutex::new(send);

    timely::execute_directly(move |worker| {
        let ((mut input, capability), probe) = worker.dataflow(|scope| {
            let (input, stream) = scope.new_unordered_input();

            let partial_events = event_collector(&stream);
            partial_events.capture_into(send.lock().unwrap().clone());

            (input, partial_events.probe())
        });

        let mut capabilities = ActivateCapabilitySet::from_elem(capability);
        for EventPair {
            ref start,
            ref end,
            worker: worker_id,
        } in pairs
        {
            let (start_capability, end_capability) = (
                capabilities.delayed(&start.recv_timestamp),
                capabilities.delayed(&end.recv_timestamp),
            );
            capabilities.insert(start_capability.clone());
            capabilities.insert(end_capability.clone());

            input
                .session(start_capability)
                .give((start.timestamp, worker_id, start.event.clone()));
            input
                .session(end_capability)
                .give((end.timestamp, worker_id, end.event.clone()));
        }

        capabilities.insert(capabilities.delayed(&final_timestamp));
        worker.step_or_park_while(None, || {
            capabilities
                .first()
                .map(|cap| probe.less_than(cap.time()))
                .unwrap_or_default()
        });
    });

    let data = recv.extract();
    prop_assert_eq!(data, expected);

    Ok(())
}
