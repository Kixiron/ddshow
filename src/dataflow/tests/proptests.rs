use crate::dataflow::{
    operators::ActivateCapabilitySet,
    tests::{
        collect_timely_events, init_test_logging,
        proptest_utils::{gen_event_pair, EventPair, Expected},
    },
    timely_source::extract_timely_info,
    worker_timeline::{
        collect_differential_events, worker_timeline, EventData, WorkerTimelineEvent,
    },
    Diff,
};
use ddshow_types::{
    differential_logging::DifferentialEvent, timely_logging::TimelyEvent, WorkerId,
};
use differential_dataflow::{
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

type EventStreamIn<'a, E> = Stream<Child<'a, Worker<Thread>, Duration>, (Duration, WorkerId, E)>;
type EventStreamOut<'a> = Stream<Child<'a, Worker<Thread>, Duration>, (EventData, Duration, Diff)>;

fn timeline_events_inner(
    mut name_rng: TestRng,
    timely: Vec<EventPair<TimelyEvent>>,
    differential: Vec<EventPair<DifferentialEvent>>,
) -> Result<(), TestCaseError> {
    init_test_logging();

    let mut expected = Vec::new();
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
                let len = name_rng.gen_range(1..=10);
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

        expected.push((event, timestamp, diff));
    }

    for pair in differential.iter() {
        let (timestamp, (event, _, diff)) = pair.expected();

        if timestamp > final_timestamp {
            final_timestamp = timestamp;
        }

        let name = operator_names
            .entry((event.worker, event.partial_event.operator_id().unwrap()))
            .or_insert_with(|| {
                let len = name_rng.gen_range(1..=10);
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

        expected.push((event, timestamp, diff));
    }

    expected.sort_unstable_by_key(|&(ref event, time, _)| {
        (event.worker, time, event.start_time, event.duration)
    });

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

            let (_, _, _, _, _, _, _, _, _, _, _, _, timely_events) =
                extract_timely_info(scope, &timely_stream);

            let partial_events = worker_timeline(
                scope,
                &timely_events,
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
        drop(operator_capability);

        let mut capabilities =
            ActivateCapabilitySet::from(vec![timely_capability, differential_capability]);

        for pair in timely {
            pair.give_to_unordered(&mut timely_input, &mut capabilities);
        }
        for pair in differential {
            pair.give_to_unordered(&mut differential_input, &mut capabilities);
        }

        capabilities.insert(capabilities.delayed(&final_timestamp));
        drop(capabilities);

        worker.step_or_park_while(None, || {
            probe.less_than(&(final_timestamp + Duration::from_millis(1)))
        });
    });

    let mut data_map: HashMap<WorkerTimelineEvent, (Duration, Diff)> = HashMap::new();
    for (_, data) in recv.extract() {
        for (event, time, diff) in data {
            // Note that we've preserved the event id of events here, this is
            // so that we can accurately react to changes in generated ids
            data_map
                .entry(event)
                .and_modify(|(t, d)| {
                    *t = time;
                    *d += diff;
                })
                .or_insert((time, diff));
        }
    }

    let mut data: Vec<_> = data_map
        .into_iter()
        .filter_map(|(mut event, (time, diff))| {
            if diff >= 1 {
                // We can't predict the id of events, so just unset them
                // TODO: Should probably assert that all output ids are unique?
                event.event_id = 0;

                Some((event, time, diff))
            } else {
                None
            }
        })
        .collect();

    data.sort_unstable_by_key(|&(ref event, time, _)| {
        (event.worker, time, event.start_time, event.duration)
    });

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

        pair.give_to(&mut input);

        input.advance_to(pair.end.recv_timestamp + Duration::from_nanos(1));
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
        for pair in pairs {
            pair.give_to_unordered(&mut input, &mut capabilities);
        }

        capabilities.insert(capabilities.delayed(&final_timestamp));
        drop(capabilities);

        worker.step_or_park_while(None, || probe.less_than(&final_timestamp));
    });

    let data = recv.extract();
    prop_assert_eq!(data, expected);

    Ok(())
}
