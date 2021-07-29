use crate::dataflow::{
    operators::ActivateCapabilitySet,
    tests::{
        collect_timely_events, init_test_logging,
        proptest_utils::{gen_event_pair, EventPair, Expected},
    },
    timely_source::extract_timely_info,
    worker_timeline::{collect_differential_events, worker_timeline, TimelineEvent},
};
use ddshow_types::{
    differential_logging::DifferentialEvent, timely_logging::TimelyEvent, WorkerId,
};
use differential_dataflow::{difference::Present, ExchangeData};
use proptest::{collection::vec as propvec, prop_assert_eq, proptest, test_runner::TestCaseError};
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

type ExpectationVec<E> = Vec<(Duration, Vec<(E, Duration, Present)>)>;

proptest! {
    #[test]
    fn timeline_events(
        timely in propvec(gen_event_pair(false), 1..10),
        differential in propvec(gen_event_pair(false), 1..10),
    ) {
        timeline_events_inner(timely, differential)?;
    }

    #[test]
    fn timely_events(pair in gen_event_pair(true)) {
        events_inner::<TimelyEvent, _>(pair, |events| collect_timely_events(events).inner)?;
    }

    #[test]
    fn timely_events_stress(pairs in propvec(gen_event_pair(true), 1..500)) {
        events_stress_inner::<TimelyEvent, _>(pairs, |events| collect_timely_events(events).inner)?;
    }

    #[test]
    fn differential_events(pair in gen_event_pair(true)) {
        events_inner::<DifferentialEvent, _>(pair, |events| collect_differential_events(events).inner)?;
    }

    #[test]
    fn differential_events_stress(pairs in propvec(gen_event_pair(true), 1..500)) {
        events_stress_inner::<DifferentialEvent, _>(pairs, |events| collect_differential_events(events).inner)?;
    }
}

type EventStreamIn<'a, E> = Stream<Child<'a, Worker<Thread>, Duration>, (Duration, WorkerId, E)>;
type EventStreamOut<'a> =
    Stream<Child<'a, Worker<Thread>, Duration>, (TimelineEvent, Duration, Present)>;

fn timeline_events_inner(
    timely: Vec<EventPair<TimelyEvent>>,
    differential: Vec<EventPair<DifferentialEvent>>,
) -> Result<(), TestCaseError> {
    init_test_logging();

    let mut expected = Vec::new();
    let mut final_timestamp = Duration::from_nanos(0);

    for pair in timely.iter() {
        let (timestamp, (event, _, diff)) = pair.expected();
        if timestamp > final_timestamp {
            final_timestamp = timestamp;
        }

        expected.push((event, timestamp, diff));
    }

    for pair in differential.iter() {
        let (timestamp, (event, _, diff)) = pair.expected();
        if timestamp > final_timestamp {
            final_timestamp = timestamp;
        }

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
        ) = worker.dataflow(|scope| {
            let (timely_input, timely_stream) = scope.new_unordered_input();
            let (differential_input, differential_stream) = scope.new_unordered_input();

            let (_, _, _, _, _, _, _, _, _, _, _, _, timely_events) =
                extract_timely_info(scope, &timely_stream, false);
            let timely_events = timely_events.unwrap();

            let partial_events = worker_timeline(scope, &timely_events, Some(&differential_stream));

            partial_events
                .inner
                .probe_with(&mut probe)
                .capture_into(send.lock().unwrap().clone());

            (timely_input, differential_input)
        });

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

    let mut data_map: HashMap<TimelineEvent, (Duration, Present), XXHasher> =
        HashMap::with_hasher(XXHasher::default());
    for (_, data) in recv.extract() {
        for (event, time, diff) in data {
            // Note that we've preserved the event id of events here, this is
            // so that we can accurately react to changes in generated ids
            data_map
                .entry(event)
                .and_modify(|(t, _)| {
                    *t = time;
                })
                .or_insert((time, diff));
        }
    }

    let mut data: Vec<_> = data_map
        .into_iter()
        .map(|(event, (time, diff))| (event, time, diff))
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

    let mut expected: ExpectationVec<TimelineEvent> = Vec::new();
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
