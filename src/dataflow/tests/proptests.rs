use crate::dataflow::{
    operators::ActivateCapabilitySet,
    tests::{
        init_test_logging,
        proptest_utils::{gen_timely_event_pair, EventPair},
    },
    worker_timeline::{collect_timely_events, EventData},
    Diff,
};
use proptest::{prop_assert_eq, proptest, test_runner::TestCaseError};
use std::{
    sync::{mpsc, Mutex},
    time::Duration,
};
use timely::dataflow::operators::{capture::Extract, Capture, Input, Probe, UnorderedInput};

type ExpectationVec = Vec<(Duration, Vec<(EventData, Duration, Diff)>)>;

proptest! {
    #[test]
    fn timely_event_association(pair in gen_timely_event_pair()) {
        timely_event_association_inner(pair)?;
    }

    #[test]
    fn timely_event_association_stress(pairs in proptest::collection::vec(gen_timely_event_pair(), 1..1000)) {
        timely_event_association_stress_inner(pairs)?;
    }
}

fn timely_event_association_inner(pair: EventPair) -> Result<(), TestCaseError> {
    init_test_logging();

    let (time, data) = pair.expected();
    let expected = vec![(time, vec![data])];

    let (send, recv) = mpsc::channel();
    let send = Mutex::new(send);

    timely::execute_directly(move |worker| {
        let (mut input, probe) = worker.dataflow(|scope| {
            let (input, stream) = scope.new_input();
            let partial_events = collect_timely_events(&stream);
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

fn timely_event_association_stress_inner(pairs: Vec<EventPair>) -> Result<(), TestCaseError> {
    init_test_logging();

    let mut expected: ExpectationVec = Vec::new();
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

            let partial_events = collect_timely_events(&stream);
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
