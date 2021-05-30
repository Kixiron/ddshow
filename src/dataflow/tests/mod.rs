#![cfg(test)]

mod proptest_utils;
mod proptests;

use crate::dataflow::{
    worker_timeline::{
        collect_differential_events, process_timely_event, EventData, EventProcessor,
        PartialTimelineEvent, TimelineEventStream,
    },
    TimelyLogBundle,
};
use ddshow_types::{
    timely_logging::{ScheduleEvent, StartStop, TimelyEvent},
    OperatorId, WorkerId,
};
use differential_dataflow::logging::{DifferentialEvent, MergeEvent, MergeShortfall};
use std::{
    collections::HashMap,
    sync::{mpsc, Arc, Mutex},
    time::Duration,
};
use timely::dataflow::{
    channels::pact::Pipeline,
    operators::{capture::Extract, Capture, Input, Operator, Probe},
    Scope, Stream,
};
use tracing_subscriber::{
    fmt::time::Uptime, prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt,
};

#[test]
fn timely_event_association() {
    init_test_logging();

    let (send, recv) = mpsc::channel();
    let send = Arc::new(Mutex::new(Some(send)));

    timely::execute_directly(move |worker| {
        let (mut input, probe) = worker.dataflow(|scope| {
            let (input, stream) = scope.new_input();
            let partial_events = collect_timely_events(&stream);
            partial_events.capture_into(send.lock().unwrap().take().unwrap());

            (input, partial_events.probe())
        });

        input.advance_to(Duration::from_nanos(1));
        input.send((
            Duration::from_nanos(1000),
            WorkerId::new(0),
            TimelyEvent::Schedule(ScheduleEvent {
                id: OperatorId::new(0),
                start_stop: StartStop::Start,
            }),
        ));

        input.advance_to(Duration::from_nanos(2));
        input.send((
            Duration::from_nanos(10_000),
            WorkerId::new(0),
            TimelyEvent::Schedule(ScheduleEvent {
                id: OperatorId::new(0),
                start_stop: StartStop::Stop,
            }),
        ));

        input.advance_to(Duration::from_nanos(3));
        worker.step_or_park_while(None, || probe.less_than(input.time()));
    });

    let data = recv.extract();
    let expected = vec![(
        Duration::from_nanos(2),
        vec![(
            EventData::new(
                WorkerId::new(0),
                PartialTimelineEvent::activation(OperatorId::new(0)),
                Duration::from_nanos(1000),
                Duration::from_nanos(9000),
            ),
            Duration::from_nanos(2),
            1,
        )],
    )];
    assert_eq!(data, expected);
}

#[test]
fn differential_event_association() {
    init_test_logging();

    let (send, recv) = mpsc::channel();
    let send = Arc::new(Mutex::new(Some(send)));

    timely::execute_directly(move |worker| {
        let (mut input, probe) = worker.dataflow(|scope| {
            let (input, stream) = scope.new_input();
            let partial_events = collect_differential_events(&stream);
            partial_events.capture_into(send.lock().unwrap().take().unwrap());

            (input, partial_events.probe())
        });

        input.advance_to(Duration::from_nanos(1));
        input.send((
            Duration::from_nanos(1000),
            WorkerId::new(0),
            DifferentialEvent::Merge(MergeEvent {
                operator: 0,
                scale: 1000,
                length1: 1000,
                length2: 1000,
                complete: None,
            }),
        ));

        input.advance_to(Duration::from_nanos(2));
        input.send((
            Duration::from_nanos(10_000),
            WorkerId::new(0),
            DifferentialEvent::Merge(MergeEvent {
                operator: 0,
                scale: 1000,
                length1: 1000,
                length2: 1000,
                complete: Some(1000),
            }),
        ));

        input.advance_to(Duration::from_nanos(3));
        input.send((
            Duration::from_nanos(20_000),
            WorkerId::new(0),
            DifferentialEvent::Merge(MergeEvent {
                operator: 1,
                scale: 1000,
                length1: 1000,
                length2: 1000,
                complete: None,
            }),
        ));

        input.advance_to(Duration::from_nanos(4));
        input.send((
            Duration::from_nanos(21_000),
            WorkerId::new(0),
            DifferentialEvent::MergeShortfall(MergeShortfall {
                operator: 1,
                scale: 1000,
                shortfall: 100,
            }),
        ));

        input.advance_to(Duration::from_nanos(5));
        worker.step_or_park_while(None, || probe.less_than(input.time()));
    });

    let data = recv.extract();
    let expected = vec![
        (
            Duration::from_nanos(2),
            vec![(
                EventData::new(
                    WorkerId::new(0),
                    PartialTimelineEvent::merge(OperatorId::new(0)),
                    Duration::from_nanos(1000),
                    Duration::from_nanos(9000),
                ),
                Duration::from_nanos(2),
                1,
            )],
        ),
        (
            Duration::from_nanos(4),
            vec![(
                EventData::new(
                    WorkerId::new(0),
                    PartialTimelineEvent::merge(OperatorId::new(1)),
                    Duration::from_nanos(20_000),
                    Duration::from_nanos(1000),
                ),
                Duration::from_nanos(4),
                1,
            )],
        ),
    ];
    assert_eq!(data, expected);
}

pub(crate) fn init_test_logging() {
    let fmt_layer = tracing_subscriber::fmt::layer()
        .pretty()
        .with_test_writer()
        .with_timer(Uptime::default())
        .with_thread_names(true)
        .with_ansi(true);

    if tracing_subscriber::registry()
        .with(fmt_layer)
        .try_init()
        .is_ok()
    {
        tracing::info!("initialized logging");
    } else {
        tracing::info!("logging was already initialized");
    }
}

fn collect_timely_events<'a, 'b, S>(
    event_stream: &'b Stream<S, TimelyLogBundle>,
) -> TimelineEventStream<S>
where
    S: Scope<Timestamp = Duration> + 'a,
{
    event_stream.unary(
        Pipeline,
        "Gather Timely Event Durations",
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

                        process_timely_event(&mut event_processor, event);
                    }
                });
            }
        },
    )
}
