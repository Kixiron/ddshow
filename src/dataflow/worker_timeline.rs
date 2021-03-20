use crate::dataflow::{Diff, DifferentialLogBundle, TimelyLogBundle};
use abomonation_derive::Abomonation;
use differential_dataflow::{
    lattice::Lattice, logging::DifferentialEvent, AsCollection, Collection,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, time::Duration};
use timely::{
    dataflow::{
        channels::pact::Pipeline,
        operators::{Concat, Enter, Operator},
        Scope, Stream,
    },
    logging::{ParkEvent, StartStop, TimelyEvent, WorkerIdentifier},
};

pub fn worker_timeline<S>(
    scope: &mut S,
    timely_stream: &Stream<S, TimelyLogBundle>,
    differential_stream: Option<&Stream<S, DifferentialLogBundle>>,
) -> Collection<S, (WorkerIdentifier, TimelineEvent, Duration), Diff>
where
    S: Scope<Timestamp = Duration>,
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
                                                        TimelineEvent::OperatorActivation {
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
                                            (worker, TimelineEvent::Application, duration),
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
                                            (worker, TimelineEvent::Message, duration),
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
                                            (worker, TimelineEvent::Progress, duration),
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
                                                    (worker, TimelineEvent::Input, duration),
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
                                                    (worker, TimelineEvent::Parked, duration),
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
                    })
                }
            },
        );

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
                                            event_map.insert(
                                                (worker, event),
                                                (time, capability.clone()),
                                            );
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
                                                    TimelineEvent::Merge {
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

                                    DifferentialEvent::Batch(_)
                                    | DifferentialEvent::Drop(_)
                                    | DifferentialEvent::MergeShortfall(_)
                                    | DifferentialEvent::TraceShare(_) => {}
                                }
                            }
                        })
                    }
                },
            )
        });

        differential_events
            .map(|differential_events| timely_events.concat(&differential_events))
            .unwrap_or(timely_events)
            .as_collection()
            .leave_region()
    })
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
pub enum TimelineEvent {
    OperatorActivation { operator_id: usize },
    Application,
    Parked,
    Input,
    Message,
    Progress,
    Merge { operator_id: usize },
}
