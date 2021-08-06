use super::ReachabilityLogBundle;
use abomonation_derive::Abomonation;
use std::{collections::HashMap, time::Duration};
use timely::dataflow::{
    channels::pact::Pipeline,
    operators::{
        flow_controlled::{iterator_source, IteratorSourceInput},
        probe::{Handle, Probe},
        Operator, Reclock,
    },
    Scope, Stream,
};

pub fn display_reachability<S>(
    scope: &mut S,
    window_size: Time,
    reachability_trace: &Stream<S, ReachabilityLogBundle>,
) where
    S: Scope<Timestamp = Time>,
{
    let mut clock_probe = Handle::new();
    let clock = iterator_source(
        scope,
        "Reachability Clock",
        move |&previous_time| {
            Some(IteratorSourceInput {
                lower_bound: Default::default(),
                data: vec![(previous_time + window_size, vec![()])],
                target: previous_time,
            })
        },
        clock_probe.clone(),
    );

    reachability_trace
        .probe_with(&mut clock_probe)
        .reclock(&clock)
        .sink(Pipeline, "Display Reachability Updates", |input| {
            let mut buffer = Vec::new();
            let mut capabilities = HashMap::with_hasher(XXHasher::default());

            input.for_each(move |_capability, data| {
                data.swap(&mut buffer);

                for (_time, _worker, event) in buffer.drain(..) {
                    match event {
                        TrackerEvent::SourceUpdate(SourceUpdate {
                            tracker_id,
                            updates,
                        }) => {
                            let tracker_map =
                                capabilities.entry(tracker_id).or_insert_with(HashMap::new);

                            for (node, port, time, diff) in updates {
                                tracker_map
                                    .entry((node, port, time))
                                    .and_modify(|current_diff| *current_diff += diff)
                                    .or_insert(diff);
                            }
                        }

                        TrackerEvent::TargetUpdate(TargetUpdate {
                            tracker_id,
                            updates,
                        }) => {
                            let tracker_map =
                                capabilities.entry(tracker_id).or_insert_with(HashMap::new);

                            for (node, port, time, diff) in updates {
                                tracker_map
                                    .entry((node, port, time))
                                    .and_modify(|current_diff| *current_diff += diff)
                                    .or_insert(diff);
                            }
                        }
                    }
                }
            });
        })
}

/// Events that the tracker may record.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub enum TrackerEvent {
    /// Updates made at a source of data.
    SourceUpdate(SourceUpdate),
    /// Updates made at a target of data.
    TargetUpdate(TargetUpdate),
}

/// An update made at a source of data.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct SourceUpdate {
    /// An identifier for the tracker.
    pub tracker_id: Vec<usize>,
    /// Updates themselves, as `(node, port, time, diff)`.
    pub updates: Vec<(usize, usize, SerializedTime, i64)>,
}

/// An update made at a target of data.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct TargetUpdate {
    /// An identifier for the tracker.
    pub tracker_id: Vec<usize>,
    /// Updates themselves, as `(node, port, time, diff)`.
    pub updates: Vec<(usize, usize, SerializedTime, i64)>,
}

impl From<SourceUpdate> for TrackerEvent {
    fn from(source: SourceUpdate) -> TrackerEvent {
        Self::SourceUpdate(source)
    }
}

impl From<TargetUpdate> for TrackerEvent {
    fn from(target: TargetUpdate) -> TrackerEvent {
        Self::TargetUpdate(target)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct SerializedTime {
    pub timestamp: String,
    pub type_name: String,
}
