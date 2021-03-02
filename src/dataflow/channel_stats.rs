use super::{Address, Diff, FilterMap, Max, Min, TimelyLogBundle};
use abomonation_derive::Abomonation;
use differential_dataflow::{
    difference::DiffPair,
    operators::{Count, Join},
    AsCollection, Collection,
};
use std::time::Duration;
use timely::{
    dataflow::{
        operators::{Enter, Filter, Map},
        Scope, Stream,
    },
    logging::TimelyEvent,
};

pub fn coagulate_channel_messages<S>(
    scope: &mut S,
    log_stream: &Stream<S, TimelyLogBundle>,
) -> Collection<S, ((usize, usize, usize), ChannelMessageStats), Diff>
where
    S: Scope<Timestamp = Duration>,
{
    scope.region_named("Collect Channel Messages", |region| {
        let message_events = log_stream
            .enter(region)
            .filter_map(|(time, _worker, event)| {
                if let TimelyEvent::Messages(event) = event {
                    if event.is_send {
                        return Some((event, time, 1));
                    }
                }

                None
            })
            .as_collection();

        message_events
            .explode(|(channel_addr, capability_updates)| {
                Some((
                    channel_addr,
                    DiffPair::new(
                        1,
                        DiffPair::new(
                            capability_updates as isize,
                            DiffPair::new(
                                Min::new(capability_updates as isize),
                                Max::new(capability_updates as isize),
                            ),
                        ),
                    ),
                ))
            })
            .count_total()
            .map(
                |(
                    channel_addr,
                    DiffPair {
                        element1: count,
                        element2:
                            DiffPair {
                                element1: total,
                                element2:
                                    DiffPair {
                                        element1: min,
                                        element2: max,
                                    },
                            },
                    },
                )| {
                    (
                        channel_addr.clone(),
                        ChannelCapabilityStats {
                            channel_addr,
                            max: max.value as usize,
                            min: min.value as usize,
                            total: total as usize,
                            average: (total as f32 / count as f32).to_bits(),
                            invocations: count as usize,
                        },
                    )
                },
            )
            .leave_region()
    })
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct ChannelMessageStats {
    pub channel_addr: Address,
    pub max: usize,
    pub min: usize,
    pub total: usize,
    /// An [`f32`] stored in bit form representing the average # of records per invocation
    pub average: u32,
    pub invocations: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct ChannelCapabilityStats {
    pub channel_addr: Address,
    pub max: usize,
    pub min: usize,
    pub total: usize,
    /// An [`f32`] stored in bit form representing the average # of records per invocation
    pub average: u32,
    pub invocations: usize,
}
