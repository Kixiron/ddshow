use crate::dataflow::{
    operators::{Max, Min},
    Diff,
};
use abomonation_derive::Abomonation;
use ddshow_types::{ChannelId, OperatorAddr, WorkerId};
use differential_dataflow::{
    difference::DiffPair, operators::CountTotal, AsCollection, Collection,
};
use std::time::Duration;
use timely::dataflow::{
    operators::{Enter, Map},
    Scope, Stream,
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct ChannelMessageStats {
    pub channel: ChannelId,
    pub worker: WorkerId,
    pub max: usize,
    pub min: usize,
    pub total: usize,
    pub average: usize,
    pub invocations: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct ChannelCapabilityStats {
    pub channel: ChannelId,
    pub worker: WorkerId,
    pub max: usize,
    pub min: usize,
    pub total: usize,
    pub average: usize,
    pub invocations: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub enum Channel {
    ScopeCrossing {
        channel_id: ChannelId,
        source_addr: OperatorAddr,
        target_addr: OperatorAddr,
    },

    Normal {
        channel_id: ChannelId,
        source_addr: OperatorAddr,
        target_addr: OperatorAddr,
    },
}

impl Channel {
    pub const fn channel_id(&self) -> ChannelId {
        match *self {
            Self::ScopeCrossing { channel_id, .. } | Self::Normal { channel_id, .. } => channel_id,
        }
    }

    pub fn source_addr(&self) -> OperatorAddr {
        match self {
            Self::ScopeCrossing { source_addr, .. } | Self::Normal { source_addr, .. } => {
                source_addr.to_owned()
            }
        }
    }

    pub fn target_addr(&self) -> OperatorAddr {
        match self {
            Self::ScopeCrossing { target_addr, .. } | Self::Normal { target_addr, .. } => {
                target_addr.to_owned()
            }
        }
    }
}

pub fn aggregate_channel_messages<S>(
    scope: &mut S,
    channel_messages: &Stream<S, ((WorkerId, ChannelId), usize, Duration)>,
) -> Collection<S, ((WorkerId, ChannelId), ChannelMessageStats), Diff>
where
    S: Scope<Timestamp = Duration>,
{
    scope.region_named("Aggregate Channel Messages", |region| {
        let channel_messages = channel_messages.enter(region);

        channel_messages
            .map(|((worker, channel), sent_tuples, time)| {
                (
                    (worker, channel),
                    time,
                    DiffPair::new(
                        1,
                        DiffPair::new(
                            sent_tuples as isize,
                            DiffPair::new(
                                Min::new(sent_tuples as isize),
                                Max::new(sent_tuples as isize),
                            ),
                        ),
                    ),
                )
            })
            .as_collection()
            .count_total()
            .map(
                |(
                    (worker, channel),
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
                    let (total, count) = (total as usize, count as usize);
                    let stats = ChannelMessageStats {
                        channel,
                        worker,
                        max: max.value as usize,
                        min: min.value as usize,
                        total,
                        average: total.checked_div(count).unwrap_or(0),
                        invocations: count,
                    };

                    ((worker, channel), stats)
                },
            )
            .leave_region()
    })
}
