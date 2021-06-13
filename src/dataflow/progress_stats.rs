use crate::dataflow::{
    operators::JoinArranged,
    utils::{granulate, ProgressLogBundle},
    Diff,
};
use abomonation_derive::Abomonation;
use ddshow_types::{ChannelId, OperatorAddr, WorkerId};
use differential_dataflow::{
    operators::{
        arrange::{ArrangeByKey, ArrangeBySelf},
        CountTotal, JoinCore, Reduce,
    },
    AsCollection, Collection,
};
use serde::{Deserialize, Serialize};
use std::{iter, time::Duration};
use timely::dataflow::{operators::Map, Scope, Stream};

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
        /// The channel ids of all channels that are part of this single path
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

#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Abomonation,
    Deserialize,
    Serialize,
)]
pub struct ProgressInfo {
    pub consumed: ProgressStats,
    pub produced: ProgressStats,
}

#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Abomonation,
    Deserialize,
    Serialize,
)]
pub struct ProgressStats {
    pub messages: usize,
    pub capability_updates: usize,
}

impl ProgressStats {
    pub const fn new(messages: usize, capability_updates: usize) -> Self {
        Self {
            messages,
            capability_updates,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub enum ConnectionKind {
    Target,
    Source,
}

pub fn aggregate_channel_messages<S>(
    progress_stream: &Stream<S, ProgressLogBundle>,
) -> Collection<S, (OperatorAddr, ProgressInfo), Diff>
where
    S: Scope<Timestamp = Duration>,
{
    let message_updates = progress_stream
        .flat_map(|(time, _dest_worker, event)| {
            let (addr, is_send) = (event.addr, event.is_send);

            event.messages.into_iter().map(move |message| {
                let kind = if is_send {
                    ConnectionKind::Source
                } else {
                    ConnectionKind::Target
                };
                let data = (message.node, message.port, addr.clone(), kind);

                (data, time, 1isize)
            })
        })
        .as_collection()
        .delay(granulate)
        .count_total()
        .arrange_by_key();

    let capability_updates = progress_stream
        .flat_map(|(time, _dest_worker, event)| {
            let (addr, is_send) = (event.addr, event.is_send);

            event.internal.into_iter().map(move |capability| {
                let kind = if is_send {
                    ConnectionKind::Source
                } else {
                    ConnectionKind::Target
                };
                let data = (capability.node, capability.port, addr.clone(), kind);

                (data, time, 1isize)
            })
        })
        .as_collection()
        .delay(granulate)
        .count_total()
        .arrange_by_key();

    let mut combined_updates = message_updates
        .join_core(&capability_updates, |key, &messages, &updates| {
            iter::once((key.clone(), (messages as usize, updates as usize)))
        });
    let combined_updates_arranged = combined_updates.map(|(key, _)| key).arrange_by_self();

    // Add back in any messages or capabilities that only have one side of things
    // (eg. only messages or only capabilities)
    combined_updates = combined_updates.concat(
        &message_updates
            .antijoin_arranged(&combined_updates_arranged)
            .map(|(key, messages)| (key, (messages as usize, 0))),
    );
    combined_updates = combined_updates.concat(
        &capability_updates
            .antijoin_arranged(&combined_updates_arranged)
            .map(|(key, updates)| (key, (0, updates as usize))),
    );

    combined_updates
        .map(|((node, port, address, kind), (messages, updates))| {
            ((node, port, address), (kind, messages, updates))
        })
        .arrange_by_key_named("ArrangeByKey: Combined Updates")
        .reduce(|_, inputs, outputs| {
            let mut info = ProgressInfo::default();
            for &(&(kind, messages, capability_updates), _diff) in inputs {
                let stats = ProgressStats::new(messages, capability_updates);

                match kind {
                    ConnectionKind::Source => info.produced = stats,
                    ConnectionKind::Target => info.consumed = stats,
                }
            }

            // TODO: Should do do something like creating the src/dest addrs here?
            outputs.push((info, 1));
        })
        .flat_map(|((node, port, mut addr), info)| {
            vec![
                (addr.push_imm(node), info),
                (
                    {
                        addr.push(port);
                        addr
                    },
                    info,
                ),
            ]
        })
}
