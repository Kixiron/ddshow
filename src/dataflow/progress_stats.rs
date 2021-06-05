use crate::dataflow::{granulate, Diff, ProgressLogBundle};
use abomonation_derive::Abomonation;
use ddshow_types::{ChannelId, OperatorAddr, OperatorId, WorkerId};
use differential_dataflow::{
    difference::DiffPair, operators::CountTotal, AsCollection, Collection,
};
use std::{borrow::Cow, time::Duration};
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
        channel_path: OperatorAddr,
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
    pub fn channel_path(&self) -> Cow<'_, OperatorAddr> {
        match self {
            Self::ScopeCrossing { channel_path, .. } => Cow::Borrowed(channel_path),

            Self::Normal { channel_id, .. } => Cow::Owned(OperatorAddr::from_elem(
                OperatorId::new(channel_id.into_inner()),
            )),
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
    _scope: &mut S,
    progress_stream: &Stream<S, ProgressLogBundle>,
) -> Collection<S, (ChannelId, (usize, usize)), Diff>
where
    S: Scope<Timestamp = Duration>,
{
    // The total messages sent through a channel, aggregated across workers
    progress_stream
        .map(|(time, _worker, progress)| {
            // println!(
            //     "channel {}, scope {}, worker {}, source worker {}\n\tmessages {}\n\tcapability updates {}",
            //     progress.channel, progress.addr, worker, progress.worker,
            //     progress.messages.iter().map(|message| if progress.is_send {
            //         format!("send from port {} at port {}", message.node, message.port)
            //     } else {
            //         format!("recv from port {} at port {}", message.port, message.node)
            //     })
            //     .collect::<Vec<_>>()
            //     .join(", "),
            //     progress.internal.iter().map(|message| if progress.is_send {
            //         format!("send from port {} at port {}", message.node, message.port)
            //     } else {
            //         format!("recv from port {} at port {}", message.port, message.node)
            //     })
            //     .collect::<Vec<_>>()
            //     .join(", "),
            // );

            (
                progress.channel,
                time,
                DiffPair::new(
                    progress.messages.len() as isize,
                    progress.internal.len() as isize,
                ),
            )
        })
        .as_collection()
        .delay(granulate)
        .count_total()
        .map(
            |(
                channel,
                DiffPair {
                    element1: messages,
                    element2: internal,
                },
            )| (channel, (messages as usize, internal as usize)),
        )
}
