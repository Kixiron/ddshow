use crate::dataflow::{
    utils::{granulate, ProgressLogBundle},
    Diff,
};
use abomonation_derive::Abomonation;
use ddshow_types::{timely_logging::ChannelsEvent, ChannelId, OperatorAddr, WorkerId};
use differential_dataflow::{
    difference::DiffPair,
    operators::{CountTotal, Join},
    AsCollection, Collection,
};
use std::time::Duration;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
enum ConnectionKind {
    Target,
    Source,
}

pub fn aggregate_channel_messages<S>(
    _scope: &mut S,
    progress_stream: &Stream<S, ProgressLogBundle>,
    channel_events: &Collection<S, (WorkerId, ChannelsEvent), Diff>,
) -> Collection<S, (ChannelId, (usize, usize)), Diff>
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
                let diff = DiffPair::new(1, message.diff);

                (data, time, diff)
            })
        })
        .as_collection()
        .delay(granulate)
        .count_total()
        .map(
            |(
                (node, port, addr, kind),
                DiffPair {
                    element1: total_messages,
                    element2: total_diff,
                },
            )| { ((node, port, addr, kind), (total_messages, total_diff)) },
        );

    let _capability_updates = progress_stream
        .flat_map(|(time, _dest_worker, event)| {
            let (addr, is_send) = (event.addr, event.is_send);

            event.internal.into_iter().map(move |capability| {
                let kind = if is_send {
                    ConnectionKind::Source
                } else {
                    ConnectionKind::Target
                };

                let data = (capability.node, capability.port, addr.clone(), kind);
                let diff = DiffPair::new(1, capability.diff);

                (data, time, diff)
            })
        })
        .as_collection()
        .delay(granulate)
        .count_total()
        .map(
            |(
                (node, port, addr, kind),
                DiffPair {
                    element1: total_messages,
                    element2: total_diff,
                },
            )| { ((node, port, addr, kind), (total_messages, total_diff)) },
        );

    let channels_by_connections = channel_events.flat_map(|(_worker, channel)| {
        vec![
            (
                (
                    channel.source.0,
                    channel.source.1,
                    channel.scope_addr.clone(),
                    ConnectionKind::Source,
                ),
                (),
            ),
            (
                (
                    channel.target.0,
                    channel.target.1,
                    channel.scope_addr,
                    ConnectionKind::Target,
                ),
                (),
            ),
        ]
    });

    message_updates
        .join(&channels_by_connections)
        .inspect(|(data, _, _)| println!("{:?}", data));

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
