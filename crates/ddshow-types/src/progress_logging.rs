//! Timely progress events

use crate::{
    ids::{ChannelId, PortId, WorkerId},
    OperatorAddr,
};
use timely::logging::TimelyProgressEvent as RawTimelyProgressEvent;

#[cfg(feature = "enable_abomonation")]
use abomonation_derive::Abomonation;

#[cfg(feature = "rkyv")]
use rkyv_dep::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

#[cfg(feature = "serde")]
use serde_dep::{Deserialize as SerdeDeserialize, Serialize as SerdeSerialize};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[cfg_attr(
    feature = "serde",
    derive(SerdeSerialize, SerdeDeserialize),
    serde(crate = "serde_dep")
)]
#[cfg_attr(
    feature = "rkyv",
    derive(Archive, RkyvSerialize, RkyvDeserialize),
    archive(crate = "rkyv_dep"),
    archive_attr(derive(bytecheck::CheckBytes))
)]
#[cfg_attr(feature = "enable_abomonation", derive(Abomonation))]
pub struct TimelyProgressEvent {
    /// `true` if the event is a send, and `false` if it is a receive.
    pub is_send: bool,
    /// Source worker index.
    pub worker: WorkerId,
    /// Communication channel identifier
    pub channel: ChannelId,
    /// Message sequence number.
    pub seq_no: usize,
    /// Sequence of nested scope identifiers indicating the path from the root to this instance.
    pub addr: OperatorAddr,
    /// List of message updates containing Target descriptor, timestamp as string, and delta.
    pub messages: Vec<MessageUpdate>,
    /// List of capability updates containing Source descriptor, timestamp as string, and delta.
    pub internal: Vec<CapabilityUpdate>,
}

impl TimelyProgressEvent {
    pub fn new(
        is_send: bool,
        worker: WorkerId,
        channel: ChannelId,
        seq_no: usize,
        addr: OperatorAddr,
        messages: Vec<MessageUpdate>,
        internal: Vec<CapabilityUpdate>,
    ) -> Self {
        Self {
            is_send,
            worker,
            channel,
            seq_no,
            addr,
            messages,
            internal,
        }
    }
}

impl From<RawTimelyProgressEvent> for TimelyProgressEvent {
    fn from(event: RawTimelyProgressEvent) -> Self {
        let messages = event
            .messages
            .iter()
            .map(|(&node, &port, time, &diff)| {
                MessageUpdate::new(
                    PortId::new(node),
                    PortId::new(port),
                    format!("{:?}", time),
                    time.type_name().to_owned(),
                    diff,
                )
            })
            .collect();

        let internal = event
            .internal
            .iter()
            .map(|(&node, &port, time, &diff)| {
                CapabilityUpdate::new(
                    PortId::new(node),
                    PortId::new(port),
                    format!("{:?}", time),
                    time.type_name().to_owned(),
                    diff,
                )
            })
            .collect();

        Self {
            is_send: event.is_send,
            worker: WorkerId::new(event.source),
            channel: ChannelId::new(event.channel),
            seq_no: event.seq_no,
            addr: OperatorAddr::from(event.addr),
            messages,
            internal,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[cfg_attr(
    feature = "serde",
    derive(SerdeSerialize, SerdeDeserialize),
    serde(crate = "serde_dep")
)]
#[cfg_attr(
    feature = "rkyv",
    derive(Archive, RkyvSerialize, RkyvDeserialize),
    archive(crate = "rkyv_dep"),
    archive_attr(derive(bytecheck::CheckBytes))
)]
#[cfg_attr(feature = "enable_abomonation", derive(Abomonation))]
pub struct MessageUpdate {
    pub node: PortId,
    pub port: PortId,
    /// The update's timestamp, printed via its [`Debug`](`std::fmt::Debug`) implementation
    pub timestamp: String,
    /// The type of the update's timestamp, printed via its [`Any`](`std::any::Any`) implementation
    pub timestamp_type: String,
    /// The number of message updates
    pub diff: i64,
}

impl MessageUpdate {
    pub const fn new(
        node: PortId,
        port: PortId,
        timestamp: String,
        timestamp_type: String,
        diff: i64,
    ) -> Self {
        Self {
            node,
            port,
            timestamp,
            timestamp_type,
            diff,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[cfg_attr(
    feature = "serde",
    derive(SerdeSerialize, SerdeDeserialize),
    serde(crate = "serde_dep")
)]
#[cfg_attr(
    feature = "rkyv",
    derive(Archive, RkyvSerialize, RkyvDeserialize),
    archive(crate = "rkyv_dep"),
    archive_attr(derive(bytecheck::CheckBytes))
)]
#[cfg_attr(feature = "enable_abomonation", derive(Abomonation))]
pub struct CapabilityUpdate {
    pub node: PortId,
    pub port: PortId,
    /// The update's timestamp, printed via its [`Debug`](`std::fmt::Debug`) implementation
    pub timestamp: String,
    /// The type of the update's timestamp, printed via its [`Any`](`std::any::Any`) implementation
    pub timestamp_type: String,
    /// The number of capability updates
    pub diff: i64,
}

impl CapabilityUpdate {
    pub const fn new(
        node: PortId,
        port: PortId,
        timestamp: String,
        timestamp_type: String,
        diff: i64,
    ) -> Self {
        Self {
            node,
            port,
            timestamp,
            timestamp_type,
            diff,
        }
    }
}
