mod timely_event;

pub use timely_event::{ArchivedTimelyEvent, TimelyEvent, TimelyEventResolver};

use crate::{
    ids::{ChannelId, OperatorId, PortId},
    OperatorAddr,
};
use core::{fmt::Debug, time::Duration};
use timely::logging::{
    ApplicationEvent as TimelyApplicationEvent, ChannelsEvent as TimelyChannelsEvent,
    CommChannelKind as TimelyCommChannelKind, CommChannelsEvent as TimelyCommChannelsEvent,
    GuardedMessageEvent as TimelyGuardedMessageEvent,
    GuardedProgressEvent as TimelyGuardedProgressEvent, InputEvent as TimelyInputEvent,
    MessagesEvent as TimelyMessagesEvent, OperatesEvent as TimelyOperatesEvent,
    ParkEvent as TimelyParkEvent, PushProgressEvent as TimelyPushProgressEvent,
    ScheduleEvent as TimelyScheduleEvent, ShutdownEvent as TimelyShutdownEvent,
    StartStop as TimelyStartStop,
};

#[cfg(feature = "rkyv")]
use rkyv_dep::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

#[cfg(feature = "serde")]
use serde_dep::{Deserialize as SerdeDeserialize, Serialize as SerdeSerialize};

#[cfg(feature = "enable_abomonation")]
use abomonation_derive::Abomonation;

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
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
pub struct OperatesEvent {
    pub id: OperatorId,
    pub addr: OperatorAddr,
    pub name: String,
}

impl OperatesEvent {
    #[inline]
    pub const fn new(id: OperatorId, addr: OperatorAddr, name: String) -> Self {
        Self { id, addr, name }
    }
}

impl From<TimelyOperatesEvent> for OperatesEvent {
    #[inline]
    fn from(event: TimelyOperatesEvent) -> Self {
        Self {
            id: OperatorId::new(event.id),
            addr: OperatorAddr::from(event.addr),
            name: event.name,
        }
    }
}

/// The creation of a channel between two operators
#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
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
pub struct ChannelsEvent {
    /// The id of the channel
    pub id: ChannelId,
    /// The address of the enclosing scope the channel is in
    pub scope_addr: OperatorAddr,
    /// The operator index and output port of the channel's source
    // TODO: Make this a named struct
    pub source: [PortId; 2],
    /// The operator index and input port of the channel's target
    // TODO: Make this a named struct
    pub target: [PortId; 2],
}

impl ChannelsEvent {
    /// Create a new [`ChannelsEvent`]
    #[inline]
    pub const fn new(
        id: ChannelId,
        scope_addr: OperatorAddr,
        source: (PortId, PortId),
        target: (PortId, PortId),
    ) -> Self {
        Self {
            id,
            scope_addr,
            source: [source.0, source.1],
            target: [target.0, target.1],
        }
    }
}

impl From<TimelyChannelsEvent> for ChannelsEvent {
    #[inline]
    fn from(event: TimelyChannelsEvent) -> Self {
        Self {
            id: ChannelId::new(event.id),
            scope_addr: OperatorAddr::from(event.scope_addr),
            source: [PortId::new(event.source.0), PortId::new(event.source.1)],
            target: [PortId::new(event.target.0), PortId::new(event.target.1)],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
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
pub struct PushProgressEvent {
    pub op_id: OperatorId,
}

impl From<TimelyPushProgressEvent> for PushProgressEvent {
    #[inline]
    fn from(event: TimelyPushProgressEvent) -> Self {
        Self {
            op_id: OperatorId::new(event.op_id),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
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
pub struct MessagesEvent {
    /// `true` if send event, `false` if receive event.
    pub is_send: bool,
    /// Channel identifier
    pub channel: ChannelId,
    /// Source worker index.
    pub source: OperatorId,
    /// Target worker index.
    pub target: OperatorId,
    /// Message sequence number.
    pub seq_no: usize,
    /// Number of typed records in the message.
    pub length: usize,
}

impl From<TimelyMessagesEvent> for MessagesEvent {
    #[inline]
    fn from(event: TimelyMessagesEvent) -> Self {
        Self {
            is_send: event.is_send,
            channel: ChannelId::new(event.channel),
            source: OperatorId::new(event.source),
            target: OperatorId::new(event.target),
            seq_no: event.seq_no,
            length: event.length,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
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
pub enum StartStop {
    /// Operator starts
    Start,
    /// Operator stops
    Stop,
}

impl StartStop {
    #[inline]
    pub const fn start() -> Self {
        Self::Start
    }

    #[inline]
    pub const fn stop() -> Self {
        Self::Stop
    }

    /// Returns `true` if the start_stop is [`StartStop::Start`].
    #[inline]
    pub const fn is_start(&self) -> bool {
        matches!(self, Self::Start)
    }

    /// Returns `true` if the start_stop is [`StartStop::Stop`].
    #[inline]
    pub const fn is_stop(&self) -> bool {
        matches!(self, Self::Stop)
    }
}

impl From<TimelyStartStop> for StartStop {
    #[inline]
    fn from(start_stop: TimelyStartStop) -> Self {
        match start_stop {
            TimelyStartStop::Start => Self::Start,
            TimelyStartStop::Stop => Self::Stop,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
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
pub struct ScheduleEvent {
    pub id: OperatorId,
    pub start_stop: StartStop,
}

impl From<TimelyScheduleEvent> for ScheduleEvent {
    #[inline]
    fn from(event: TimelyScheduleEvent) -> Self {
        Self {
            id: OperatorId::new(event.id),
            start_stop: event.start_stop.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
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
pub struct ShutdownEvent {
    pub id: OperatorId,
}

impl From<TimelyShutdownEvent> for ShutdownEvent {
    #[inline]
    fn from(event: TimelyShutdownEvent) -> Self {
        Self {
            id: OperatorId::new(event.id),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
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
pub struct ApplicationEvent {
    pub id: usize,
    // TODO: Make this a `RkyvStartStop`?
    pub is_start: bool,
}

impl From<TimelyApplicationEvent> for ApplicationEvent {
    #[inline]
    fn from(event: TimelyApplicationEvent) -> Self {
        Self {
            id: event.id,
            is_start: event.is_start,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
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
pub struct GuardedMessageEvent {
    // TODO: Make this a `RkyvStartStop`?
    pub is_start: bool,
}

impl From<TimelyGuardedMessageEvent> for GuardedMessageEvent {
    #[inline]
    fn from(event: TimelyGuardedMessageEvent) -> Self {
        Self {
            is_start: event.is_start,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
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
pub struct GuardedProgressEvent {
    // TODO: Make this a `RkyvStartStop`?
    pub is_start: bool,
}

impl From<TimelyGuardedProgressEvent> for GuardedProgressEvent {
    #[inline]
    fn from(event: TimelyGuardedProgressEvent) -> Self {
        Self {
            is_start: event.is_start,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
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
pub struct CommChannelsEvent {
    pub identifier: usize,
    pub kind: CommChannelKind,
}

impl From<TimelyCommChannelsEvent> for CommChannelsEvent {
    #[inline]
    fn from(event: TimelyCommChannelsEvent) -> Self {
        Self {
            identifier: event.identifier,
            kind: event.kind.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
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
pub enum CommChannelKind {
    Progress,
    Data,
}

impl CommChannelKind {
    /// Returns `true` if the comm_channel_kind is [`CommChannelKind::Progress`].
    #[inline]
    pub const fn is_progress(&self) -> bool {
        matches!(self, Self::Progress)
    }

    /// Returns `true` if the comm_channel_kind is [`CommChannelKind::Data`].
    #[inline]
    pub const fn is_data(&self) -> bool {
        matches!(self, Self::Data)
    }
}

impl From<TimelyCommChannelKind> for CommChannelKind {
    #[inline]
    fn from(channel_kind: TimelyCommChannelKind) -> Self {
        match channel_kind {
            TimelyCommChannelKind::Progress => Self::Progress,
            TimelyCommChannelKind::Data => Self::Data,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
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
pub struct InputEvent {
    pub start_stop: StartStop,
}

impl InputEvent {
    #[inline]
    pub const fn new(start_stop: StartStop) -> Self {
        Self { start_stop }
    }
}

impl From<TimelyInputEvent> for InputEvent {
    #[inline]
    fn from(event: TimelyInputEvent) -> Self {
        Self {
            start_stop: event.start_stop.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
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
pub enum ParkEvent {
    Park(Option<Duration>),
    Unpark,
}

impl ParkEvent {
    /// Returns `true` if the park_event is [`ParkEvent::Park`].
    #[inline]
    pub const fn is_park(&self) -> bool {
        matches!(self, Self::Park(..))
    }

    /// Returns `true` if the park_event is [`ParkEvent::Unpark`].
    #[inline]
    pub const fn is_unpark(&self) -> bool {
        matches!(self, Self::Unpark)
    }

    /// Returns the maximum duration the park event will last for
    #[inline]
    pub const fn as_park(&self) -> Option<&Option<Duration>> {
        if let Self::Park(duration) = self {
            Some(duration)
        } else {
            None
        }
    }
}

impl From<TimelyParkEvent> for ParkEvent {
    #[inline]
    fn from(park: TimelyParkEvent) -> Self {
        match park {
            TimelyParkEvent::Park(duration) => Self::Park(duration),
            TimelyParkEvent::Unpark => Self::Unpark,
        }
    }
}
