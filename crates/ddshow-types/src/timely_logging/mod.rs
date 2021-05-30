mod timely_event;

pub use timely_event::{ArchivedTimelyEvent, TimelyEvent, TimelyEventResolver};

use crate::{
    ids::{ChannelId, OperatorId, PortId},
    OperatorAddr,
};
#[cfg(feature = "rkyv")]
use _rkyv as rkyv;
#[cfg(feature = "rkyv")]
use _rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
#[cfg(feature = "serde")]
use _serde::{Deserialize as SerdeDeserialize, Serialize as SerdeSerialize};
#[cfg(feature = "enable_abomonation")]
use abomonation_derive::Abomonation;
#[cfg(feature = "rkyv")]
use bytecheck::CheckBytes;
use std::{fmt::Debug, time::Duration};
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

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
#[cfg_attr(feature = "rkyv", derive(Archive, RkyvSerialize, RkyvDeserialize))]
#[cfg_attr(feature = "rkyv", archive(strict, derive(CheckBytes)))]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
#[cfg_attr(feature = "serde", serde(crate = "_serde"))]
#[cfg_attr(feature = "enable_abomonation", derive(Abomonation))]
pub struct OperatesEvent {
    pub id: OperatorId,
    pub addr: OperatorAddr,
    pub name: String,
}

impl OperatesEvent {
    pub const fn new(id: OperatorId, addr: OperatorAddr, name: String) -> Self {
        Self { id, addr, name }
    }
}

impl From<TimelyOperatesEvent> for OperatesEvent {
    fn from(event: TimelyOperatesEvent) -> Self {
        Self {
            id: OperatorId::new(event.id),
            addr: OperatorAddr::from(event.addr),
            name: event.name,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
#[cfg_attr(feature = "rkyv", derive(Archive, RkyvSerialize, RkyvDeserialize))]
#[cfg_attr(feature = "rkyv", archive(strict, derive(CheckBytes)))]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
#[cfg_attr(feature = "serde", serde(crate = "_serde"))]
#[cfg_attr(feature = "enable_abomonation", derive(Abomonation))]
pub struct ChannelsEvent {
    pub id: ChannelId,
    pub scope_addr: OperatorAddr,
    pub source: (PortId, PortId),
    pub target: (PortId, PortId),
}

impl ChannelsEvent {
    pub const fn new(
        id: ChannelId,
        scope_addr: OperatorAddr,
        source: (PortId, PortId),
        target: (PortId, PortId),
    ) -> Self {
        Self {
            id,
            scope_addr,
            source,
            target,
        }
    }
}

impl From<TimelyChannelsEvent> for ChannelsEvent {
    fn from(event: TimelyChannelsEvent) -> Self {
        Self {
            id: ChannelId::new(event.id),
            scope_addr: OperatorAddr::from(event.scope_addr),
            source: (PortId::new(event.source.0), PortId::new(event.source.1)),
            target: (PortId::new(event.target.0), PortId::new(event.target.1)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
#[cfg_attr(feature = "rkyv", derive(Archive, RkyvSerialize, RkyvDeserialize))]
#[cfg_attr(feature = "rkyv", archive(strict, derive(CheckBytes)))]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
#[cfg_attr(feature = "serde", serde(crate = "_serde"))]
#[cfg_attr(feature = "enable_abomonation", derive(Abomonation))]
pub struct PushProgressEvent {
    pub op_id: OperatorId,
}

impl From<TimelyPushProgressEvent> for PushProgressEvent {
    fn from(event: TimelyPushProgressEvent) -> Self {
        Self {
            op_id: OperatorId::new(event.op_id),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
#[cfg_attr(feature = "rkyv", derive(Archive, RkyvSerialize, RkyvDeserialize))]
#[cfg_attr(feature = "rkyv", archive(strict, derive(CheckBytes)))]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
#[cfg_attr(feature = "serde", serde(crate = "_serde"))]
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
#[cfg_attr(feature = "rkyv", derive(Archive, RkyvSerialize, RkyvDeserialize))]
#[cfg_attr(feature = "rkyv", archive(strict, derive(CheckBytes)))]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
#[cfg_attr(feature = "serde", serde(crate = "_serde"))]
#[cfg_attr(feature = "enable_abomonation", derive(Abomonation))]
pub enum StartStop {
    /// Operator starts
    Start,
    /// Operator stops
    Stop,
}

impl StartStop {
    /// Returns `true` if the start_stop is [`StartStop::Start`].
    pub const fn is_start(&self) -> bool {
        matches!(self, Self::Start)
    }

    /// Returns `true` if the start_stop is [`StartStop::Stop`].
    pub const fn is_stop(&self) -> bool {
        matches!(self, Self::Stop)
    }
}

impl From<TimelyStartStop> for StartStop {
    fn from(start_stop: TimelyStartStop) -> Self {
        match start_stop {
            TimelyStartStop::Start => Self::Start,
            TimelyStartStop::Stop => Self::Stop,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
#[cfg_attr(feature = "rkyv", derive(Archive, RkyvSerialize, RkyvDeserialize))]
#[cfg_attr(feature = "rkyv", archive(strict, derive(CheckBytes)))]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
#[cfg_attr(feature = "serde", serde(crate = "_serde"))]
#[cfg_attr(feature = "enable_abomonation", derive(Abomonation))]
pub struct ScheduleEvent {
    pub id: OperatorId,
    pub start_stop: StartStop,
}

impl From<TimelyScheduleEvent> for ScheduleEvent {
    fn from(event: TimelyScheduleEvent) -> Self {
        Self {
            id: OperatorId::new(event.id),
            start_stop: event.start_stop.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
#[cfg_attr(feature = "rkyv", derive(Archive, RkyvSerialize, RkyvDeserialize))]
#[cfg_attr(feature = "rkyv", archive(strict, derive(CheckBytes)))]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
#[cfg_attr(feature = "serde", serde(crate = "_serde"))]
#[cfg_attr(feature = "enable_abomonation", derive(Abomonation))]
pub struct ShutdownEvent {
    pub id: OperatorId,
}

impl From<TimelyShutdownEvent> for ShutdownEvent {
    fn from(event: TimelyShutdownEvent) -> Self {
        Self {
            id: OperatorId::new(event.id),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
#[cfg_attr(feature = "rkyv", derive(Archive, RkyvSerialize, RkyvDeserialize))]
#[cfg_attr(feature = "rkyv", archive(strict, derive(CheckBytes)))]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
#[cfg_attr(feature = "serde", serde(crate = "_serde"))]
#[cfg_attr(feature = "enable_abomonation", derive(Abomonation))]
pub struct ApplicationEvent {
    pub id: usize,
    // TODO: Make this a `RkyvStartStop`?
    pub is_start: bool,
}

impl From<TimelyApplicationEvent> for ApplicationEvent {
    fn from(event: TimelyApplicationEvent) -> Self {
        Self {
            id: event.id,
            is_start: event.is_start,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
#[cfg_attr(feature = "rkyv", derive(Archive, RkyvSerialize, RkyvDeserialize))]
#[cfg_attr(feature = "rkyv", archive(strict, derive(CheckBytes)))]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
#[cfg_attr(feature = "serde", serde(crate = "_serde"))]
#[cfg_attr(feature = "enable_abomonation", derive(Abomonation))]
pub struct GuardedMessageEvent {
    // TODO: Make this a `RkyvStartStop`?
    pub is_start: bool,
}

impl From<TimelyGuardedMessageEvent> for GuardedMessageEvent {
    fn from(event: TimelyGuardedMessageEvent) -> Self {
        Self {
            is_start: event.is_start,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
#[cfg_attr(feature = "rkyv", derive(Archive, RkyvSerialize, RkyvDeserialize))]
#[cfg_attr(feature = "rkyv", archive(strict, derive(CheckBytes)))]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
#[cfg_attr(feature = "serde", serde(crate = "_serde"))]
#[cfg_attr(feature = "enable_abomonation", derive(Abomonation))]
pub struct GuardedProgressEvent {
    // TODO: Make this a `RkyvStartStop`?
    pub is_start: bool,
}

impl From<TimelyGuardedProgressEvent> for GuardedProgressEvent {
    fn from(event: TimelyGuardedProgressEvent) -> Self {
        Self {
            is_start: event.is_start,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
#[cfg_attr(feature = "rkyv", derive(Archive, RkyvSerialize, RkyvDeserialize))]
#[cfg_attr(feature = "rkyv", archive(strict, derive(CheckBytes)))]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
#[cfg_attr(feature = "serde", serde(crate = "_serde"))]
#[cfg_attr(feature = "enable_abomonation", derive(Abomonation))]
pub struct CommChannelsEvent {
    pub identifier: usize,
    pub kind: CommChannelKind,
}

impl From<TimelyCommChannelsEvent> for CommChannelsEvent {
    fn from(event: TimelyCommChannelsEvent) -> Self {
        Self {
            identifier: event.identifier,
            kind: event.kind.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
#[cfg_attr(feature = "rkyv", derive(Archive, RkyvSerialize, RkyvDeserialize))]
#[cfg_attr(feature = "rkyv", archive(strict, derive(CheckBytes)))]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
#[cfg_attr(feature = "serde", serde(crate = "_serde"))]
#[cfg_attr(feature = "enable_abomonation", derive(Abomonation))]
pub enum CommChannelKind {
    Progress,
    Data,
}

impl CommChannelKind {
    /// Returns `true` if the comm_channel_kind is [`CommChannelKind::Progress`].
    pub const fn is_progress(&self) -> bool {
        matches!(self, Self::Progress)
    }

    /// Returns `true` if the comm_channel_kind is [`CommChannelKind::Data`].
    pub const fn is_data(&self) -> bool {
        matches!(self, Self::Data)
    }
}

impl From<TimelyCommChannelKind> for CommChannelKind {
    fn from(channel_kind: TimelyCommChannelKind) -> Self {
        match channel_kind {
            TimelyCommChannelKind::Progress => Self::Progress,
            TimelyCommChannelKind::Data => Self::Data,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
#[cfg_attr(feature = "rkyv", derive(Archive, RkyvSerialize, RkyvDeserialize))]
#[cfg_attr(feature = "rkyv", archive(strict, derive(CheckBytes)))]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
#[cfg_attr(feature = "serde", serde(crate = "_serde"))]
#[cfg_attr(feature = "enable_abomonation", derive(Abomonation))]
pub struct InputEvent {
    pub start_stop: StartStop,
}

impl From<TimelyInputEvent> for InputEvent {
    fn from(event: TimelyInputEvent) -> Self {
        Self {
            start_stop: event.start_stop.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
#[cfg_attr(feature = "rkyv", derive(Archive, RkyvSerialize, RkyvDeserialize))]
#[cfg_attr(feature = "rkyv", archive(strict, derive(CheckBytes)))]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
#[cfg_attr(feature = "serde", serde(crate = "_serde"))]
#[cfg_attr(feature = "enable_abomonation", derive(Abomonation))]
pub enum ParkEvent {
    Park(Option<Duration>),
    Unpark,
}

impl ParkEvent {
    /// Returns `true` if the park_event is [`ParkEvent::Park`].
    pub const fn is_park(&self) -> bool {
        matches!(self, Self::Park(..))
    }

    /// Returns `true` if the park_event is [`ParkEvent::Unpark`].
    pub const fn is_unpark(&self) -> bool {
        matches!(self, Self::Unpark)
    }

    /// Returns the maximum duration the park event will last for
    pub const fn as_park(&self) -> Option<&Option<Duration>> {
        if let Self::Park(duration) = self {
            Some(duration)
        } else {
            None
        }
    }
}

impl From<TimelyParkEvent> for ParkEvent {
    fn from(park: TimelyParkEvent) -> Self {
        match park {
            TimelyParkEvent::Park(duration) => Self::Park(duration),
            TimelyParkEvent::Unpark => Self::Unpark,
        }
    }
}
