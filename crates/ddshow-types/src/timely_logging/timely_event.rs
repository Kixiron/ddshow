use crate::{
    timely_logging::{
        ApplicationEvent, ChannelsEvent, CommChannelsEvent, GuardedMessageEvent,
        GuardedProgressEvent, InputEvent, MessagesEvent, OperatesEvent, ParkEvent,
        PushProgressEvent, ScheduleEvent, ShutdownEvent,
    },
    ChannelId, OperatorId,
};
#[cfg(feature = "enable_abomonation")]
use abomonation_derive::Abomonation;
use core::fmt::Debug;
#[cfg(feature = "rkyv")]
use rkyv_dep::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
#[cfg(feature = "serde")]
use serde_dep::{Deserialize as SerdeDeserialize, Serialize as SerdeSerialize};
use timely::logging::TimelyEvent as RawTimelyEvent;

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
pub enum TimelyEvent {
    Operates(OperatesEvent),
    Channels(ChannelsEvent),
    PushProgress(PushProgressEvent),
    Messages(MessagesEvent),
    Schedule(ScheduleEvent),
    Shutdown(ShutdownEvent),
    Application(ApplicationEvent),
    GuardedMessage(GuardedMessageEvent),
    GuardedProgress(GuardedProgressEvent),
    CommChannels(CommChannelsEvent),
    Input(InputEvent),
    Park(ParkEvent),
    Text(String),
}

impl TimelyEvent {
    /// Returns `true` if the timely_event is [`TimelyEvent::Operates`].
    #[inline]
    pub const fn is_operates(&self) -> bool {
        matches!(self, Self::Operates(..))
    }

    /// Returns `true` if the timely_event is [`TimelyEvent::Channels`].
    #[inline]
    pub const fn is_channels(&self) -> bool {
        matches!(self, Self::Channels(..))
    }

    /// Returns `true` if the timely_event is [`TimelyEvent::PushProgress`].
    #[inline]
    pub const fn is_push_progress(&self) -> bool {
        matches!(self, Self::PushProgress(..))
    }

    /// Returns `true` if the timely_event is [`TimelyEvent::Messages`].
    #[inline]
    pub const fn is_messages(&self) -> bool {
        matches!(self, Self::Messages(..))
    }

    /// Returns `true` if the timely_event is [`TimelyEvent::Schedule`].
    #[inline]
    pub const fn is_schedule(&self) -> bool {
        matches!(self, Self::Schedule(..))
    }

    /// Returns `true` if the timely_event is [`TimelyEvent::Shutdown`].
    #[inline]
    pub const fn is_shutdown(&self) -> bool {
        matches!(self, Self::Shutdown(..))
    }

    /// Returns `true` if the timely_event is [`TimelyEvent::Application`].
    #[inline]
    pub const fn is_application(&self) -> bool {
        matches!(self, Self::Application(..))
    }

    /// Returns `true` if the timely_event is [`TimelyEvent::GuardedMessage`].
    #[inline]
    pub const fn is_guarded_message(&self) -> bool {
        matches!(self, Self::GuardedMessage(..))
    }

    /// Returns `true` if the timely_event is [`TimelyEvent::GuardedProgress`].
    #[inline]
    pub const fn is_guarded_progress(&self) -> bool {
        matches!(self, Self::GuardedProgress(..))
    }

    /// Returns `true` if the timely_event is [`TimelyEvent::CommChannels`].
    #[inline]
    pub const fn is_comm_channels(&self) -> bool {
        matches!(self, Self::CommChannels(..))
    }

    /// Returns `true` if the timely_event is [`TimelyEvent::Input`].
    #[inline]
    pub const fn is_input(&self) -> bool {
        matches!(self, Self::Input(..))
    }

    /// Returns `true` if the timely_event is [`TimelyEvent::Park`].
    #[inline]
    pub const fn is_park(&self) -> bool {
        matches!(self, Self::Park(..))
    }

    /// Returns `true` if the timely_event is [`TimelyEvent::Text`].
    #[inline]
    pub const fn is_text(&self) -> bool {
        matches!(self, Self::Text(..))
    }

    #[inline]
    pub const fn as_operates(&self) -> Option<&OperatesEvent> {
        if let Self::Operates(v) = self {
            Some(v)
        } else {
            None
        }
    }

    #[inline]
    pub fn try_into_operates(self) -> Result<OperatesEvent, Self> {
        if let Self::Operates(v) = self {
            Ok(v)
        } else {
            Err(self)
        }
    }

    #[inline]
    pub const fn as_channels(&self) -> Option<&ChannelsEvent> {
        if let Self::Channels(v) = self {
            Some(v)
        } else {
            None
        }
    }

    #[inline]
    pub fn try_into_channels(self) -> Result<ChannelsEvent, Self> {
        if let Self::Channels(v) = self {
            Ok(v)
        } else {
            Err(self)
        }
    }

    #[inline]
    pub const fn as_push_progress(&self) -> Option<&PushProgressEvent> {
        if let Self::PushProgress(v) = self {
            Some(v)
        } else {
            None
        }
    }

    #[inline]
    pub fn try_into_push_progress(self) -> Result<PushProgressEvent, Self> {
        if let Self::PushProgress(v) = self {
            Ok(v)
        } else {
            Err(self)
        }
    }

    #[inline]
    pub const fn as_messages(&self) -> Option<&MessagesEvent> {
        if let Self::Messages(v) = self {
            Some(v)
        } else {
            None
        }
    }

    #[inline]
    pub fn try_into_messages(self) -> Result<MessagesEvent, Self> {
        if let Self::Messages(v) = self {
            Ok(v)
        } else {
            Err(self)
        }
    }

    #[inline]
    pub const fn as_schedule(&self) -> Option<&ScheduleEvent> {
        if let Self::Schedule(v) = self {
            Some(v)
        } else {
            None
        }
    }

    #[inline]
    pub fn try_into_schedule(self) -> Result<ScheduleEvent, Self> {
        if let Self::Schedule(v) = self {
            Ok(v)
        } else {
            Err(self)
        }
    }

    #[inline]
    pub const fn as_shutdown(&self) -> Option<&ShutdownEvent> {
        if let Self::Shutdown(v) = self {
            Some(v)
        } else {
            None
        }
    }

    #[inline]
    pub fn try_into_shutdown(self) -> Result<ShutdownEvent, Self> {
        if let Self::Shutdown(v) = self {
            Ok(v)
        } else {
            Err(self)
        }
    }

    #[inline]
    pub const fn as_application(&self) -> Option<&ApplicationEvent> {
        if let Self::Application(v) = self {
            Some(v)
        } else {
            None
        }
    }

    #[inline]
    pub fn try_into_application(self) -> Result<ApplicationEvent, Self> {
        if let Self::Application(v) = self {
            Ok(v)
        } else {
            Err(self)
        }
    }

    #[inline]
    pub const fn as_guarded_message(&self) -> Option<&GuardedMessageEvent> {
        if let Self::GuardedMessage(v) = self {
            Some(v)
        } else {
            None
        }
    }

    #[inline]
    pub fn try_into_guarded_message(self) -> Result<GuardedMessageEvent, Self> {
        if let Self::GuardedMessage(v) = self {
            Ok(v)
        } else {
            Err(self)
        }
    }

    #[inline]
    pub const fn as_guarded_progress(&self) -> Option<&GuardedProgressEvent> {
        if let Self::GuardedProgress(v) = self {
            Some(v)
        } else {
            None
        }
    }

    #[inline]
    pub fn try_into_guarded_progress(self) -> Result<GuardedProgressEvent, Self> {
        if let Self::GuardedProgress(v) = self {
            Ok(v)
        } else {
            Err(self)
        }
    }

    #[inline]
    pub const fn as_comm_channels(&self) -> Option<&CommChannelsEvent> {
        if let Self::CommChannels(v) = self {
            Some(v)
        } else {
            None
        }
    }

    #[inline]
    pub fn try_into_comm_channels(self) -> Result<CommChannelsEvent, Self> {
        if let Self::CommChannels(v) = self {
            Ok(v)
        } else {
            Err(self)
        }
    }

    #[inline]
    pub const fn as_input(&self) -> Option<&InputEvent> {
        if let Self::Input(v) = self {
            Some(v)
        } else {
            None
        }
    }

    #[inline]
    pub fn try_into_input(self) -> Result<InputEvent, Self> {
        if let Self::Input(v) = self {
            Ok(v)
        } else {
            Err(self)
        }
    }

    #[inline]
    pub const fn as_park(&self) -> Option<&ParkEvent> {
        if let Self::Park(v) = self {
            Some(v)
        } else {
            None
        }
    }

    #[inline]
    pub fn try_into_park(self) -> Result<ParkEvent, Self> {
        if let Self::Park(v) = self {
            Ok(v)
        } else {
            Err(self)
        }
    }

    #[inline]
    pub const fn as_text(&self) -> Option<&String> {
        if let Self::Text(v) = self {
            Some(v)
        } else {
            None
        }
    }

    #[inline]
    pub fn try_into_text(self) -> Result<String, Self> {
        if let Self::Text(v) = self {
            Ok(v)
        } else {
            Err(self)
        }
    }

    #[inline]
    pub const fn distinguishing_id(&self) -> DistinguishingId {
        match *self {
            Self::Operates(OperatesEvent { id, .. }) | Self::Shutdown(ShutdownEvent { id }) => {
                DistinguishingId::OperatorExists(id)
            }
            Self::Schedule(ScheduleEvent { id, .. }) => DistinguishingId::OperatorSchedule(id),
            Self::Channels(ChannelsEvent { id, .. }) => DistinguishingId::Channel(id),
            Self::PushProgress(PushProgressEvent { op_id }) => {
                DistinguishingId::PushProgress(op_id)
            }
            Self::Messages(MessagesEvent {
                channel,
                source,
                target,
                ..
            }) => DistinguishingId::Messages(channel, source, target),
            Self::Application(ApplicationEvent { id, .. }) => DistinguishingId::Application(id),
            Self::GuardedMessage(_) => DistinguishingId::GuardedMessage,
            Self::GuardedProgress(_) => DistinguishingId::GuardedProgress,
            Self::CommChannels(CommChannelsEvent { identifier, .. }) => {
                DistinguishingId::CommChannel(identifier)
            }
            Self::Input(_) => DistinguishingId::Input,
            Self::Park(_) => DistinguishingId::Park,
            Self::Text(_) => DistinguishingId::Text,
        }
    }
}

/// Distinguishes a [`TimelyEvent`]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DistinguishingId {
    OperatorExists(OperatorId),
    OperatorSchedule(OperatorId),
    Channel(ChannelId),
    PushProgress(OperatorId),
    Messages(ChannelId, OperatorId, OperatorId),
    Application(usize),
    GuardedMessage,
    GuardedProgress,
    CommChannel(usize),
    Input,
    Park,
    Text,
}

impl From<RawTimelyEvent> for TimelyEvent {
    #[inline]
    fn from(event: RawTimelyEvent) -> Self {
        match event {
            RawTimelyEvent::Operates(operates) => Self::Operates(operates.into()),
            RawTimelyEvent::Channels(channels) => Self::Channels(channels.into()),
            RawTimelyEvent::PushProgress(progress) => Self::PushProgress(progress.into()),
            RawTimelyEvent::Messages(inner) => Self::Messages(inner.into()),
            RawTimelyEvent::Schedule(inner) => Self::Schedule(inner.into()),
            RawTimelyEvent::Shutdown(inner) => Self::Shutdown(inner.into()),
            RawTimelyEvent::Application(inner) => Self::Application(inner.into()),
            RawTimelyEvent::GuardedMessage(inner) => Self::GuardedMessage(inner.into()),
            RawTimelyEvent::GuardedProgress(inner) => Self::GuardedProgress(inner.into()),
            RawTimelyEvent::CommChannels(inner) => Self::CommChannels(inner.into()),
            RawTimelyEvent::Input(inner) => Self::Input(inner.into()),
            RawTimelyEvent::Park(inner) => Self::Park(inner.into()),
            RawTimelyEvent::Text(text) => Self::Text(text),
        }
    }
}

impl From<ParkEvent> for TimelyEvent {
    #[inline]
    fn from(v: ParkEvent) -> Self {
        Self::Park(v)
    }
}

impl From<InputEvent> for TimelyEvent {
    #[inline]
    fn from(v: InputEvent) -> Self {
        Self::Input(v)
    }
}

impl From<CommChannelsEvent> for TimelyEvent {
    #[inline]
    fn from(v: CommChannelsEvent) -> Self {
        Self::CommChannels(v)
    }
}

impl From<GuardedProgressEvent> for TimelyEvent {
    #[inline]
    fn from(v: GuardedProgressEvent) -> Self {
        Self::GuardedProgress(v)
    }
}

impl From<GuardedMessageEvent> for TimelyEvent {
    #[inline]
    fn from(v: GuardedMessageEvent) -> Self {
        Self::GuardedMessage(v)
    }
}

impl From<ApplicationEvent> for TimelyEvent {
    #[inline]
    fn from(v: ApplicationEvent) -> Self {
        Self::Application(v)
    }
}

impl From<ShutdownEvent> for TimelyEvent {
    #[inline]
    fn from(v: ShutdownEvent) -> Self {
        Self::Shutdown(v)
    }
}

impl From<ScheduleEvent> for TimelyEvent {
    #[inline]
    fn from(v: ScheduleEvent) -> Self {
        Self::Schedule(v)
    }
}

impl From<MessagesEvent> for TimelyEvent {
    #[inline]
    fn from(v: MessagesEvent) -> Self {
        Self::Messages(v)
    }
}

impl From<PushProgressEvent> for TimelyEvent {
    #[inline]
    fn from(v: PushProgressEvent) -> Self {
        Self::PushProgress(v)
    }
}

impl From<ChannelsEvent> for TimelyEvent {
    #[inline]
    fn from(v: ChannelsEvent) -> Self {
        Self::Channels(v)
    }
}

impl From<OperatesEvent> for TimelyEvent {
    #[inline]
    fn from(v: OperatesEvent) -> Self {
        Self::Operates(v)
    }
}
