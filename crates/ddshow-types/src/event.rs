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
use timely::dataflow::operators::capture::event::Event as TimelyEvent;

/// Data and progress events of a captured stream.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "rkyv", derive(Archive, RkyvSerialize, RkyvDeserialize))]
#[cfg_attr(feature = "rkyv", archive(strict, derive(CheckBytes)))]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
#[cfg_attr(feature = "serde", serde(crate = "_serde"))]
#[cfg_attr(feature = "enable_abomonation", derive(Abomonation))]
pub enum Event<T, D> {
    /// Progress received via `push_external_progress`
    Progress(Vec<(T, i64)>),
    /// Messages received via the data stream
    Messages(T, Vec<D>),
}

impl<T, D> Event<T, D> {
    /// Returns `true` if the event is [`Event::Progress`].
    pub const fn is_progress(&self) -> bool {
        matches!(self, Self::Progress(..))
    }

    /// Returns `true` if the event is [`Event::Messages`]
    pub const fn is_messages(&self) -> bool {
        matches!(self, Self::Messages(..))
    }

    /// Returns progress data if the event is [`Event::Progress`]
    pub const fn as_progress(&self) -> Option<&Vec<(T, i64)>> {
        if let Self::Progress(progress) = self {
            Some(progress)
        } else {
            None
        }
    }

    /// Returns message data if the event is [`Event::Messages`]
    pub const fn as_messages(&self) -> Option<(&T, &Vec<D>)> {
        if let Self::Messages(time, messages) = self {
            Some((time, messages))
        } else {
            None
        }
    }
}

impl<T, D> From<TimelyEvent<T, D>> for Event<T, D> {
    fn from(event: TimelyEvent<T, D>) -> Self {
        match event {
            TimelyEvent::Progress(progress) => Self::Progress(progress),
            TimelyEvent::Messages(time, messages) => Self::Messages(time, messages),
        }
    }
}

impl<T, D> From<Event<T, D>> for TimelyEvent<T, D> {
    fn from(val: Event<T, D>) -> Self {
        match val {
            Event::Progress(progress) => Self::Progress(progress),
            Event::Messages(time, messages) => Self::Messages(time, messages),
        }
    }
}
