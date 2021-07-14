//! Timely progress events

use crate::WorkerId;
use core::time::Duration;
use timely::dataflow::operators::capture::event::Event as TimelyEvent;

#[cfg(feature = "enable_abomonation")]
use abomonation_derive::Abomonation;

#[cfg(feature = "rkyv")]
use rkyv_dep::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

#[cfg(feature = "serde")]
use serde_dep::{Deserialize as SerdeDeserialize, Serialize as SerdeSerialize};

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
pub struct Bundle<D, Id = WorkerId> {
    pub time: Duration,
    pub worker: Id,
    pub event: D,
}

impl<D, Id> Bundle<D, Id> {
    pub fn new(time: Duration, worker: Id, event: D) -> Self {
        Self {
            time,
            worker,
            event,
        }
    }
}

impl<D, Id> From<(Duration, Id, D)> for Bundle<D, Id> {
    fn from((time, worker, event): (Duration, Id, D)) -> Self {
        Self {
            time,
            worker,
            event,
        }
    }
}

impl<D, Id> From<Bundle<D, Id>> for (Duration, Id, D) {
    fn from(
        Bundle {
            time,
            worker,
            event,
        }: Bundle<D, Id>,
    ) -> Self {
        (time, worker, event)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
pub struct CapabilityBundle<T> {
    pub time: T,
    pub diff: i64,
}

impl<T> CapabilityBundle<T> {
    pub fn new(time: T, diff: i64) -> Self {
        Self { time, diff }
    }
}

impl<T> From<(T, i64)> for CapabilityBundle<T> {
    fn from((time, diff): (T, i64)) -> Self {
        Self { time, diff }
    }
}

impl<T> From<CapabilityBundle<T>> for (T, i64) {
    fn from(CapabilityBundle { time, diff }: CapabilityBundle<T>) -> Self {
        (time, diff)
    }
}

/// Data and progress events of a captured stream.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
