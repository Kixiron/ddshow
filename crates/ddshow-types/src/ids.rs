//! All dataflow related ids

#[cfg(feature = "enable_abomonation")]
use abomonation_derive::Abomonation;
#[cfg(feature = "rkyv")]
use bytecheck::CheckBytes;
#[cfg(feature = "rkyv")]
use rkyv_dep as rkyv;
#[cfg(feature = "rkyv")]
use rkyv_dep::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
#[cfg(feature = "serde")]
use serde_dep::{Deserialize as SerdeDeserialize, Serialize as SerdeSerialize};
use std::fmt::{self, Debug, Display};
use timely::logging::WorkerIdentifier;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
#[cfg_attr(feature = "serde", serde(crate = "serde_dep", transparent))]
#[cfg_attr(feature = "rkyv", derive(Archive, RkyvSerialize, RkyvDeserialize))]
#[cfg_attr(feature = "rkyv", archive(strict, derive(CheckBytes)))]
#[cfg_attr(feature = "enable_abomonation", derive(Abomonation))]
#[repr(transparent)]
pub struct WorkerId {
    worker: WorkerIdentifier,
}

impl WorkerId {
    pub const fn new(worker: WorkerIdentifier) -> Self {
        Self { worker }
    }

    pub const fn into_inner(self) -> WorkerIdentifier {
        self.worker
    }
}

impl From<usize> for WorkerId {
    fn from(worker: usize) -> Self {
        Self::new(worker)
    }
}

impl Debug for WorkerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WorkerId({})", self.worker)
    }
}

impl Display for WorkerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.worker, f)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
#[cfg_attr(feature = "serde", serde(crate = "serde_dep", transparent))]
#[cfg_attr(feature = "rkyv", derive(Archive, RkyvSerialize, RkyvDeserialize))]
#[cfg_attr(feature = "rkyv", archive(strict, derive(CheckBytes)))]
#[cfg_attr(feature = "enable_abomonation", derive(Abomonation))]
#[repr(transparent)]
pub struct OperatorId {
    operator: usize,
}

impl OperatorId {
    pub const fn new(operator: usize) -> Self {
        Self { operator }
    }

    pub const fn into_inner(self) -> usize {
        self.operator
    }
}

impl Debug for OperatorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OperatorId({})", self.operator)
    }
}

impl Display for OperatorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.operator, f)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
#[cfg_attr(feature = "serde", serde(crate = "serde_dep", transparent))]
#[cfg_attr(feature = "rkyv", derive(Archive, RkyvSerialize, RkyvDeserialize))]
#[cfg_attr(feature = "rkyv", archive(strict, derive(CheckBytes)))]
#[cfg_attr(feature = "enable_abomonation", derive(Abomonation))]
#[repr(transparent)]
pub struct PortId {
    port: usize,
}

impl PortId {
    pub const fn new(port: usize) -> Self {
        Self { port }
    }

    pub const fn into_inner(self) -> usize {
        self.port
    }

    pub const fn zero() -> Self {
        Self::new(0)
    }

    pub const fn is_zero(&self) -> bool {
        self.port == 0
    }
}

impl Debug for PortId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PortId({})", self.port)
    }
}

impl Display for PortId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.port, f)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
#[cfg_attr(feature = "serde", serde(crate = "serde_dep", transparent))]
#[cfg_attr(feature = "rkyv", derive(Archive, RkyvSerialize, RkyvDeserialize))]
#[cfg_attr(feature = "rkyv", archive(strict, derive(CheckBytes)))]
#[cfg_attr(feature = "enable_abomonation", derive(Abomonation))]
#[repr(transparent)]
pub struct ChannelId {
    channel: usize,
}

impl ChannelId {
    pub const fn new(channel: usize) -> Self {
        Self { channel }
    }

    pub const fn into_inner(self) -> usize {
        self.channel
    }
}

impl Debug for ChannelId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ChannelId({})", self.channel)
    }
}

impl Display for ChannelId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.channel, f)
    }
}
