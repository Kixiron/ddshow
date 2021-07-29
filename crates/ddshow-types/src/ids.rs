//! All dataflow related ids

use core::fmt::{self, Debug, Display, Write};
use timely::logging::WorkerIdentifier;

#[cfg(feature = "enable_abomonation")]
use abomonation_derive::Abomonation;

#[cfg(feature = "rkyv")]
use rkyv_dep::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

#[cfg(feature = "serde")]
use serde_dep::{Deserialize as SerdeDeserialize, Serialize as SerdeSerialize};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[cfg_attr(
    feature = "serde",
    derive(SerdeSerialize, SerdeDeserialize),
    serde(crate = "serde_dep", transparent)
)]
#[cfg_attr(
    feature = "rkyv",
    derive(Archive, RkyvSerialize, RkyvDeserialize),
    archive(crate = "rkyv_dep", repr(transparent)),
    archive_attr(derive(bytecheck::CheckBytes))
)]
#[cfg_attr(feature = "enable_abomonation", derive(Abomonation))]
#[repr(transparent)]
pub struct WorkerId {
    worker: WorkerIdentifier,
}

impl WorkerId {
    #[inline]
    pub const fn new(worker: WorkerIdentifier) -> Self {
        Self { worker }
    }

    #[inline]
    pub const fn into_inner(self) -> WorkerIdentifier {
        self.worker
    }
}

impl From<usize> for WorkerId {
    #[inline]
    fn from(worker: usize) -> Self {
        Self::new(worker)
    }
}

impl Debug for WorkerId {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("WorkerId(")?;
        Display::fmt(&self.worker, f)?;
        f.write_char(')')
    }
}

impl Display for WorkerId {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.worker, f)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[cfg_attr(
    feature = "serde",
    derive(SerdeSerialize, SerdeDeserialize),
    serde(crate = "serde_dep", transparent)
)]
#[cfg_attr(
    feature = "rkyv",
    derive(Archive, RkyvSerialize, RkyvDeserialize),
    archive(crate = "rkyv_dep", repr(transparent)),
    archive_attr(derive(bytecheck::CheckBytes))
)]
#[cfg_attr(feature = "enable_abomonation", derive(Abomonation))]
#[repr(transparent)]
pub struct OperatorId {
    operator: usize,
}

impl OperatorId {
    #[inline]
    pub const fn new(operator: usize) -> Self {
        Self { operator }
    }

    #[inline]
    pub const fn into_inner(self) -> usize {
        self.operator
    }
}

impl From<PortId> for OperatorId {
    #[inline]
    fn from(port: PortId) -> Self {
        Self::new(port.into_inner())
    }
}

impl Debug for OperatorId {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("OperatorId(")?;
        Display::fmt(&self.operator, f)?;
        f.write_char(')')
    }
}

impl Display for OperatorId {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.operator, f)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[cfg_attr(
    feature = "serde",
    derive(SerdeSerialize, SerdeDeserialize),
    serde(crate = "serde_dep", transparent)
)]
#[cfg_attr(
    feature = "rkyv",
    derive(Archive, RkyvSerialize, RkyvDeserialize),
    archive(crate = "rkyv_dep", repr(transparent)),
    archive_attr(derive(bytecheck::CheckBytes))
)]
#[cfg_attr(feature = "enable_abomonation", derive(Abomonation))]
#[repr(transparent)]
pub struct PortId {
    port: usize,
}

impl PortId {
    #[inline]
    pub const fn new(port: usize) -> Self {
        Self { port }
    }

    #[inline]
    pub const fn into_inner(self) -> usize {
        self.port
    }

    #[inline]
    pub const fn zero() -> Self {
        Self::new(0)
    }

    #[inline]
    pub const fn is_zero(&self) -> bool {
        self.port == 0
    }
}

impl Debug for PortId {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PortId(")?;
        Display::fmt(&self.port, f)?;
        f.write_char(')')
    }
}

impl Display for PortId {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.port, f)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[cfg_attr(
    feature = "serde",
    derive(SerdeSerialize, SerdeDeserialize),
    serde(crate = "serde_dep", transparent)
)]
#[cfg_attr(
    feature = "rkyv",
    derive(Archive, RkyvSerialize, RkyvDeserialize),
    archive(crate = "rkyv_dep", repr(transparent)),
    archive_attr(derive(bytecheck::CheckBytes))
)]
#[cfg_attr(feature = "enable_abomonation", derive(Abomonation))]
#[repr(transparent)]
pub struct ChannelId {
    channel: usize,
}

impl ChannelId {
    #[inline]
    pub const fn new(channel: usize) -> Self {
        Self { channel }
    }

    #[inline]
    pub const fn into_inner(self) -> usize {
        self.channel
    }
}

impl Debug for ChannelId {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ChannelId(")?;
        Display::fmt(&self.channel, f)?;
        f.write_char(')')
    }
}

impl Display for ChannelId {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.channel, f)
    }
}
