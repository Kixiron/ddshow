mod operator_addr;

pub use operator_addr::{ArchivedOperatorAddr, OperatorAddr, OperatorAddrResolver};

use abomonation_derive::Abomonation;
use bytecheck::CheckBytes;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use std::fmt::{self, Debug};
use timely::logging::{OperatesEvent as TimelyOperatesEvent, WorkerIdentifier};

// TODO: ChannelId

#[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize, Abomonation,
)]
#[serde(transparent)]
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

impl Debug for WorkerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WorkerId({})", self.worker)
    }
}

#[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize, Abomonation,
)]
#[serde(transparent)]
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

#[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize, Abomonation,
)]
#[serde(transparent)]
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
}

impl Debug for PortId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PortId({})", self.port)
    }
}

#[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize, Abomonation,
)]
#[serde(transparent)]
#[repr(transparent)]
pub struct ChannelId {
    edge: usize,
}

impl ChannelId {
    pub const fn new(edge: usize) -> Self {
        Self { edge }
    }

    pub const fn into_inner(self) -> usize {
        self.edge
    }
}

impl Debug for ChannelId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "EdgeId({})", self.edge)
    }
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Default,
    Serialize,
    Deserialize,
    Abomonation,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[archive(strict, derive(CheckBytes))]
pub struct OperatesEvent {
    pub id: usize,
    pub addr: OperatorAddr,
    pub name: String,
}

impl OperatesEvent {
    pub const fn new(id: usize, addr: OperatorAddr, name: String) -> Self {
        Self { id, addr, name }
    }
}

impl From<TimelyOperatesEvent> for OperatesEvent {
    fn from(TimelyOperatesEvent { id, addr, name }: TimelyOperatesEvent) -> Self {
        Self {
            id,
            addr: OperatorAddr::from(addr),
            name,
        }
    }
}
