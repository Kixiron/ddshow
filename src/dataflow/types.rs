use abomonation::Abomonation;
use abomonation_derive::Abomonation;
use serde::{Deserialize, Serialize};
use std::{
    convert::TryFrom,
    fmt::{self, Debug},
    io,
    iter::IntoIterator,
    ops::Deref,
    slice,
};
use timely::logging::{OperatesEvent as TimelyOperatesEvent, WorkerIdentifier};
use tinyvec::{ArrayVec, TinyVec};

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

// TODO: Change this to use `OperatorId` instead of `usize
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize)]
#[serde(transparent)]
#[repr(transparent)]
pub struct OperatorAddr {
    addr: TinyVec<[usize; 8]>,
}

impl OperatorAddr {
    pub const fn new(addr: TinyVec<[usize; 8]>) -> Self {
        Self { addr }
    }

    pub fn from_elem(segment: usize) -> Self {
        Self::new(TinyVec::Inline(ArrayVec::from([
            segment, 0, 0, 0, 0, 0, 0, 0,
        ])))
    }

    pub fn from_slice(addr: &[usize]) -> Self {
        let tiny_vec =
            ArrayVec::try_from(addr).map_or_else(|_| TinyVec::Heap(addr.to_vec()), TinyVec::Inline);

        Self::new(tiny_vec)
    }

    pub fn push(&mut self, segment: usize) {
        self.addr.push(segment);
    }

    pub fn pop(&mut self) -> Option<usize> {
        self.addr.pop()
    }

    pub fn as_slice(&self) -> &[usize] {
        self.addr.as_slice()
    }

    pub fn iter(&self) -> slice::Iter<'_, usize> {
        self.as_slice().iter()
    }
}

impl From<&[usize]> for OperatorAddr {
    fn from(addr: &[usize]) -> Self {
        Self::from_slice(addr)
    }
}

impl From<&Vec<usize>> for OperatorAddr {
    fn from(addr: &Vec<usize>) -> Self {
        Self::from_slice(addr)
    }
}

impl From<Vec<usize>> for OperatorAddr {
    fn from(addr: Vec<usize>) -> Self {
        let tiny_vec = ArrayVec::try_from(addr.as_slice())
            .map_or_else(|_| TinyVec::Heap(addr), TinyVec::Inline);

        Self::new(tiny_vec)
    }
}

impl Deref for OperatorAddr {
    type Target = [usize];

    fn deref(&self) -> &Self::Target {
        &self.addr
    }
}

impl Extend<usize> for OperatorAddr {
    fn extend<T>(&mut self, segments: T)
    where
        T: IntoIterator<Item = usize>,
    {
        self.addr.extend(segments);
    }
}

impl<'a> Extend<&'a usize> for OperatorAddr {
    fn extend<T>(&mut self, segments: T)
    where
        T: IntoIterator<Item = &'a usize>,
    {
        self.addr.extend(segments.into_iter().copied());
    }
}

impl Debug for OperatorAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.addr.iter()).finish()
    }
}

impl Abomonation for OperatorAddr {
    unsafe fn entomb<W: io::Write>(&self, write: &mut W) -> io::Result<()> {
        match &self.addr {
            TinyVec::Inline(array) => array.into_inner().entomb(write),
            TinyVec::Heap(vec) => vec.entomb(write),
        }
    }

    unsafe fn exhume<'a, 'b>(&'a mut self, bytes: &'b mut [u8]) -> Option<&'b mut [u8]> {
        let mut inner = Vec::new();
        let output = Vec::exhume(&mut inner, bytes)?;
        self.addr = TinyVec::Heap(inner);

        Some(output)
    }

    fn extent(&self) -> usize {
        match &self.addr {
            TinyVec::Inline(array) => array.into_inner().extent(),
            TinyVec::Heap(vec) => vec.extent(),
        }
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize, Abomonation,
)]
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
