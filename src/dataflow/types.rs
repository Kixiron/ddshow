use abomonation::Abomonation;
use abomonation_derive::Abomonation;
use core::{
    convert::TryFrom,
    fmt::{self, Debug},
    iter::IntoIterator,
    ops::Deref,
    slice,
};
use serde::{Deserialize, Serialize};
use std::io;
use timely::logging::{OperatesEvent as TimelyOperatesEvent, WorkerIdentifier};
use tinyvec::{ArrayVec, TinyVec};

#[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Abomonation,
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
        write!(f, "Worker({})", self.worker)
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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
        0
    }
}

#[derive(
    Serialize, Deserialize, Abomonation, Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd,
)]
pub struct OperatesEvent {
    pub id: usize,
    pub addr: OperatorAddr,
    pub name: String,
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
