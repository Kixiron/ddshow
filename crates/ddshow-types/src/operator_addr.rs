//! The [`OperatorAddr`] type

use crate::ids::{OperatorId, PortId};
use core::{
    convert::TryFrom,
    fmt::{self, Debug, Display},
    iter::{FromIterator, IntoIterator},
    mem::ManuallyDrop,
    ops::Deref,
    slice,
};
use tinyvec::{ArrayVec, TinyVec};

#[cfg(feature = "enable_abomonation")]
use abomonation::Abomonation;
#[cfg(feature = "enable_abomonation")]
use std::io;

#[cfg(feature = "serde")]
use serde_dep::{Deserialize as SerdeDeserialize, Serialize as SerdeSerialize};

#[cfg(feature = "rkyv")]
use rkyv_dep::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

// TODO: Change this to use `OperatorId` instead of `usize`
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
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
#[repr(transparent)]
pub struct OperatorAddr {
    addr: TinyVec<[OperatorId; 8]>,
}

impl OperatorAddr {
    #[inline]
    pub const fn new(addr: TinyVec<[OperatorId; 8]>) -> Self {
        Self { addr }
    }

    #[inline]
    pub fn from_elem(segment: OperatorId) -> Self {
        let zero = OperatorId::new(0);

        Self::new(TinyVec::Inline(ArrayVec::from_array_len(
            [segment, zero, zero, zero, zero, zero, zero, zero],
            1,
        )))
    }

    #[inline]
    fn from_vec(addr: Vec<OperatorId>) -> Self {
        let tiny_vec = ArrayVec::try_from(addr.as_slice())
            .map_or_else(|_| TinyVec::Heap(addr), TinyVec::Inline);

        Self::new(tiny_vec)
    }

    #[inline]
    pub fn from_slice(addr: &[OperatorId]) -> Self {
        let tiny_vec =
            ArrayVec::try_from(addr).map_or_else(|_| TinyVec::Heap(addr.to_vec()), TinyVec::Inline);

        Self::new(tiny_vec)
    }

    #[inline]
    pub fn is_top_level(&self) -> bool {
        self.len() == 1
    }

    #[inline]
    pub fn push(&mut self, segment: PortId) {
        self.addr.push(OperatorId::new(segment.into_inner()));
    }

    #[inline]
    pub fn push_imm(&self, elem: PortId) -> Self {
        let mut this = self.clone();
        this.push(elem);
        this
    }

    #[inline]
    pub fn pop(&mut self) -> Option<OperatorId> {
        self.addr.pop()
    }

    #[inline]
    pub fn pop_imm(&self) -> (Self, Option<OperatorId>) {
        let mut this = self.clone();
        let popped = this.pop();

        (this, popped)
    }

    #[inline]
    pub fn as_slice(&self) -> &[OperatorId] {
        self.addr.as_slice()
    }

    #[inline]
    pub fn iter(&self) -> slice::Iter<'_, OperatorId> {
        self.as_slice().iter()
    }
}

impl From<&[OperatorId]> for OperatorAddr {
    #[inline]
    fn from(addr: &[OperatorId]) -> Self {
        Self::from_slice(addr)
    }
}

impl From<&Vec<OperatorId>> for OperatorAddr {
    #[inline]
    fn from(addr: &Vec<OperatorId>) -> Self {
        Self::from_slice(addr)
    }
}

impl From<Vec<OperatorId>> for OperatorAddr {
    #[inline]
    fn from(addr: Vec<OperatorId>) -> Self {
        Self::from_vec(addr)
    }
}

impl From<Vec<usize>> for OperatorAddr {
    #[inline]
    fn from(addr: Vec<usize>) -> Self {
        // FIXME: Use `Vec::into_raw_parts()` once that's stable
        // FIXME: Use `Vec::into_raw_parts_with_alloc()` once that's stable
        let addr: Vec<OperatorId> = {
            let mut addr = ManuallyDrop::new(addr);
            let (ptr, len, cap) = (addr.as_mut_ptr().cast(), addr.len(), addr.capacity());

            // Safety: `OperatorId` is a transparent wrapper around `usize`
            unsafe { Vec::from_raw_parts(ptr, len, cap) }
        };

        let tiny_vec = ArrayVec::try_from(addr.as_slice())
            .map_or_else(|_| TinyVec::Heap(addr), TinyVec::Inline);

        Self::new(tiny_vec)
    }
}

impl Deref for OperatorAddr {
    type Target = [OperatorId];

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.addr
    }
}

impl Extend<OperatorId> for OperatorAddr {
    #[inline]
    fn extend<T>(&mut self, segments: T)
    where
        T: IntoIterator<Item = OperatorId>,
    {
        self.addr.extend(segments);
    }
}

impl<'a> Extend<&'a OperatorId> for OperatorAddr {
    #[inline]
    fn extend<T>(&mut self, segments: T)
    where
        T: IntoIterator<Item = &'a OperatorId>,
    {
        self.addr.extend(segments.into_iter().copied());
    }
}

impl FromIterator<OperatorId> for OperatorAddr {
    #[inline]
    fn from_iter<T: IntoIterator<Item = OperatorId>>(iter: T) -> Self {
        Self {
            addr: <TinyVec<[OperatorId; 8]>>::from_iter(iter),
        }
    }
}

impl<'a> FromIterator<&'a OperatorId> for OperatorAddr {
    #[inline]
    fn from_iter<T: IntoIterator<Item = &'a OperatorId>>(iter: T) -> Self {
        Self {
            addr: iter.into_iter().copied().collect(),
        }
    }
}

impl Debug for OperatorAddr {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // We can forward to our display implementation
        Display::fmt(self, f)
    }
}

impl Display for OperatorAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list()
            // FIXME: Is there really no way to make `.debug_list()`
            //        call `Display` on elements?
            .entries(self.as_slice().iter().copied().map(OperatorId::into_inner))
            .finish()
    }
}

#[cfg(feature = "enable_abomonation")]
impl Abomonation for OperatorAddr {
    #[inline]
    unsafe fn entomb<W: io::Write>(&self, write: &mut W) -> io::Result<()> {
        match &self.addr {
            TinyVec::Inline(array) => array.into_inner().entomb(write),
            TinyVec::Heap(vec) => vec.entomb(write),
        }
    }

    #[inline]
    unsafe fn exhume<'a, 'b>(&'a mut self, bytes: &'b mut [u8]) -> Option<&'b mut [u8]> {
        let mut inner = Vec::new();
        let output = Vec::exhume(&mut inner, bytes)?;
        self.addr = TinyVec::Heap(inner);

        Some(output)
    }

    #[inline]
    fn extent(&self) -> usize {
        match &self.addr {
            TinyVec::Inline(array) => array.into_inner().extent(),
            TinyVec::Heap(vec) => vec.extent(),
        }
    }
}
