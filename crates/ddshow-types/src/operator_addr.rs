//! The [`OperatorAddr`] type

use crate::ids::{OperatorId, PortId};
#[cfg(feature = "rkyv")]
use _rkyv::{
    Archive, Archived, Deserialize as RkyvDeserialize, Fallible, Resolver,
    Serialize as RkyvSerialize,
};
#[cfg(feature = "serde")]
use _serde::{Deserialize as SerdeDeserialize, Serialize as SerdeSerialize};
#[cfg(feature = "enable_abomonation")]
use abomonation::Abomonation;
use bytecheck::handle_error;
#[cfg(feature = "rkyv")]
use bytecheck::{CheckBytes, StructCheckError};
#[cfg(feature = "enable_abomonation")]
use std::io;
use std::{
    convert::TryFrom,
    fmt::{self, Debug, Display},
    iter::{FromIterator, IntoIterator},
    mem::{ManuallyDrop, MaybeUninit},
    ops::Deref,
    slice,
};
use tinyvec::{ArrayVec, TinyVec};

// TODO: Change this to use `OperatorId` instead of `usize`
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
#[cfg_attr(feature = "serde", serde(crate = "_serde", transparent))]
#[repr(transparent)]
pub struct OperatorAddr {
    addr: TinyVec<[OperatorId; 8]>,
}

impl OperatorAddr {
    pub const fn new(addr: TinyVec<[OperatorId; 8]>) -> Self {
        Self { addr }
    }

    pub fn from_elem(segment: OperatorId) -> Self {
        let zero = OperatorId::new(0);

        Self::new(TinyVec::Inline(ArrayVec::from_array_len(
            [segment, zero, zero, zero, zero, zero, zero, zero],
            1,
        )))
    }

    fn from_vec(addr: Vec<OperatorId>) -> Self {
        let tiny_vec = ArrayVec::try_from(addr.as_slice())
            .map_or_else(|_| TinyVec::Heap(addr), TinyVec::Inline);

        Self::new(tiny_vec)
    }

    pub fn from_slice(addr: &[OperatorId]) -> Self {
        let tiny_vec =
            ArrayVec::try_from(addr).map_or_else(|_| TinyVec::Heap(addr.to_vec()), TinyVec::Inline);

        Self::new(tiny_vec)
    }

    pub fn is_top_level(&self) -> bool {
        self.len() == 1
    }

    pub fn push(&mut self, segment: PortId) {
        self.addr.push(OperatorId::new(segment.into_inner()));
    }

    pub fn pop(&mut self) -> Option<OperatorId> {
        self.addr.pop()
    }

    pub fn as_slice(&self) -> &[OperatorId] {
        self.addr.as_slice()
    }

    pub fn iter(&self) -> slice::Iter<'_, OperatorId> {
        self.as_slice().iter()
    }
}

impl From<&[OperatorId]> for OperatorAddr {
    fn from(addr: &[OperatorId]) -> Self {
        Self::from_slice(addr)
    }
}

impl From<&Vec<OperatorId>> for OperatorAddr {
    fn from(addr: &Vec<OperatorId>) -> Self {
        Self::from_slice(addr)
    }
}

impl From<Vec<OperatorId>> for OperatorAddr {
    fn from(addr: Vec<OperatorId>) -> Self {
        Self::from_vec(addr)
    }
}

impl From<Vec<usize>> for OperatorAddr {
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

    fn deref(&self) -> &Self::Target {
        &self.addr
    }
}

impl Extend<OperatorId> for OperatorAddr {
    fn extend<T>(&mut self, segments: T)
    where
        T: IntoIterator<Item = OperatorId>,
    {
        self.addr.extend(segments);
    }
}

impl<'a> Extend<&'a OperatorId> for OperatorAddr {
    fn extend<T>(&mut self, segments: T)
    where
        T: IntoIterator<Item = &'a OperatorId>,
    {
        self.addr.extend(segments.into_iter().copied());
    }
}

impl FromIterator<OperatorId> for OperatorAddr {
    fn from_iter<T: IntoIterator<Item = OperatorId>>(iter: T) -> Self {
        Self {
            addr: <TinyVec<[OperatorId; 8]>>::from_iter(iter),
        }
    }
}

impl<'a> FromIterator<&'a OperatorId> for OperatorAddr {
    fn from_iter<T: IntoIterator<Item = &'a OperatorId>>(iter: T) -> Self {
        Self {
            addr: iter.into_iter().copied().collect(),
        }
    }
}

impl Debug for OperatorAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.addr.iter()).finish()
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

///An archived `OperatorAddr`
#[repr(C)]
pub struct ArchivedOperatorAddr
where
    Vec<OperatorId>: Archive,
{
    ///The archived counterpart of `OperatorAddr::addr`
    addr: Archived<Vec<OperatorId>>,
}

impl<C: ?Sized> CheckBytes<C> for ArchivedOperatorAddr
where
    Vec<OperatorId>: Archive,
    Archived<Vec<OperatorId>>: CheckBytes<C>,
{
    type Error = StructCheckError;

    unsafe fn check_bytes<'a>(
        value: *const Self,
        context: &mut C,
    ) -> Result<&'a Self, Self::Error> {
        let bytes = value.cast::<u8>();
        <Archived<Vec<OperatorId>> as CheckBytes<C>>::check_bytes(
            bytes
                .add(_rkyv::offset_of!(ArchivedOperatorAddr, addr))
                .cast(),
            context,
        )
        .map_err(|e| StructCheckError {
            field_name: "addr",
            inner: handle_error(e),
        })?;

        Ok(&*value)
    }
}

/// The resolver for an archived [`OperatorAddr`]
pub struct OperatorAddrResolver
where
    Vec<OperatorId>: Archive,
{
    addr: Resolver<Vec<OperatorId>>,
}

impl Archive for OperatorAddr
where
    Vec<OperatorId>: Archive,
{
    type Archived = ArchivedOperatorAddr;
    type Resolver = OperatorAddrResolver;

    #[inline]
    fn resolve(&self, pos: usize, resolver: Self::Resolver, out: &mut MaybeUninit<Self::Archived>) {
        self.addr.to_vec().resolve(
            pos,
            resolver.addr,
            _rkyv::project_struct!(out: Self::Archived => addr: Archived<Vec<OperatorId>>),
        );
    }
}

impl<S: Fallible + ?Sized> RkyvSerialize<S> for OperatorAddr
where
    Vec<OperatorId>: RkyvSerialize<S>,
{
    #[inline]
    fn serialize(&self, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        Ok(OperatorAddrResolver {
            addr: RkyvSerialize::<S>::serialize(&self.addr.to_vec(), serializer)?,
        })
    }
}

impl<D: Fallible + ?Sized> RkyvDeserialize<OperatorAddr, D> for Archived<OperatorAddr>
where
    Vec<OperatorId>: Archive,
    Archived<Vec<OperatorId>>: RkyvDeserialize<Vec<OperatorId>, D>,
{
    #[inline]
    fn deserialize(&self, deserializer: &mut D) -> Result<OperatorAddr, D::Error> {
        Ok(OperatorAddr::from_vec(self.addr.deserialize(deserializer)?))
    }
}
