use crate::dataflow::types::PortId;
use abomonation::Abomonation;
use bytecheck::{CheckBytes, StructCheckError};
use rkyv::{
    Archive, Archived, Deserialize as RkyvDeserialize, Fallible, Resolver,
    Serialize as RkyvSerialize,
};
use serde::{Deserialize, Serialize};
use std::{
    convert::TryFrom,
    fmt::{self, Debug, Display},
    io,
    iter::IntoIterator,
    mem::MaybeUninit,
    ops::Deref,
    ptr, slice,
};
use tinyvec::{ArrayVec, TinyVec};

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

    pub fn push(&mut self, segment: PortId) {
        self.addr.push(segment.into_inner());
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

impl Display for OperatorAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Debug::fmt(&self.addr.as_slice(), f)
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

#[repr(C)]
pub struct ArchivedOperatorAddr
where
    Vec<usize>: Archive,
{
    addr: Archived<Vec<usize>>,
}

impl<C: ?Sized> CheckBytes<C> for ArchivedOperatorAddr
where
    Vec<usize>: Archive,
    Archived<Vec<usize>>: CheckBytes<C>,
{
    type Error = StructCheckError;

    unsafe fn check_bytes<'a>(
        value: *const Self,
        context: &mut C,
    ) -> Result<&'a Self, Self::Error> {
        let bytes = value.cast::<u8>();
        <Archived<Vec<usize>> as CheckBytes<C>>::check_bytes(
            bytes
                .add({
                    let uninit = MaybeUninit::<ArchivedOperatorAddr>::uninit();
                    let base_ptr: *const ArchivedOperatorAddr = uninit.as_ptr();

                    let field_ptr = {
                        let ArchivedOperatorAddr { addr: _, .. };
                        let base = base_ptr;

                        ptr::addr_of!(*(base as *const ArchivedOperatorAddr))
                    };

                    (field_ptr as usize) - (base_ptr as usize)
                })
                .cast(),
            context,
        )
        .map_err(|e| StructCheckError {
            field_name: "addr",
            inner: e.into(),
        })?;

        Ok(&*value)
    }
}

pub struct OperatorAddrResolver
where
    Vec<usize>: Archive,
{
    addr: Resolver<Vec<usize>>,
}

impl Archive for OperatorAddr
where
    Vec<usize>: Archive,
{
    type Archived = ArchivedOperatorAddr;
    type Resolver = OperatorAddrResolver;

    fn resolve(&self, pos: usize, resolver: Self::Resolver) -> Self::Archived {
        Self::Archived {
            addr: self.addr.to_vec().resolve(
                pos + {
                    let uninit = MaybeUninit::<ArchivedOperatorAddr>::uninit();
                    let base_ptr: *const ArchivedOperatorAddr = uninit.as_ptr();

                    let field_ptr = {
                        #[allow(clippy::unneeded_field_pattern)]
                        let ArchivedOperatorAddr { addr: _, .. };
                        let base = base_ptr;

                        unsafe {
                            {
                                ptr::addr_of!(*(base as *const ArchivedOperatorAddr))
                            }
                        }
                    };

                    (field_ptr as usize) - (base_ptr as usize)
                },
                resolver.addr,
            ),
        }
    }
}

impl<S: Fallible + ?Sized> RkyvSerialize<S> for OperatorAddr
where
    Vec<usize>: RkyvSerialize<S>,
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
    Vec<usize>: Archive,
    Archived<Vec<usize>>: RkyvDeserialize<Vec<usize>, D>,
{
    #[inline]
    fn deserialize(&self, deserializer: &mut D) -> Result<OperatorAddr, D::Error> {
        Ok(OperatorAddr::from(self.addr.deserialize(deserializer)?))
    }
}
