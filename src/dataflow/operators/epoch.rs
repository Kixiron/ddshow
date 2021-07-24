use abomonation_derive::Abomonation;
use bytecheck::CheckBytes;
use differential_dataflow::lattice::Lattice;
use rkyv::{Archive, Deserialize, Serialize};
use serde::{Deserialize as SerdeDeserialize, Serialize as SerdeSerialize};
use std::fmt::{self, Debug, Write};
use timely::{
    order::PartialOrder,
    progress::{timestamp::Refines, PathSummary, Timestamp},
};

/// A pair of timestamps, partially ordered by the product order.
#[derive(
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Default,
    Archive,
    Deserialize,
    Serialize,
    SerdeDeserialize,
    SerdeSerialize,
    Abomonation,
)]
#[archive_attr(derive(CheckBytes))]
pub struct Epoch<S, D> {
    /// The system time
    pub system: S,
    /// The data time
    pub data: D,
}

impl<S, D> Epoch<S, D> {
    /// Create a new pair.
    #[inline]
    pub fn new(system: S, data: D) -> Self {
        Epoch { system, data }
    }

    /// Get the epoch's system time
    #[inline]
    pub fn system_time(&self) -> S
    where
        S: Clone,
    {
        self.system.clone()
    }

    /// Get the epoch's data time
    #[inline]
    pub fn data_time(&self) -> D
    where
        D: Clone,
    {
        self.data.clone()
    }
}

impl<S, D> PartialOrder for Epoch<S, D>
where
    S: PartialOrder,
    D: PartialOrder,
{
    #[inline]
    fn less_equal(&self, other: &Self) -> bool {
        self.system.less_equal(&other.system) && self.data.less_equal(&other.data)
    }
}

#[allow(clippy::wrong_self_convention)]
impl<S, D> Refines<()> for Epoch<S, D>
where
    S: Timestamp,
    D: Timestamp,
{
    #[inline]
    fn to_inner(_outer: ()) -> Self {
        Self::minimum()
    }

    #[inline]
    fn to_outer(self) {}

    #[inline]
    fn summarize(_summary: <Self>::Summary) {}
}

impl<S, D> PathSummary<Epoch<S, D>> for ()
where
    S: Timestamp,
    D: Timestamp,
{
    #[inline]
    fn results_in(&self, timestamp: &Epoch<S, D>) -> Option<Epoch<S, D>> {
        Some(timestamp.clone())
    }

    #[inline]
    fn followed_by(&self, &(): &Self) -> Option<Self> {
        Some(())
    }
}

impl<S, D> Timestamp for Epoch<S, D>
where
    S: Timestamp,
    D: Timestamp,
{
    type Summary = ();

    #[inline]
    fn minimum() -> Self {
        Self::new(S::minimum(), D::minimum())
    }
}

impl<S, D> Lattice for Epoch<S, D>
where
    S: Lattice,
    D: Lattice,
{
    #[inline]
    fn join(&self, other: &Self) -> Self {
        Self::new(self.system.join(&other.system), self.data.join(&other.data))
    }

    #[inline]
    fn meet(&self, other: &Self) -> Self {
        Self::new(self.system.meet(&other.system), self.data.meet(&other.data))
    }
}

impl<S, D> Debug for Epoch<S, D>
where
    S: Debug,
    D: Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_char('(')?;
        Debug::fmt(&self.system, f)?;
        f.write_str(", ")?;
        Debug::fmt(&self.data, f)?;
        f.write_char(')')
    }
}
