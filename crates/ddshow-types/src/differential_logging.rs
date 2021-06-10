//! Differential dataflow logging events

use crate::ids::OperatorId;
#[cfg(feature = "enable_abomonation")]
use abomonation_derive::Abomonation;
#[cfg(feature = "rkyv")]
use bytecheck::CheckBytes;
use differential_dataflow::logging::{
    BatchEvent as RawBatchEvent, DifferentialEvent as RawDifferentialEvent,
    DropEvent as RawDropEvent, MergeEvent as RawMergeEvent, MergeShortfall as RawMergeShortfall,
    TraceShare as RawTraceShare,
};
#[cfg(feature = "rkyv")]
use rkyv_dep as rkyv;
#[cfg(feature = "rkyv")]
use rkyv_dep::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
#[cfg(feature = "serde")]
use serde_dep::{Deserialize as SerdeDeserialize, Serialize as SerdeSerialize};

/// Differential dataflow events
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "rkyv", derive(Archive, RkyvSerialize, RkyvDeserialize))]
#[cfg_attr(feature = "rkyv", archive(strict, derive(CheckBytes)))]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
#[cfg_attr(feature = "serde", serde(crate = "serde_dep"))]
#[cfg_attr(feature = "enable_abomonation", derive(Abomonation))]
pub enum DifferentialEvent {
    /// Batch creation.
    Batch(BatchEvent),
    /// Merge start and stop events.
    Merge(MergeEvent),
    /// Batch dropped when trace dropped.
    Drop(DropEvent),
    /// A merge failed to complete in time.
    MergeShortfall(MergeShortfall),
    /// Trace sharing event.
    TraceShare(TraceShare),
}

impl DifferentialEvent {
    /// Returns `true` if the differential_event is [`Batch`].
    pub const fn is_batch(&self) -> bool {
        matches!(self, Self::Batch(..))
    }

    /// Returns `true` if the differential_event is [`Merge`].
    pub const fn is_merge(&self) -> bool {
        matches!(self, Self::Merge(..))
    }

    /// Returns `true` if the differential_event is [`Drop`].
    pub const fn is_drop(&self) -> bool {
        matches!(self, Self::Drop(..))
    }

    /// Returns `true` if the differential_event is [`MergeShortfall`].
    pub const fn is_merge_shortfall(&self) -> bool {
        matches!(self, Self::MergeShortfall(..))
    }

    /// Returns `true` if the differential_event is [`TraceShare`].
    pub const fn is_trace_share(&self) -> bool {
        matches!(self, Self::TraceShare(..))
    }
}

impl From<RawDifferentialEvent> for DifferentialEvent {
    fn from(event: RawDifferentialEvent) -> Self {
        match event {
            RawDifferentialEvent::Batch(batch) => Self::Batch(batch.into()),
            RawDifferentialEvent::Merge(merge) => Self::Merge(merge.into()),
            RawDifferentialEvent::Drop(drop) => Self::Drop(drop.into()),
            RawDifferentialEvent::MergeShortfall(shortfall) => {
                Self::MergeShortfall(shortfall.into())
            }
            RawDifferentialEvent::TraceShare(share) => Self::TraceShare(share.into()),
        }
    }
}

impl From<DifferentialEvent> for RawDifferentialEvent {
    fn from(event: DifferentialEvent) -> Self {
        match event {
            DifferentialEvent::Batch(batch) => Self::Batch(batch.into()),
            DifferentialEvent::Merge(merge) => Self::Merge(merge.into()),
            DifferentialEvent::Drop(drop) => Self::Drop(drop.into()),
            DifferentialEvent::MergeShortfall(shortfall) => Self::MergeShortfall(shortfall.into()),
            DifferentialEvent::TraceShare(share) => Self::TraceShare(share.into()),
        }
    }
}

impl From<TraceShare> for DifferentialEvent {
    fn from(share: TraceShare) -> Self {
        Self::TraceShare(share)
    }
}

impl From<MergeShortfall> for DifferentialEvent {
    fn from(shortfall: MergeShortfall) -> Self {
        Self::MergeShortfall(shortfall)
    }
}

impl From<MergeEvent> for DifferentialEvent {
    fn from(merge: MergeEvent) -> Self {
        Self::Merge(merge)
    }
}

impl From<BatchEvent> for DifferentialEvent {
    fn from(batch: BatchEvent) -> Self {
        Self::Batch(batch)
    }
}

/// A batch of data sent to an arrangement
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "rkyv", derive(Archive, RkyvSerialize, RkyvDeserialize))]
#[cfg_attr(feature = "rkyv", archive(strict, derive(CheckBytes)))]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
#[cfg_attr(feature = "serde", serde(crate = "serde_dep"))]
#[cfg_attr(feature = "enable_abomonation", derive(Abomonation))]
pub struct BatchEvent {
    /// Operator identifier.
    pub operator: OperatorId,
    /// Which order of magnitude.
    pub length: usize,
}

impl BatchEvent {
    pub const fn new(operator: OperatorId, length: usize) -> Self {
        Self { operator, length }
    }
}

impl From<RawBatchEvent> for BatchEvent {
    fn from(event: RawBatchEvent) -> Self {
        Self {
            operator: OperatorId::new(event.operator),
            length: event.length,
        }
    }
}

impl From<BatchEvent> for RawBatchEvent {
    fn from(event: BatchEvent) -> Self {
        Self {
            operator: event.operator.into_inner(),
            length: event.length,
        }
    }
}

/// The destruction of an arrangement
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "rkyv", derive(Archive, RkyvSerialize, RkyvDeserialize))]
#[cfg_attr(feature = "rkyv", archive(strict, derive(CheckBytes)))]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
#[cfg_attr(feature = "serde", serde(crate = "serde_dep"))]
#[cfg_attr(feature = "enable_abomonation", derive(Abomonation))]
pub struct DropEvent {
    /// Operator identifier.
    pub operator: OperatorId,
    /// Which order of magnitude.
    pub length: usize,
}

impl DropEvent {
    pub const fn new(operator: OperatorId, length: usize) -> Self {
        Self { operator, length }
    }
}

impl From<RawDropEvent> for DropEvent {
    fn from(event: RawDropEvent) -> Self {
        Self {
            operator: OperatorId::new(event.operator),
            length: event.length,
        }
    }
}

impl From<DropEvent> for RawDropEvent {
    fn from(event: DropEvent) -> Self {
        Self {
            operator: event.operator.into_inner(),
            length: event.length,
        }
    }
}

/// Either the start or end of a merge event.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "rkyv", derive(Archive, RkyvSerialize, RkyvDeserialize))]
#[cfg_attr(feature = "rkyv", archive(strict, derive(CheckBytes)))]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
#[cfg_attr(feature = "serde", serde(crate = "serde_dep"))]
#[cfg_attr(feature = "enable_abomonation", derive(Abomonation))]
pub struct MergeEvent {
    /// Operator identifier.
    pub operator: OperatorId,
    /// Which order of magnitude.
    pub scale: usize,
    /// Length of first trace.
    pub length1: usize,
    /// Length of second trace.
    pub length2: usize,
    /// None implies a start.
    pub complete: Option<usize>,
}

impl MergeEvent {
    pub const fn new(
        operator: OperatorId,
        scale: usize,
        length1: usize,
        length2: usize,
        complete: Option<usize>,
    ) -> Self {
        Self {
            operator,
            scale,
            length1,
            length2,
            complete,
        }
    }
}

impl From<RawMergeEvent> for MergeEvent {
    fn from(event: RawMergeEvent) -> Self {
        Self {
            operator: OperatorId::new(event.operator),
            scale: event.scale,
            length1: event.length1,
            length2: event.length2,
            complete: event.complete,
        }
    }
}

impl From<MergeEvent> for RawMergeEvent {
    fn from(event: MergeEvent) -> Self {
        Self {
            operator: event.operator.into_inner(),
            scale: event.scale,
            length1: event.length1,
            length2: event.length2,
            complete: event.complete,
        }
    }
}

/// A merge failed to complete in time.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "rkyv", derive(Archive, RkyvSerialize, RkyvDeserialize))]
#[cfg_attr(feature = "rkyv", archive(strict, derive(CheckBytes)))]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
#[cfg_attr(feature = "serde", serde(crate = "serde_dep"))]
#[cfg_attr(feature = "enable_abomonation", derive(Abomonation))]
pub struct MergeShortfall {
    /// Operator identifer.
    pub operator: OperatorId,
    /// Which order of magnitude.
    pub scale: usize,
    /// By how much were we short.
    pub shortfall: usize,
}

impl MergeShortfall {
    pub const fn new(operator: OperatorId, scale: usize, shortfall: usize) -> Self {
        Self {
            operator,
            scale,
            shortfall,
        }
    }
}

impl From<RawMergeShortfall> for MergeShortfall {
    fn from(event: RawMergeShortfall) -> Self {
        Self {
            operator: OperatorId::new(event.operator),
            scale: event.scale,
            shortfall: event.shortfall,
        }
    }
}

impl From<MergeShortfall> for RawMergeShortfall {
    fn from(event: MergeShortfall) -> Self {
        Self {
            operator: event.operator.into_inner(),
            scale: event.scale,
            shortfall: event.shortfall,
        }
    }
}

/// The sharing of an arrangement trace
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "rkyv", derive(Archive, RkyvSerialize, RkyvDeserialize))]
#[cfg_attr(feature = "rkyv", archive(strict, derive(CheckBytes)))]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
#[cfg_attr(feature = "serde", serde(crate = "serde_dep"))]
#[cfg_attr(feature = "enable_abomonation", derive(Abomonation))]
pub struct TraceShare {
    /// Operator identifier.
    pub operator: OperatorId,
    /// Change in number of shares.
    pub diff: isize,
}

impl TraceShare {
    pub const fn new(operator: OperatorId, diff: isize) -> Self {
        Self { operator, diff }
    }
}

impl From<RawTraceShare> for TraceShare {
    fn from(event: RawTraceShare) -> Self {
        Self {
            operator: OperatorId::new(event.operator),
            diff: event.diff,
        }
    }
}

impl From<TraceShare> for RawTraceShare {
    fn from(event: TraceShare) -> Self {
        Self {
            operator: event.operator.into_inner(),
            diff: event.diff,
        }
    }
}
