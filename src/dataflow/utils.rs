#[cfg(feature = "timely-next")]
use crate::dataflow::reachability::TrackerEvent;
use crate::dataflow::PROGRAM_NS_GRANULARITY;
use ddshow_types::{
    differential_logging::DifferentialEvent, progress_logging::TimelyProgressEvent,
    timely_logging::TimelyEvent, WorkerId,
};
use differential_dataflow::{
    operators::arrange::{Arranged, TraceAgent},
    trace::implementations::ord::{OrdKeySpine, OrdValSpine},
};
use std::{convert::TryFrom, time::Duration};
use timely::dataflow::ScopeParent;

pub(crate) type Diff = isize;
pub(crate) type Time = Duration;

pub(crate) type ArrangedVal<S, K, V, D = Diff> =
    Arranged<S, TraceAgent<OrdValSpine<K, V, <S as ScopeParent>::Timestamp, D>>>;

pub(crate) type ArrangedKey<S, K, D = Diff> =
    Arranged<S, TraceAgent<OrdKeySpine<K, <S as ScopeParent>::Timestamp, D>>>;

pub type TimelyLogBundle<Id = WorkerId, Event = TimelyEvent> = (Time, Id, Event);
pub type DifferentialLogBundle<Id = WorkerId, Event = DifferentialEvent> = (Time, Id, Event);
pub type ProgressLogBundle<Id = WorkerId> = (Time, Id, TimelyProgressEvent);

#[cfg(feature = "timely-next")]
pub type ReachabilityLogBundle<Id = WorkerId> = (Time, Id, TrackerEvent);

/// Puts timestamps into non-overlapping buckets that contain
/// the timestamps from `last_bucket..PROGRAM_NS_GRANULARITY`
/// to reduce the load on timely
pub(crate) fn granulate(&time: &Duration) -> Duration {
    let timestamp = time.as_nanos();
    let window_idx = (timestamp / PROGRAM_NS_GRANULARITY) + 1;

    let minted = Duration::from_nanos((window_idx * PROGRAM_NS_GRANULARITY) as u64);
    debug_assert_eq!(
        u64::try_from(window_idx * PROGRAM_NS_GRANULARITY).map(|res| res as u128),
        Ok(window_idx * PROGRAM_NS_GRANULARITY),
    );
    debug_assert!(time <= minted);

    minted
}
