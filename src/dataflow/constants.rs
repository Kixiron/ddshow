//! Various constants and defaults for ddshow

use core::{
    num::{NonZeroU128, NonZeroUsize},
    time::Duration,
};

/// Only cause the program stats to update every N milliseconds to
/// prevent this from absolutely thrashing the scheduler.
///
/// The value is currently set to 50 milliseconds
// TODO: Make this configurable by the user
// Safety: 50,000,000 isn't zero
pub const PROGRAM_NS_GRANULARITY: NonZeroU128 = unsafe { NonZeroU128::new_unchecked(50_000_000) };

/// The default capacity to initialize extractor maps to
pub(crate) const DEFAULT_EXTRACTOR_CAPACITY: usize = 1024;

/// The margin to decide whether or not to fuse adjacent events by
pub(crate) const EVENT_NS_MARGIN: u64 = 500_000;

/// The read timeout to impose on tcp connections
pub(crate) const TCP_READ_TIMEOUT: Option<Duration> = Some(Duration::from_millis(200));

/// The fuel used to extract data from the dataflow within the
/// main thread's spin loop
// Safety: 1,000,000 isn't zero
pub(crate) const IDLE_EXTRACTION_FUEL: NonZeroUsize =
    unsafe { NonZeroUsize::new_unchecked(100_000_000) };

/// The fuel used to extract events from a file-backed source
///
/// This is important because a file gives us what's essentially
/// immediate access to *all* events, whereas a networked source
/// drips us events slowly. If this constant is set too high,
/// it'll cause too many (or all) events to be immediately ingested,
/// causing frontiers to be advanced very far (or to `[]`) and
/// triggering our notifiers to dump their contents all at once
/// (this mainly affects `.delay()`-type operators since they're
/// at the forefront of our dataflow) which overloads the system
/// with gratuitously large vectors, causing us to quickly reach
/// an OOM situation
// Safety: 10,000 isn't zero
pub(crate) const FILE_SOURCED_FUEL: NonZeroUsize = unsafe { NonZeroUsize::new_unchecked(50_000) };

/// The delay to reactivate replay operators after
pub(crate) const DEFAULT_REACTIVATION_DELAY: Duration = Duration::from_millis(200);

/// The current version of DDShow
pub const DDSHOW_VERSION: &str = concat!(
    env!("CARGO_PKG_NAME"),
    " ",
    env!("CARGO_PKG_VERSION"),
    " (rustc ",
    env!("VERGEN_RUSTC_SEMVER"),
    ")",
);
