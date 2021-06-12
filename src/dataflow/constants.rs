//! Various constants and defaults for ddshow

use std::{num::NonZeroUsize, time::Duration};

/// Only cause the program stats to update every N milliseconds to
/// prevent this from absolutely thrashing the scheduler
// TODO: Make this configurable by the user
pub const PROGRAM_NS_GRANULARITY: u128 = 5_000_000_000;

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
    unsafe { NonZeroUsize::new_unchecked(1_000_000) };

/// The delay to reactivate replay operators after
pub(crate) const DEFAULT_REACTIVATION_DELAY: Duration = Duration::from_millis(200);

/// The current version of DDShow
pub const DDSHOW_VERSION: &str = env!("VERGEN_GIT_SEMVER");
