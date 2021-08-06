#[cfg(feature = "timely-next")]
use crate::dataflow::reachability::TrackerEvent;
use crate::dataflow::{operators::CrossbeamPusher, PROGRAM_NS_GRANULARITY};
use anyhow::{Context, Result};
use crossbeam_channel::Sender;
use ddshow_sink::{EventWriter, DIFFERENTIAL_ARRANGEMENT_LOG_FILE, TIMELY_LOG_FILE};
use ddshow_types::{
    differential_logging::DifferentialEvent, progress_logging::TimelyProgressEvent,
    timely_logging::TimelyEvent, OperatorId, WorkerId,
};
use differential_dataflow::{
    operators::{
        arrange::{Arranged, TraceAgent},
        Consolidate,
    },
    trace::implementations::ord::{OrdKeySpine, OrdValSpine},
    Collection, ExchangeData, Hashable,
};
use std::{
    any::Any,
    convert::TryFrom,
    fmt::{self, Debug, Display},
    fs::{self, File},
    hash::BuildHasherDefault,
    io::BufWriter,
    num::Wrapping,
    ops::{Deref, DerefMut, Range},
    path::{Path, PathBuf},
    thread::{self, JoinHandle},
    time::Duration,
};
use timely::{
    dataflow::{
        operators::{capture::Event, Capture, Probe},
        ProbeHandle, Scope, ScopeParent, Stream,
    },
    PartialOrder,
};
use xxhash_rust::xxh3::Xxh3;

pub(crate) type Diff = isize;
pub(crate) type Time = Duration; // Epoch<Duration, Duration>;
pub(crate) type OpKey = (WorkerId, OperatorId);

pub(crate) type XXHasher = BuildHasherDefault<Xxh3>;

pub(crate) type ArrangedVal<S, K, V, D = Diff> =
    Arranged<S, TraceAgent<OrdValSpine<K, V, <S as ScopeParent>::Timestamp, D>>>;

pub(crate) type ArrangedKey<S, K, D = Diff> =
    Arranged<S, TraceAgent<OrdKeySpine<K, <S as ScopeParent>::Timestamp, D>>>;

pub type TimelyLogBundle<Id = WorkerId, Event = TimelyEvent> = (Duration, Id, Event);
pub type DifferentialLogBundle<Id = WorkerId, Event = DifferentialEvent> = (Duration, Id, Event);
pub type ProgressLogBundle<Id = WorkerId> = (Duration, Id, TimelyProgressEvent);

#[cfg(feature = "timely-next")]
pub type ReachabilityLogBundle<Id = WorkerId> = (Duration, Id, TrackerEvent);

/// Puts timestamps into non-overlapping buckets that contain
/// the timestamps from `last_bucket..PROGRAM_NS_GRANULARITY`
/// to reduce the load on timely
#[allow(dead_code)]
pub(crate) fn granulate(&time: &Time) -> Time {
    let timestamp = time.as_nanos();
    let window_idx = (timestamp / PROGRAM_NS_GRANULARITY).saturating_add(1);
    let minted = Duration::from_nanos((window_idx * PROGRAM_NS_GRANULARITY.get()) as u64);

    debug_assert!(u64::try_from(window_idx * PROGRAM_NS_GRANULARITY.get())
        .map(|res| res as u128)
        .is_ok());
    debug_assert!(time.less_equal(&minted));

    minted
}

#[allow(clippy::type_complexity)]
pub(super) fn channel_sink<S, D>(
    collection: &Collection<S, D, Diff>,
    probe: &mut ProbeHandle<S::Timestamp>,
    channel: Sender<Event<S::Timestamp, (D, S::Timestamp, Diff)>>,
    should_consolidate: bool,
) where
    S: Scope<Timestamp = Time>,
    D: ExchangeData + Hashable,
{
    let collection = if should_consolidate {
        collection.consolidate()
    } else {
        collection.clone()
    };

    collection
        .inner
        .probe_with(probe)
        .capture_into(CrossbeamPusher::new(channel));

    tracing::debug!(
        "installed channel sink on worker {}",
        collection.scope().index(),
    );
}

/// Store all timely and differential events to disk
pub(super) fn logging_event_sink<S>(
    save_logs: &Path,
    scope: &mut S,
    timely_stream: &Stream<S, TimelyLogBundle>,
    probe: &mut ProbeHandle<Time>,
    differential_stream: Option<&Stream<S, DifferentialLogBundle>>,
) -> Result<()>
where
    S: Scope<Timestamp = Time>,
{
    // Create the directory for log files to go to
    fs::create_dir_all(&save_logs).with_context(|| {
        anyhow::format_err!(
            "failed to create `--save-logs` directory '{}'",
            save_logs.display(),
        )
    })?;

    let timely_path = log_file_path(TIMELY_LOG_FILE, save_logs, scope.index())?;

    tracing::info!(
        "installing timely file sink on worker {} pointed at {}",
        scope.index(),
        timely_path.display(),
    );

    let timely_file = BufWriter::new(File::create(&timely_path).with_context(|| {
        anyhow::format_err!(
            "failed to create `--save-logs` timely file '{}'",
            timely_path.display(),
        )
    })?);

    timely_stream
        .probe_with(probe)
        .capture_into(EventWriter::new(timely_file));

    if let Some(differential_stream) = differential_stream {
        let differential_path =
            log_file_path(DIFFERENTIAL_ARRANGEMENT_LOG_FILE, save_logs, scope.index())?;

        tracing::info!(
            "installing differential file sink on worker {} pointed at {}",
            scope.index(),
            differential_path.display(),
        );

        let differential_file =
            BufWriter::new(File::create(&differential_path).with_context(|| {
                anyhow::format_err!(
                    "failed to create `--save-logs` differential file '{}'",
                    differential_path.display(),
                )
            })?);

        differential_stream
            .probe_with(probe)
            .capture_into(EventWriter::new(differential_file));
    }

    Ok(())
}

/// Constructs the path to a logging file for the given worker
pub(super) fn log_file_path(file_prefix: &str, dir: &Path, worker_id: usize) -> Result<PathBuf> {
    let path = dir.join(format!(
        "{}.replay-worker-{}.ddshow",
        file_prefix, worker_id,
    ));

    path.canonicalize()
        .with_context(|| anyhow::format_err!("failed to canonicalize path '{}'", path.display()))
}

// pub(crate) fn set_steady_tick(progress: &ProgressBar, delta: usize) {
//     let time = SystemTime::UNIX_EPOCH.elapsed().map_or_else(
//         |err| {
//             tracing::error!("failed to get system time for seeding generator: {:?}", err);
//             Pcg64::new(delta as u128).next_u64() as u128
//         },
//         |time| time.as_nanos(),
//     );
//
//     let mut rng = Pcg64::new(time);
//     rng.advance(delta as u128);
//
//     progress.enable_steady_tick(rng.gen_range(50..500));
// }

#[allow(dead_code)]
pub(crate) struct Pcg64 {
    state: Wrapping<u128>,
    increment: Wrapping<u128>,
}

#[allow(dead_code)]
impl Pcg64 {
    const MULTIPLIER: Wrapping<u128> = Wrapping(6_364_136_223_846_793_005);
    const DEFAULT_INCREMENT: Wrapping<u128> = Wrapping(1_442_695_040_888_963_407);

    pub fn new(seed: u128) -> Self {
        Self::with_increment(Wrapping(seed), Self::DEFAULT_INCREMENT)
    }

    pub fn with_increment(seed: Wrapping<u128>, mut increment: Wrapping<u128>) -> Self {
        if increment % Wrapping(2) != Wrapping(0) {
            increment += Wrapping(1);
        }

        let mut gen = Self {
            state: seed + increment,
            increment,
        };
        gen.next_u64();

        gen
    }

    /// Advances the generator `delta` steps in `log(delta)` time
    pub fn advance(&mut self, delta: u128) {
        self.state = self.jump_lcg128(delta);
    }

    fn jump_lcg128(&self, mut delta: u128) -> Wrapping<u128> {
        let mut current_multiplier = Self::MULTIPLIER;
        let mut current_plus = self.increment;
        let mut accumulated_multiplier = Wrapping(1);
        let mut accumulated_plus = Wrapping(0);

        while delta > 0 {
            if (delta & 1) > 0 {
                accumulated_multiplier *= current_multiplier;
                accumulated_plus = (accumulated_plus * current_multiplier) + current_plus;
            }

            current_plus = (current_multiplier + Wrapping(1)) * current_plus;
            current_multiplier *= current_multiplier;

            delta /= 2;
        }

        (accumulated_multiplier * self.state) + accumulated_plus
    }

    pub fn next_u64(&mut self) -> u64 {
        let x = self.state;
        let count = (x >> 122).0 as u32;

        self.state = x * Self::MULTIPLIER + self.increment;
        let x = (x ^ (x >> 64)).0 as u64;

        x.rotate_right(count)
    }

    pub fn gen_range(&mut self, range: Range<u64>) -> u64 {
        range.start + self.next_u64() % (range.end - range.start)
    }
}

#[derive(Debug)]
pub struct JoinOnDrop<T>(Option<JoinHandle<T>>);

#[allow(dead_code)]
impl<T> JoinOnDrop<T> {
    pub const fn new(handle: JoinHandle<T>) -> Self {
        Self(Some(handle))
    }

    #[inline]
    pub fn unpark(&self) {
        self.thread().unpark();
    }

    #[allow(dead_code)]
    pub fn into_handle(mut self) -> JoinHandle<T> {
        self.0
            .take()
            .expect("the handle will always exist while the struct does")
    }
}

impl<T> Deref for JoinOnDrop<T> {
    type Target = JoinHandle<T>;

    fn deref(&self) -> &Self::Target {
        self.0
            .as_ref()
            .expect("the handle will always exist while the struct does")
    }
}

impl<T> DerefMut for JoinOnDrop<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0
            .as_mut()
            .expect("the handle will always exist while the struct does")
    }
}

impl<T> Drop for JoinOnDrop<T> {
    fn drop(&mut self) {
        #[cold]
        #[track_caller]
        #[inline(never)]
        fn cold_panic(err: Box<dyn Any>) -> ! {
            panic!("failed to join thread on drop: {:?}", err)
        }

        if let Some(handle) = self.0.take() {
            tracing::debug!("joining a thread handle on drop");

            if let Err(err) = handle.join() {
                if !thread::panicking() {
                    cold_panic(err);
                }
            }
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct HumanDuration(pub Duration);

impl HumanDuration {
    #[allow(dead_code)]
    pub const fn new(duration: Duration) -> Self {
        Self(duration)
    }
}

impl Deref for HumanDuration {
    type Target = Duration;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for HumanDuration {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Debug for HumanDuration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Debug::fmt(&self.0, f)
    }
}

impl Display for HumanDuration {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fn item_plural(
            f: &mut fmt::Formatter,
            started: &mut bool,
            name: &str,
            value: u64,
        ) -> fmt::Result {
            if value > 0 {
                if *started {
                    f.write_str(" ")?;
                }

                Display::fmt(&value, f)?;
                f.write_str(name)?;

                if value > 1 {
                    f.write_str("s")?;
                }

                *started = true;
            }

            Ok(())
        }

        fn item(f: &mut fmt::Formatter, started: &mut bool, name: &str, value: u32) -> fmt::Result {
            if value > 0 {
                if *started {
                    f.write_str(" ")?;
                }

                Display::fmt(&value, f)?;
                f.write_str(name)?;
                *started = true;
            }

            Ok(())
        }

        let secs = self.0.as_secs();
        let nanos = self.0.subsec_nanos();

        if secs == 0 && nanos == 0 {
            f.write_str("0s")?;
            return Ok(());
        }

        let years = secs / 31_557_600; // 365.25d
        let ydays = secs % 31_557_600;
        let months = ydays / 2_630_016; // 30.44d
        let mdays = ydays % 2_630_016;
        let days = mdays / 86400;
        let day_secs = mdays % 86400;
        let hours = day_secs / 3600;
        let minutes = day_secs % 3600 / 60;
        let seconds = day_secs % 60;

        let millis = nanos / 1_000_000;
        let micros = nanos / 1000 % 1000;
        let nanosec = nanos % 1000;

        let started = &mut false;
        item_plural(f, started, "year", years)?;
        item_plural(f, started, "month", months)?;
        item_plural(f, started, "day", days)?;
        item(f, started, "h", hours as u32)?;
        item(f, started, "m", minutes as u32)?;
        item(f, started, "s", seconds as u32)?;
        item(f, started, "ms", millis)?;
        item(f, started, "us", micros)?;
        item(f, started, "ns", nanosec)?;

        Ok(())
    }
}
