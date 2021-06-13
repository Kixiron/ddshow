#[cfg(feature = "timely-next")]
use crate::dataflow::reachability::TrackerEvent;
use crate::dataflow::{operators::CrossbeamPusher, PROGRAM_NS_GRANULARITY};
use anyhow::{Context, Result};
use crossbeam_channel::Sender;
use ddshow_sink::{EventWriter, DIFFERENTIAL_ARRANGEMENT_LOG_FILE, TIMELY_LOG_FILE};
use ddshow_types::{
    differential_logging::DifferentialEvent, progress_logging::TimelyProgressEvent,
    timely_logging::TimelyEvent, WorkerId,
};
use differential_dataflow::{
    difference::Semigroup,
    lattice::Lattice,
    operators::{
        arrange::{Arranged, TraceAgent},
        Consolidate,
    },
    trace::implementations::ord::{OrdKeySpine, OrdValSpine},
    Collection, ExchangeData, Hashable,
};
use std::{
    convert::TryFrom,
    fs::{self, File},
    io::BufWriter,
    path::{Path, PathBuf},
    time::Duration,
};
use timely::dataflow::{
    operators::{capture::Event, Capture, Probe},
    ProbeHandle, Scope, ScopeParent, Stream,
};

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

#[allow(clippy::type_complexity)]
pub(super) fn channel_sink<S, D, R>(
    collection: &Collection<S, D, R>,
    probe: &mut ProbeHandle<S::Timestamp>,
    channel: Sender<Event<S::Timestamp, (D, S::Timestamp, R)>>,
    should_consolidate: bool,
) where
    S: Scope,
    S::Timestamp: Lattice,
    D: ExchangeData + Hashable,
    R: Semigroup + ExchangeData,
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
    timely_stream: &Stream<S, (Duration, WorkerId, TimelyEvent)>,
    probe: &mut ProbeHandle<Duration>,
    differential_stream: Option<&Stream<S, (Duration, WorkerId, DifferentialEvent)>>,
) -> Result<()>
where
    S: Scope<Timestamp = Duration>,
{
    // Create the directory for log files to go to
    fs::create_dir_all(&save_logs).context("failed to create `--save-logs` directory")?;

    let timely_path = log_file_path(TIMELY_LOG_FILE, save_logs, scope.index());

    tracing::debug!(
        "installing timely file sink on worker {} pointed at {}",
        scope.index(),
        timely_path.display(),
    );

    let timely_file = BufWriter::new(
        File::create(timely_path).context("failed to create `--save-logs` timely file")?,
    );

    timely_stream
        .probe_with(probe)
        .capture_into(EventWriter::new(timely_file));

    if let Some(differential_stream) = differential_stream {
        let differential_path =
            log_file_path(DIFFERENTIAL_ARRANGEMENT_LOG_FILE, save_logs, scope.index());

        tracing::debug!(
            "installing differential file sink on worker {} pointed at {}",
            scope.index(),
            differential_path.display(),
        );

        let differential_file = BufWriter::new(
            File::create(differential_path)
                .context("failed to create `--save-logs` differential file")?,
        );

        differential_stream
            .probe_with(probe)
            .capture_into(EventWriter::new(differential_file));
    }

    Ok(())
}

/// Constructs the path to a logging file for the given worker
pub(super) fn log_file_path(file_prefix: &str, dir: &Path, worker_id: usize) -> PathBuf {
    dir.join(format!(
        "{}.replay-worker-{}.ddshow",
        file_prefix, worker_id
    ))
}
