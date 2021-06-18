use crate::{
    dataflow::{
        constants::DEFAULT_EXTRACTOR_CAPACITY,
        operator_stats::{AggregatedOperatorStats, OperatorStats},
        operators::{CrossbeamExtractor, Fuel},
        progress_stats::{Channel, ProgressInfo},
        utils::{channel_sink, Diff, Time},
        worker_timeline::WorkerTimelineEvent,
    },
    ui::{DataflowStats, ProgramStats, WorkerStats},
};
use crossbeam_channel::{Receiver, Sender};
use ddshow_types::{timely_logging::OperatesEvent, OperatorAddr, OperatorId, WorkerId};
use differential_dataflow::{
    operators::arrange::{Arranged, TraceAgent},
    trace::implementations::ord::OrdKeySpine,
    Collection,
};
use std::{collections::HashMap, convert::identity, iter::Cycle, time::Duration};
use strum::{EnumIter, IntoEnumIterator};
use timely::{
    dataflow::{
        operators::{capture::Event, probe::Handle as ProbeHandle},
        Scope, ScopeParent,
    },
    progress::ChangeBatch,
};

pub(crate) type ChannelAddrs<S, D> = Arranged<
    S,
    TraceAgent<OrdKeySpine<(WorkerId, OperatorAddr), <S as ScopeParent>::Timestamp, D>>,
>;
type Bundled<D> = (D, Time, Diff);
type ESender<D> = Sender<Event<Time, Bundled<D>>>;
type EReceiver<D> = Receiver<Event<Time, Bundled<D>>>;
type Extractor<D> = CrossbeamExtractor<Event<Time, Bundled<D>>>;

macro_rules! make_send_recv {
    ($($name:ident : $ty:ty),* $(,)?) => {
        #[derive(Clone, Debug)]
        pub struct DataflowSenders {
            $($name: (ESender<$ty>, bool),)*
        }

        impl DataflowSenders {
            #[allow(clippy::too_many_arguments)]
            pub fn new(
                $($name: ESender<$ty>,)*
            ) -> Self {
                Self {
                    $($name: ($name, false),)*
                }
            }

            pub fn create() -> (Self, DataflowReceivers) {
                $(let $name = crossbeam_channel::unbounded();)*

                let sender = {
                    $(let ($name, _) = $name;)*

                    Self::new($($name,)*)
                };

                let receiver =  {
                    $(let (_, $name) = $name;)*

                    DataflowReceivers::new($($name,)*)
                };

                (sender, receiver)
            }

            #[allow(clippy::too_many_arguments)]
            pub fn install_sinks<S>(
                mut self,
                probe: &mut ProbeHandle<Duration>,
                $($name: (&Collection<S, $ty, Diff>, bool),)*
            )
            where
                S: Scope<Timestamp = Duration>,
            {
                $(
                    let (stream, needs_consolidation) = $name;
                    channel_sink(
                        stream,
                        probe,
                        self.$name(),
                        needs_consolidation,
                    );
                )*
            }

            $(
                pub fn $name(&mut self) -> ESender<$ty> {
                    tracing::debug!("sent to dataflow sender {}", stringify!($name));

                    self.$name.1 = true;
                    self.$name.0.clone()
                }
            )*
        }

        impl Drop for DataflowSenders {
            #[inline(never)]
            fn drop(&mut self) {
                $(
                    if cfg!(debug_assertions) && !self.$name.1 {
                        tracing::warn!("never sent to dataflow sender `{}`", stringify!($name));
                    }
                )*
            }
        }

        #[derive(Clone, Debug)]
        pub struct DataflowReceivers {
            $(pub $name: EReceiver<$ty>,)*
        }

        impl DataflowReceivers {
            /// Create a new dataflow receiver from the given channels
            #[allow(clippy::too_many_arguments)]
            pub fn new(
                $($name: EReceiver<$ty>,)*
            ) -> Self {
                Self {
                    $($name,)*
                }
            }

            /// Make a [`DataflowExtractor`] for the target dataflow
            pub fn into_extractor(self) -> DataflowExtractor {
                DataflowExtractor::new($(self.$name,)*)
            }
        }

        const NUM_VARIANTS: usize = 0 $(+ {
            #[allow(dead_code, non_upper_case_globals)]
            const $name: () = ();
            1
        })*;

        #[derive(Clone)]
        pub struct DataflowExtractor {
            $(pub $name: (Extractor<$ty>, HashMap<$ty, Diff>),)*
            step: Cycle<DataflowStepIter>,
            consumed: ChangeBatch<Duration>,
            last_consumed: Duration,
        }

        impl DataflowExtractor {
            #[allow(clippy::too_many_arguments)]
            pub fn new(
                $($name: EReceiver<$ty>,)*
            ) -> Self {
                Self {
                    $($name: (
                        CrossbeamExtractor::new($name),
                        HashMap::with_capacity(DEFAULT_EXTRACTOR_CAPACITY),
                    ),)*
                    step: DataflowStep::iter().cycle(),
                    consumed: ChangeBatch::new(),
                    last_consumed: Duration::from_secs(0),
                }
            }

            /// Extract data from the current dataflow in a non-blocking manner
            #[inline(never)]
            pub fn extract_with_fuel(&mut self, fuel: &mut Fuel) {
                for step in self.step.by_ref().take(NUM_VARIANTS) {
                    if fuel.is_exhausted() {
                        break;
                    }

                    match step {
                        $(
                            DataflowStep::$name => {
                                let (extractor, sink) = &mut self.$name;

                                extractor.extract_with_fuel(fuel, sink, &mut self.consumed);
                            },
                        )*
                    }
                }
            }

            // // FIXME: Use the tracker progress infrastructure for a more
            // //        accurate version of this
            // pub fn latest_timestamp(&mut self) -> Duration {
            //     self.consumed.compact();
            //     self.consumed
            //         .unstable_internal_updates()
            //         .iter()
            //         .map(|&(time, _)| time)
            //         .max()
            //         .map(|time| if time < self.last_consumed {
            //             self.last_consumed
            //         } else {
            //             self.last_consumed = time;
            //             time
            //         })
            //         .unwrap_or(self.last_consumed)
            // }

            // TODO: Does this need to guard against never-disconnected channels?
            #[inline(never)]
            pub fn extract_all(mut self) -> DataflowData {
                let mut fuel = Fuel::unlimited();
                let mut extractor_status = Vec::with_capacity(NUM_VARIANTS);

                loop {
                    $(
                        extractor_status.push({
                            let (extractor, sink) = &mut self.$name;

                            extractor.extract_with_fuel(&mut fuel, sink, &mut self.consumed)
                        });
                    )*

                    if extractor_status.iter().copied().all(identity) {
                        break;
                    }

                    extractor_status.clear();
                }

                $(
                    let $name: Vec<_> = self.$name.1
                        .into_iter()
                        // Note: No filtering is needed here since `0..-N` produces no
                        //       outputs and neither does `0..0` which ensures that only
                        //       values with a difference of `diff >= 1` are produced
                        .flat_map(|(data, diff)| (0..diff).map(move |_| data.clone()))
                        .collect();

                    tracing::debug!(
                        "extracted {} {} events",
                        $name.len(),
                        stringify!($name).split("_").collect::<Vec<_>>().join(" "),
                    );
                )*

                DataflowData::new($($name,)*)
            }
        }

        #[derive(Clone, Debug)]
        pub struct DataflowData {
            $(pub $name: Vec<$ty>,)*
        }

        impl DataflowData {
            #[allow(clippy::too_many_arguments)]
            pub fn new(
                $($name: Vec<$ty>,)*
            ) -> Self {
                Self {
                    $($name,)*
                }
            }
        }

        #[derive(Debug, Clone, Copy, EnumIter)]
        #[allow(non_camel_case_types)]
        enum DataflowStep {
            $($name,)*
        }
    };
}

type WorkerStatsData = Vec<(WorkerId, WorkerStats)>;
type NodeData = ((WorkerId, OperatorAddr), OperatesEvent);
type EdgeData = (
    WorkerId,
    OperatesEvent,
    Channel,
    OperatesEvent,
    // Option<ChannelMessageStats>,
);
type SubgraphData = ((WorkerId, OperatorAddr), OperatesEvent);
type OperatorStatsData = ((WorkerId, OperatorId), OperatorStats);
type AggOperatorStatsData = (OperatorId, AggregatedOperatorStats);
type TimelineEventData = WorkerTimelineEvent;
type NameLookupData = ((WorkerId, OperatorId), String);
type AddrLookupData = ((WorkerId, OperatorId), OperatorAddr);
type ChannelProgressData = (OperatorAddr, ProgressInfo);

make_send_recv! {
    program_stats: ProgramStats,
    worker_stats: WorkerStatsData,
    nodes: NodeData,
    edges: EdgeData,
    subgraphs: SubgraphData,
    operator_stats: OperatorStatsData,
    aggregated_operator_stats: AggOperatorStatsData,
    dataflow_stats: DataflowStats,
    timeline_events: TimelineEventData,
    name_lookup: NameLookupData,
    addr_lookup: AddrLookupData,
    channel_progress: ChannelProgressData,
}
