use crate::{
    dataflow::{
        constants::DEFAULT_EXTRACTOR_CAPACITY,
        differential::{ArrangementStats, SplineLevel},
        operators::{CrossbeamExtractor, Fuel},
        progress_stats::{Channel, OperatorProgress, ProgressInfo},
        summation::Summation,
        utils::{channel_sink, Diff, OpKey, Time},
        worker_timeline::TimelineEvent,
        OperatorShape,
    },
    ui::{DataflowStats, ProgramStats, WorkerStats},
};
use crossbeam_channel::{Receiver, Sender};
use ddshow_types::{timely_logging::OperatesEvent, OperatorAddr, OperatorId, WorkerId};
use differential_dataflow::{
    difference::{Present, Semigroup},
    operators::arrange::{Arranged, TraceAgent},
    trace::implementations::ord::OrdKeySpine,
    Collection,
};
use serde::Serialize;
use std::{collections::HashMap, iter::Cycle, time::Duration};
use strum::{EnumIter, IntoEnumIterator};
use timely::{
    dataflow::{
        operators::{capture::Event, probe::Handle as ProbeHandle},
        Scope, ScopeParent,
    },
    progress::{ChangeBatch, Timestamp},
};

pub(crate) type ChannelAddrs<S, D> = Arranged<
    S,
    TraceAgent<OrdKeySpine<(WorkerId, OperatorAddr), <S as ScopeParent>::Timestamp, D>>,
>;
type Bundled<D, R = Diff> = (D, Time, R);
type ESender<D, R = Diff> = Sender<Event<Time, Bundled<D, R>>>;
type EReceiver<D, R = Diff> = Receiver<Event<Time, Bundled<D, R>>>;
type Extractor<D, R = Diff> = CrossbeamExtractor<Event<Time, Bundled<D, R>>>;

macro_rules! make_send_recv {
    ($($name:ident : $ty:ty $(= $diff:ty)?),* $(,)?) => {
        #[derive(Clone, Debug)]
        pub struct DataflowSenders {
            $($name: (ESender<$ty, $($diff)?>, bool),)*
        }

        impl DataflowSenders {
            #[allow(clippy::too_many_arguments)]
            pub fn new(
                $($name: ESender<$ty, $($diff)?>,)*
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
                $($name: (&Collection<S, $ty, make_send_recv!(@diff $($diff)?)>, bool),)*
            ) -> Vec<(ProbeHandle<Time>, &'static str)>
            where
                S: Scope<Timestamp = Time>,
            {
                let mut probes = Vec::with_capacity(NUM_VARIANTS + 1);

                $(
                    let (stream, needs_consolidation) = $name;
                    let mut probe = ProbeHandle::new();

                    channel_sink(
                        stream,
                        &mut probe,
                        self.$name(),
                        needs_consolidation,
                    );

                    probes.push((probe, stringify!($name)));
                )*

                probes
            }

            $(
                pub fn $name(&mut self) -> ESender<$ty, $($diff)?> {
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
            $(pub $name: EReceiver<$ty, $($diff)?>,)*
        }

        impl DataflowReceivers {
            /// Create a new dataflow receiver from the given channels
            #[allow(clippy::too_many_arguments)]
            pub fn new(
                $($name: EReceiver<$ty, $($diff)?>,)*
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
            $(pub $name: (Extractor<$ty, $($diff)?>, HashMap<$ty, make_send_recv!(@diff $($diff)?)>),)*
            step: Cycle<DataflowStepIter>,
            consumed: ChangeBatch<Time>,
            last_consumed: Time,
        }

        impl DataflowExtractor {
            #[allow(clippy::too_many_arguments)]
            pub fn new(
                $($name: EReceiver<$ty, $($diff)?>,)*
            ) -> Self {
                Self {
                    $($name: (
                        CrossbeamExtractor::new($name),
                        HashMap::with_capacity(DEFAULT_EXTRACTOR_CAPACITY),
                    ),)*
                    step: DataflowStep::iter().cycle(),
                    consumed: ChangeBatch::new(),
                    last_consumed: Time::minimum(),
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
                $({
                    let (extractor, sink) = &mut self.$name;
                    let is_disconnected = extractor.extract_with_fuel(
                        &mut Fuel::unlimited(),
                        sink,
                        &mut self.consumed,
                    );

                    if !is_disconnected {
                        tracing::error!(
                            concat!("the output channel for ", stringify!($name), " never disconnected"),
                        );
                    }
                })*

                $(
                    let $name: Vec<_> = self.$name.1
                        .into_iter()
                        .filter_map(|(data, diff)| if !diff.is_zero() { Some(data) } else { None })
                        .collect();

                    tracing::debug!(
                        "extracted {} {} events",
                        $name.len(),
                        stringify!($name).split("_").collect::<Vec<_>>().join(" "),
                    );
                )*

                DataflowData::new($($name,)*)
            }

            #[inline(never)]
            #[allow(dead_code)]
            pub fn current_dataflow_data(&self) -> DataflowData {
                $(
                    let $name: Vec<_> = self.$name.1
                        .iter()
                        .filter_map(|(data, diff)| if !diff.is_zero() { Some(data.clone()) } else { None })
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

        #[derive(Clone, Debug, Serialize)]
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

    (@diff) => { Diff };
    (@diff $diff:ty) => { $diff };
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
type TimelineEventData = TimelineEvent;
type NameLookupData = ((WorkerId, OperatorId), String);
type AddrLookupData = ((WorkerId, OperatorId), OperatorAddr);
type ChannelProgressData = (OperatorAddr, ProgressInfo);

make_send_recv! {
    program_stats: ProgramStats,
    worker_stats: WorkerStatsData,
    nodes: NodeData,
    edges: EdgeData,
    subgraphs: SubgraphData,
    dataflow_stats: DataflowStats,
    timeline_events: TimelineEventData = Present,
    name_lookup: NameLookupData,
    addr_lookup: AddrLookupData,
    channel_progress: ChannelProgressData,
    operator_shapes: OperatorShape,
    operator_progress: OperatorProgress,
    operator_activations: (OpKey, (Duration, Duration)),
    summarized: (OpKey, Summation),
    aggregated_summaries: (OperatorId, Summation),
    arrangements: (OpKey, ArrangementStats),
    aggregated_arrangements: (OperatorId, ArrangementStats),
    spline_levels: (OpKey, SplineLevel),
}
