use crate::{
    dataflow::{
        operators::{rkyv_capture::RkyvOperatesEvent, CrossbeamExtractor, Fuel},
        Channel, Diff, OperatorAddr, OperatorId, OperatorStats, Time, WorkerId,
        WorkerTimelineEvent,
    },
    ui::{DataflowStats, ProgramStats, WorkerStats},
};
use crossbeam_channel::{Receiver, Sender};
use differential_dataflow::{
    operators::arrange::{Arranged, TraceAgent},
    trace::implementations::ord::OrdKeySpine,
};
use std::{collections::HashMap, convert::identity, iter::Cycle};
use strum::{EnumIter, IntoEnumIterator};
use timely::dataflow::{operators::capture::Event, ScopeParent};

pub(crate) type ChannelAddrs<S, D> = Arranged<
    S,
    TraceAgent<OrdKeySpine<(WorkerId, OperatorAddr), <S as ScopeParent>::Timestamp, D>>,
>;
type Bundled<D> = (D, Time, Diff);
type ESender<D> = Sender<Event<Time, Bundled<D>>>;
type EReceiver<D> = Receiver<Event<Time, Bundled<D>>>;
type Extractor<D> = CrossbeamExtractor<Event<Time, Bundled<D>>>;

const DEFAULT_EXTRACTOR_CAPACITY: usize = 1024;

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

                                extractor.extract_with_fuel(fuel, sink);
                            },
                        )*
                    }
                }
            }

            // TODO: Does this need to guard against never-disconnected channels?
            #[inline(never)]
            pub fn extract_all(mut self) -> DataflowData {
                let mut fuel = Fuel::unlimited();
                let mut extractor_status = Vec::with_capacity(NUM_VARIANTS);

                loop {
                    $(
                        extractor_status.push({
                            let (extractor, sink) = &mut self.$name;

                            extractor.extract_with_fuel(&mut fuel, sink)
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
                        .filter(|&(_, diff)| diff >= 1)
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
type NodeData = ((WorkerId, OperatorAddr), RkyvOperatesEvent);
type EdgeData = (
    WorkerId,
    RkyvOperatesEvent,
    Channel,
    RkyvOperatesEvent,
    // Option<ChannelMessageStats>,
);
type SubgraphData = ((WorkerId, OperatorAddr), RkyvOperatesEvent);
type OperatorStatsData = ((WorkerId, OperatorId), OperatorStats);
type AggOperatorStatsData = (OperatorId, OperatorStats);
type TimelineEventData = WorkerTimelineEvent;
type NameLookupData = ((WorkerId, OperatorId), String);
type AddrLookupData = ((WorkerId, OperatorId), OperatorAddr);

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
}
