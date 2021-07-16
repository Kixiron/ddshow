use crate::dataflow::{
    operators::{FlatSplit, Keys},
    utils::ProgressLogBundle,
    Diff, OperatorShape,
};
use abomonation_derive::Abomonation;
use ddshow_types::{ChannelId, OperatorAddr, OperatorId, PortId, WorkerId};
use differential_dataflow::{
    operators::{arrange::ArrangeByKey, CountTotal, Join, Reduce},
    AsCollection, Collection,
};
use serde::{Deserialize, Serialize};
use std::{iter, time::Duration};
use timely::dataflow::{Scope, Stream};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct ChannelMessageStats {
    pub channel: ChannelId,
    pub worker: WorkerId,
    pub max: usize,
    pub min: usize,
    pub total: usize,
    pub average: usize,
    pub invocations: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct ChannelCapabilityStats {
    pub channel: ChannelId,
    pub worker: WorkerId,
    pub max: usize,
    pub min: usize,
    pub total: usize,
    pub average: usize,
    pub invocations: usize,
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation, Deserialize, Serialize,
)]
pub enum Channel {
    ScopeCrossing {
        /// The channel ids of all channels that are part of this single path
        channel_id: ChannelId,
        source_addr: OperatorAddr,
        target_addr: OperatorAddr,
    },

    Normal {
        channel_id: ChannelId,
        source_addr: OperatorAddr,
        target_addr: OperatorAddr,
    },
}

impl Channel {
    pub const fn channel_id(&self) -> ChannelId {
        match *self {
            Self::ScopeCrossing { channel_id, .. } | Self::Normal { channel_id, .. } => channel_id,
        }
    }

    pub fn source_addr(&self) -> OperatorAddr {
        match self {
            Self::ScopeCrossing { source_addr, .. } | Self::Normal { source_addr, .. } => {
                source_addr.to_owned()
            }
        }
    }

    pub fn target_addr(&self) -> OperatorAddr {
        match self {
            Self::ScopeCrossing { target_addr, .. } | Self::Normal { target_addr, .. } => {
                target_addr.to_owned()
            }
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
pub struct OperatorProgress {
    pub operator: OperatorId,
    pub worker: WorkerId,
    /// Input port -> (messages, channel)
    pub input_messages: Vec<(PortId, (isize, ChannelId))>,
    /// Output port -> (messages, channel)
    pub output_messages: Vec<(PortId, (isize, ChannelId))>,
}

impl OperatorProgress {
    pub const fn new(
        operator: OperatorId,
        worker: WorkerId,
        input_messages: Vec<(PortId, (isize, ChannelId))>,
        output_messages: Vec<(PortId, (isize, ChannelId))>,
    ) -> Self {
        Self {
            operator,
            worker,
            input_messages,
            output_messages,
        }
    }
}

impl abomonation::Abomonation for OperatorProgress {}

#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Abomonation,
    Deserialize,
    Serialize,
)]
pub struct ProgressInfo {
    pub consumed: ProgressStats,
    pub produced: ProgressStats,
    pub channel_id: ChannelId,
}

#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Abomonation,
    Deserialize,
    Serialize,
)]
pub struct ProgressStats {
    pub messages: usize,
    pub capability_updates: usize,
}

pub fn aggregate_channel_messages<S>(
    progress_stream: &Stream<S, ProgressLogBundle>,
    shapes: &Collection<S, OperatorShape, Diff>,
) -> Collection<S, OperatorProgress, Diff>
where
    S: Scope<Timestamp = Duration>,
{
    let (produced, consumed) =
        progress_stream.flat_split_named("Progress Inputs & Outputs", |(time, worker, event)| {
            // FIXME: Rust 2021 edition allows closures to capture fields instead of the whole struct
            let (scope_addr, channel) = (event.addr, event.channel);

            let messages: Box<dyn Iterator<Item = (_, _, _)>> =
                Box::new(event.messages.into_iter().map(move |message| {
                    (
                        (
                            (worker, message.node, scope_addr.clone()),
                            (message.port, channel, message.diff),
                        ),
                        time,
                        1,
                    )
                }));
            let empty: Box<dyn Iterator<Item = (_, _, _)>> = Box::new(iter::empty());

            // When this is a source event `message.node` is the output port of the producing operator
            // while `message.port` is the input port on the consuming operator
            if event.is_send {
                // `messages` is `((worker, output_port, scope_addr), (input_port, channel, diff))`
                (messages, empty)

            // When this is a target event `message.node` is the input port of the consuming operator
            // while `message.port` is the output port on the producing operator
            } else {
                // `messages` is `((worker, input_port, scope_addr), (output_port, channel, diff))`
                (empty, messages)
            }
        });
    let (produced, consumed) = (produced.as_collection(), consumed.as_collection());

    let shape_ids = shapes
        .map(|shape| ((shape.worker, shape.id), ()))
        .arrange_by_key();

    let mut shape_outputs =
        shapes
            .flat_map(|shape| {
                shape.outputs.clone().into_iter().map(move |output_port| {
                    ((shape.worker, output_port, shape.addr.clone()), shape.id)
                })
            })
            .join_map(
                &produced,
                |&(worker, output_port, ref _addr), &operator_id, &(input_port, channel, diff)| {
                    (
                        ((worker, operator_id, output_port, input_port, channel), ()),
                        diff as isize,
                    )
                },
            )
            .explode(iter::once)
            .count_total()
            .map(
                |(((worker, operator, output, _input, channel), ()), messages)| {
                    ((worker, operator), (output, (messages, channel)))
                },
            )
            .reduce(|_, counts, output| {
                output.push((
                    counts.iter().map(|(&count, _)| count).collect::<Vec<_>>(),
                    1isize,
                ));
            });

    shape_outputs = shape_ids
        .antijoin(&shape_outputs.keys())
        .map(|((worker, operator), ())| ((worker, operator), Vec::new()))
        .concat(&shape_outputs);

    let mut shape_inputs =
        shapes
            .flat_map(|shape| {
                shape.inputs.clone().into_iter().map(move |input_port| {
                    ((shape.worker, input_port, shape.addr.clone()), shape.id)
                })
            })
            .join_map(
                &consumed,
                |&(worker, input_port, ref _addr), &operator_id, &(output_port, channel, diff)| {
                    (
                        ((worker, operator_id, input_port, output_port, channel), ()),
                        diff as isize,
                    )
                },
            )
            .explode(iter::once)
            .count_total()
            .map(
                |(((worker, operator, input, _output, channel), ()), messages)| {
                    ((worker, operator), (input, (messages, channel)))
                },
            )
            .reduce(|_, counts, output| {
                output.push((
                    counts.iter().map(|(&count, _)| count).collect::<Vec<_>>(),
                    1isize,
                ));
            });

    shape_inputs = shape_ids
        .antijoin(&shape_inputs.keys())
        .map(|((worker, operator), ())| ((worker, operator), Vec::new()))
        .concat(&shape_inputs);

    shape_inputs
        .join(&shape_outputs)
        .map(|((worker, operator), (inputs, outputs))| {
            OperatorProgress::new(operator, worker, inputs, outputs)
        })
}
