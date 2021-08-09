use crate::dataflow::{
    operators::Keys,
    utils::{ArrangedKey, Diff, Time},
};
use abomonation_derive::Abomonation;
use ddshow_types::{
    timely_logging::{ChannelsEvent, OperatesEvent},
    OperatorAddr, OperatorId, PortId,
};
use differential_dataflow::{
    operators::{
        arrange::{Arrange, ArrangeByKey},
        Join, JoinCore, Reduce,
    },
    Collection,
};
use serde::{Deserialize, Serialize};
use std::iter;
use timely::dataflow::Scope;

#[derive(
    Debug, Default, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation, Deserialize, Serialize,
)]
pub struct OperatorShape {
    pub id: OperatorId,
    pub addr: OperatorAddr,
    pub inputs: Vec<PortId>,
    pub outputs: Vec<PortId>,
}

impl OperatorShape {
    pub const fn new(
        id: OperatorId,
        addr: OperatorAddr,
        inputs: Vec<PortId>,
        outputs: Vec<PortId>,
    ) -> Self {
        Self {
            id,
            addr,
            inputs,
            outputs,
        }
    }
}

pub fn operator_shapes<S>(
    operators: &Collection<S, OperatesEvent, Diff>,
    channels: &Collection<S, ChannelsEvent, Diff>,
) -> Collection<S, OperatorShape, Diff>
where
    S: Scope<Timestamp = Time>,
{
    let operator_ids: ArrangedKey<_, OperatorId, Diff> = operators
        .map(|operator| (operator.id, ()))
        .arrange_named("Arrange: Operator Ids");
    let operator_addrs = operators
        .map(|operator| (operator.addr, operator.id))
        .arrange_by_key_named("Arrange: Operator Addresses");

    let inputs = channels.map(|mut channel| {
        channel.scope_addr.push(channel.target[0]);
        (channel.scope_addr, channel.target[1])
    });

    let mut operator_inputs = inputs
        .join_core(&operator_addrs, |_, &input_port, &operator_id| {
            iter::once((operator_id, input_port))
        })
        .reduce(|_, ports, output| {
            output.push((ports.iter().map(|(&port, _)| port).collect::<Vec<_>>(), 1));
        });
    operator_inputs = operator_ids
        .antijoin(&operator_inputs.keys())
        .map(|(operator_id, ())| (operator_id, Vec::new()))
        .concat(&operator_inputs);

    let outputs = channels.map(|mut channel| {
        channel.scope_addr.push(channel.source[0]);
        (channel.scope_addr, channel.source[1])
    });

    let mut operator_outputs = outputs
        .join_core(&operator_addrs, |_, &output_port, &operator_id| {
            iter::once((operator_id, output_port))
        })
        .reduce(|_, ports, output| {
            output.push((ports.iter().map(|(&port, _)| port).collect::<Vec<_>>(), 1));
        });
    operator_outputs = operator_ids
        .antijoin(&operator_outputs.keys())
        .map(|(operator_id, ())| (operator_id, Vec::new()))
        .concat(&operator_outputs);

    operators
        .map(|operator| (operator.id, operator.addr))
        .join(&operator_inputs)
        .join(&operator_outputs)
        .map(|(operator_id, ((scope_addr, inputs), outputs))| {
            OperatorShape::new(operator_id, scope_addr, inputs, outputs)
        })
}
