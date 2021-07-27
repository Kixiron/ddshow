use crate::dataflow::{
    operators::{InspectExt, Keys},
    Diff,
};
use abomonation_derive::Abomonation;
use ddshow_types::{
    timely_logging::{ChannelsEvent, OperatesEvent},
    OperatorAddr, OperatorId, PortId, WorkerId,
};
use differential_dataflow::{
    lattice::Lattice,
    operators::{arrange::ArrangeByKey, Join, JoinCore, Reduce},
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
    pub worker: WorkerId,
    pub inputs: Vec<PortId>,
    pub outputs: Vec<PortId>,
}

impl OperatorShape {
    pub const fn new(
        id: OperatorId,
        addr: OperatorAddr,
        worker: WorkerId,
        inputs: Vec<PortId>,
        outputs: Vec<PortId>,
    ) -> Self {
        Self {
            id,
            addr,
            worker,
            inputs,
            outputs,
        }
    }
}

pub fn operator_shapes<S>(
    operators: &Collection<S, (WorkerId, OperatesEvent), Diff>,
    channels: &Collection<S, (WorkerId, ChannelsEvent), Diff>,
) -> Collection<S, OperatorShape, Diff>
where
    S: Scope,
    S::Timestamp: Lattice,
{
    let operator_ids = operators
        .map(|(worker, operator)| ((worker, operator.id), ()))
        .debug_frontier()
        .arrange_by_key();
    let operator_addrs = operators
        .map(|(worker, operator)| ((worker, operator.addr), operator.id))
        .debug_frontier()
        .arrange_by_key();

    let inputs = channels
        .map(|(worker, mut channel)| {
            channel.scope_addr.push(channel.target[0]);
            ((worker, channel.scope_addr), channel.target[1])
        })
        .debug_frontier();

    let mut operator_inputs = inputs
        .join_core(
            &operator_addrs,
            |&(worker, _), &input_port, &operator_id| {
                iter::once(((worker, operator_id), input_port))
            },
        )
        .reduce(|_, ports, output| {
            output.push((ports.iter().map(|(&port, _)| port).collect::<Vec<_>>(), 1));
        })
        .debug_frontier();
    operator_inputs = operator_ids
        .antijoin(&operator_inputs.keys())
        .map(|((worker, operator_id), ())| ((worker, operator_id), Vec::new()))
        .concat(&operator_inputs)
        .debug_frontier();

    let outputs = channels
        .map(|(worker, mut channel)| {
            channel.scope_addr.push(channel.source[0]);
            ((worker, channel.scope_addr), channel.source[1])
        })
        .debug_frontier();

    let mut operator_outputs = outputs
        .join_core(
            &operator_addrs,
            |&(worker, _), &output_port, &operator_id| {
                iter::once(((worker, operator_id), output_port))
            },
        )
        .reduce(|_, ports, output| {
            output.push((ports.iter().map(|(&port, _)| port).collect::<Vec<_>>(), 1));
        })
        .debug_frontier();
    operator_outputs = operator_ids
        .antijoin(&operator_outputs.keys())
        .map(|((worker, operator_id), ())| ((worker, operator_id), Vec::new()))
        .concat(&operator_outputs)
        .debug_frontier();

    operators
        .map(|(worker, operator)| ((worker, operator.id), operator.addr))
        .join(&operator_inputs)
        .join(&operator_outputs)
        .map(|((worker, operator_id), ((scope_addr, inputs), outputs))| {
            OperatorShape::new(operator_id, scope_addr, worker, inputs, outputs)
        })
        .debug_frontier()
}
