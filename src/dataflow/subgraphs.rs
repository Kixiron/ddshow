use crate::dataflow::{
    operators::{InspectExt, IterateExt, JoinArranged},
    send_recv::ChannelAddrs,
    Channel, FilterMap, Multiply,
};
use ddshow_types::{
    timely_logging::ChannelsEvent, ChannelId, OperatorAddr, OperatorId, PortId, WorkerId,
};
use differential_dataflow::{
    difference::Abelian,
    lattice::Lattice,
    operators::{arrange::ArrangeByKey, JoinCore, Threshold},
    Collection, ExchangeData,
};
use timely::dataflow::Scope;

pub fn rewire_channels<S, D>(
    _scope: &mut S,
    channels: &Collection<S, (WorkerId, ChannelsEvent), D>,
    subgraphs: &ChannelAddrs<S, D>,
) -> Collection<S, (WorkerId, Channel), D>
where
    S: Scope,
    S::Timestamp: Lattice,
    D: Abelian + ExchangeData + Multiply<Output = D> + From<i8>,
{
    let (channels, subgraphs) = (
        channels.debug_frontier_with("channels entered rewrite_channels"),
        subgraphs,
    );

    let subgraph_crosses =
        subgraph_crosses(&channels, &subgraphs).debug_frontier_with("subgraph_crosses");
    let subgraph_normal =
        subgraph_normal(&channels, &subgraphs).debug_frontier_with("subgraph_normal");

    subgraph_crosses
        .concat(&subgraph_normal)
        .debug_frontier_with("rewire_channels final")
}

fn subgraph_crosses<S, D>(
    channels: &Collection<S, (WorkerId, ChannelsEvent), D>,
    subgraphs: &ChannelAddrs<S, D>,
) -> Collection<S, (WorkerId, Channel), D>
where
    S: Scope,
    S::Timestamp: Lattice,
    D: Abelian + ExchangeData + Multiply<Output = D> + From<i8>,
{
    let (channels, subgraphs) = (
        channels.debug_frontier_with("channels enter subgraph_crosses"),
        subgraphs,
    );

    let channels = channels
        .map(|(worker, channel)| {
            let mut source = channel.scope_addr.clone();
            source.push(channel.source[0]);

            let mut target = channel.scope_addr;
            target.push(channel.target[0]);

            (
                (worker, source, channel.source[1]),
                (
                    (target, channel.target[1]),
                    OperatorAddr::from_elem(OperatorId::new(channel.id.into_inner())),
                ),
            )
        })
        .debug_frontier_with("channels mapped");

    let channels_forward = channels.arrange_by_key_named("ArrangeByKey: Subgraph Channels");
    let channels_reverse = channels
        .map(
            |((worker, source_addr, src_channel), ((target_addr, target_channel), path))| {
                (
                    (worker, target_addr, target_channel),
                    ((source_addr, src_channel), path),
                )
            },
        )
        .debug_frontier_with("channels_reverse")
        .arrange_by_key_named("ArrangeByKey: Subgraph Channels Reversed");

    let propagated_channels =
        channels.iterate_named("Propagate Channels Over Scope Boundaries", |links| {
            let (channels_arranged, channels_reverse) = (
                channels_forward.enter(&links.scope()),
                channels_reverse.enter(&links.scope()),
            );

            let ingress_candidates = links
                .map(|((worker, source, channel), (target, path))| {
                    let mut new_target = target.0.clone();
                    new_target.push(PortId::zero());

                    ((worker, new_target, target.1), ((source, channel), path))
                })
                .arrange_by_key_named("ArrangeByKey: Ingress Candidates");

            let egress_candidates = links
                .map(|((worker, mut source, channel), (target, path))| {
                    source.push(PortId::zero());

                    ((worker, source, channel), (target, path))
                })
                .arrange_by_key_named("ArrangeByKey: Egress Candidates");

            let ingress = channels_arranged.join_core(
                &ingress_candidates,
                |&(worker, _, _), (inner, inner_vec), (outer, outer_vec)| {
                    if inner_vec != outer_vec {
                        let mut outer_vec = outer_vec.clone();
                        outer_vec.extend(inner_vec.iter());

                        Some((
                            (worker, outer.0.to_owned(), outer.1.to_owned()),
                            (inner.to_owned(), outer_vec),
                        ))
                    } else {
                        None
                    }
                },
            );

            let egress = channels_reverse.join_core(
                &egress_candidates,
                |&(worker, _, _), (inner, inner_vec), (outer, outer_vec)| {
                    if inner_vec != outer_vec {
                        let mut inner_vec = inner_vec.to_owned();
                        inner_vec.extend(outer_vec.iter());

                        Some((
                            (worker, inner.0.to_owned(), inner.1.to_owned()),
                            (outer.to_owned(), inner_vec),
                        ))
                    } else {
                        None
                    }
                },
            );

            links
                .concatenate(vec![ingress, egress])
                .debug_frontier_with("propagated_channels links, ingress and egress concat")
                .distinct_core::<D>()
                .debug_frontier_with("propagated_channels output")
        });

    propagated_channels
        .filter_map(
            |((worker, source_addr, _source_port), ((target_addr, _target_port), channel_path))| {
                if channel_path.len() >= 2 {
                    let channel = Channel::ScopeCrossing {
                        channel_id: ChannelId::new(channel_path[0].into_inner()),
                        source_addr,
                        target_addr,
                    };

                    Some(((worker, channel.target_addr().to_owned()), channel))
                } else {
                    None
                }
            },
        )
        .debug_frontier_with("propagated_channels filter_mapped")
        .antijoin_arranged(&subgraphs)
        .debug_frontier_with("propagated_channels antijoined")
        .map(|((worker, _), channel)| (worker, channel))
        .debug_frontier_with("propagated_channels mapped")
}

fn subgraph_normal<S, D>(
    channels: &Collection<S, (WorkerId, ChannelsEvent), D>,
    subgraphs: &ChannelAddrs<S, D>,
) -> Collection<S, (WorkerId, Channel), D>
where
    S: Scope,
    S::Timestamp: Lattice,
    D: Abelian + ExchangeData + Multiply<Output = D>,
{
    let (channels, subgraphs) = (
        channels.debug_frontier_with("channels entered subgraph_normal"),
        subgraphs,
    );

    let channels = channels
        .filter_map(|(worker, channel)| {
            if channel.source[0] != PortId::zero() && channel.target[0] != PortId::zero() {
                let mut source_addr = channel.scope_addr.clone();
                source_addr.push(channel.source[0]);

                let mut target_addr = channel.scope_addr;
                target_addr.push(channel.target[0]);

                Some(((worker, source_addr), (channel.id, target_addr)))
            } else {
                None
            }
        })
        .debug_frontier_with("channels filter_mapped (subgraph_normal)")
        .antijoin_arranged(&subgraphs)
        .debug_frontier_with("channels antijoined (subgraph_normal)")
        .map(|((worker, source_addr), (channel_id, target_addr))| {
            ((worker, target_addr), (channel_id, source_addr))
        })
        .debug_frontier_with("channels mapped (subgraph_normal)")
        .antijoin_arranged(&subgraphs)
        .debug_frontier_with("channels antijoined again (subgraph_normal)");

    channels
        .map(|((worker, target_addr), (channel_id, source_addr))| {
            (
                worker,
                Channel::Normal {
                    channel_id,
                    source_addr,
                    target_addr,
                },
            )
        })
        .debug_frontier_with("subgraph_normal output")
}
