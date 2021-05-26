use crate::dataflow::{
    operators::{JoinArranged, RkyvChannelsEvent},
    send_recv::ChannelAddrs,
    Channel, ChannelId, FilterMap, Multiply, OperatorAddr, PortId, WorkerId,
};
use differential_dataflow::{
    difference::Abelian,
    lattice::Lattice,
    operators::{arrange::ArrangeByKey, Consolidate, Iterate, JoinCore, Threshold},
    Collection, ExchangeData,
};
use timely::dataflow::Scope;

pub fn rewire_channels<S, D>(
    scope: &mut S,
    channels: &Collection<S, (WorkerId, RkyvChannelsEvent), D>,
    subgraphs: &ChannelAddrs<S, D>,
) -> Collection<S, (WorkerId, Channel), D>
where
    S: Scope,
    S::Timestamp: Lattice,
    D: Abelian + ExchangeData + Multiply<Output = D> + From<i8>,
{
    scope.region_named("Rewire Channels", |region| {
        let (channels, subgraphs) = (
            channels.enter_region(region),
            subgraphs.enter_region(region),
        );

        let subgraph_crosses = subgraph_crosses(region, &channels, &subgraphs);
        let subgraph_normal = subgraph_normal(region, &channels, &subgraphs);

        subgraph_crosses
            .concat(&subgraph_normal)
            .consolidate()
            .leave_region()
    })
}

fn subgraph_crosses<S, D>(
    scope: &mut S,
    channels: &Collection<S, (WorkerId, RkyvChannelsEvent), D>,
    subgraphs: &ChannelAddrs<S, D>,
) -> Collection<S, (WorkerId, Channel), D>
where
    S: Scope,
    S::Timestamp: Lattice,
    D: Abelian + ExchangeData + Multiply<Output = D> + From<i8>,
{
    scope.region_named("Subgraph Crosses", |region| {
        let (channels, subgraphs) = (
            channels.enter_region(region),
            subgraphs.enter_region(region),
        );

        let channels = channels.map(|(worker, channel)| {
            let mut source = channel.scope_addr.clone();
            source.push(channel.source.0);

            let mut target = channel.scope_addr;
            target.push(channel.target.0);

            (
                (worker, source, channel.source.1),
                (
                    (target, channel.target.1),
                    OperatorAddr::from_elem(channel.id.into_inner()),
                ),
            )
        });

        let channels_arranged = channels.arrange_by_key();
        let channels_reverse = channels
            .map(
                |((worker, source_addr, src_channel), ((target_addr, target_channel), path))| {
                    (
                        (worker, target_addr, target_channel),
                        ((source_addr, src_channel), path),
                    )
                },
            )
            .arrange_by_key();

        let propagated_channels = channels.iterate(|links| {
            let ingress_candidates = links.map(|((worker, source, channel), (target, path))| {
                let mut new_target = target.0.clone();
                new_target.push(PortId::zero());

                ((worker, new_target, target.1), ((source, channel), path))
            });

            let egress_candidates = links.map(|((worker, mut source, channel), (target, path))| {
                source.push(PortId::zero());

                ((worker, source, channel), (target, path))
            });

            let ingress = channels_arranged.enter(&links.scope()).join_core(
                &ingress_candidates.arrange_by_key(),
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

            let egress = channels_reverse.enter(&links.scope()).join_core(
                &egress_candidates.arrange_by_key(),
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

            links.concatenate(vec![ingress, egress]).distinct_core()
        });

        propagated_channels
            .filter(|(_, (_, path))| path.len() >= 2)
            // .reduce(|_source, input, output| {
            //     if let Some((target, path)) = input
            //         .iter()
            //         .filter(|((_, path), _)| path.len() >= 2)
            //         .max_by_key(|((_, path), _)| path.len())
            //         .map(|((target, path), _diff)| (target.to_owned(), path.to_owned()))
            //     {
            //         output.push(((target, path), D::from(1)));
            //     }
            // })
            .map(
                |(
                    (worker, source_addr, _source_port),
                    ((target_addr, _target_port), channel_ids_along_path),
                )| {
                    (
                        worker,
                        Channel::ScopeCrossing {
                            channel_id: ChannelId::new(channel_ids_along_path[0]),
                            source_addr,
                            target_addr,
                        },
                    )
                },
            )
            .map(|(worker, channel)| ((worker, channel.target_addr()), channel))
            .antijoin_arranged(&subgraphs)
            .map(|((worker, _), channel)| (worker, channel))
            .consolidate()
            .leave_region()
    })
}

fn subgraph_normal<S, D>(
    scope: &mut S,
    channels: &Collection<S, (WorkerId, RkyvChannelsEvent), D>,
    subgraphs: &ChannelAddrs<S, D>,
) -> Collection<S, (WorkerId, Channel), D>
where
    S: Scope,
    S::Timestamp: Lattice,
    D: Abelian + ExchangeData + Multiply<Output = D>,
{
    scope.region_named("Subgraph Normal", |region| {
        let (channels, subgraphs) = (
            channels.enter_region(region),
            subgraphs.enter_region(region),
        );

        channels
            .filter_map(|(worker, channel)| {
                if channel.source.0 != PortId::zero() && channel.target.0 != PortId::zero() {
                    let mut source_addr = channel.scope_addr.clone();
                    source_addr.push(channel.source.0);

                    let mut target_addr = channel.scope_addr;
                    target_addr.push(channel.target.0);

                    Some(((worker, source_addr), (channel.id, target_addr)))
                } else {
                    None
                }
            })
            .antijoin_arranged(&subgraphs)
            .map(|((worker, source_addr), (channel_id, target_addr))| {
                ((worker, target_addr), (channel_id, source_addr))
            })
            .antijoin_arranged(&subgraphs)
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
            .leave_region()
    })
}
