use super::{Address, Channel, FilterMap, Multiply};
use differential_dataflow::{
    difference::Abelian,
    lattice::Lattice,
    operators::{arrange::ArrangeByKey, Consolidate, Iterate, Join, JoinCore, Threshold},
    Collection, ExchangeData,
};
use timely::{dataflow::Scope, logging::ChannelsEvent};

pub fn rewire_channels<S, D>(
    scope: &mut S,
    channels: &Collection<S, ChannelsEvent, D>,
    subgraphs: &Collection<S, Address, D>,
) -> Collection<S, Channel, D>
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
    channels: &Collection<S, ChannelsEvent, D>,
    subgraphs: &Collection<S, Address, D>,
) -> Collection<S, Channel, D>
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

        let channels = channels.map(|channel| {
            let mut source = channel.scope_addr.clone();
            source.push(channel.source.0);

            let mut target = channel.scope_addr;
            target.push(channel.target.0);

            (
                (Address::new(source), channel.source.1),
                (
                    (Address::new(target), channel.target.1),
                    Address::new(vec![channel.id]),
                ),
            )
        });

        let channels_arranged = channels.arrange_by_key();
        let channels_reverse = channels
            .map(|(source, (target, path))| (target, (source, path)))
            .arrange_by_key();

        let propagated_channels = channels.iterate(|links| {
            let ingress_candidates = links.map(|(source, (target, path))| {
                let mut new_target = target.0.clone();
                new_target.addr.push(0);

                ((new_target, target.1), (source, path))
            });

            let egress_candidates = links.map(|(source, (target, path))| {
                let mut new_source = source.0.clone();
                new_source.addr.push(0);

                ((new_source, source.1), (target, path))
            });

            let ingress = channels_arranged.enter(&links.scope()).join_core(
                &ingress_candidates.arrange_by_key(),
                |_middle, (inner, inner_vec), (outer, outer_vec)| {
                    if inner_vec != outer_vec {
                        let mut outer_vec = outer_vec.to_owned();
                        outer_vec.addr.extend(inner_vec.addr.iter());

                        Some((outer.to_owned(), (inner.to_owned(), outer_vec)))
                    } else {
                        None
                    }
                },
            );

            let egress = channels_reverse.enter(&links.scope()).join_core(
                &egress_candidates.arrange_by_key(),
                |_middle, (inner, inner_vec), (outer, outer_vec)| {
                    if inner_vec != outer_vec {
                        let mut inner_vec = inner_vec.to_owned();
                        inner_vec.addr.extend(outer_vec.addr.iter());

                        Some((inner.to_owned(), (outer.to_owned(), inner_vec)))
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
                    (source_addr, _source_port),
                    ((target_addr, _target_port), channel_ids_along_path),
                )| Channel::ScopeCrossing {
                    channel_id: channel_ids_along_path[0],
                    source_addr,
                    target_addr,
                },
            )
            .map(|channel| (channel.target_addr(), channel))
            .antijoin(&subgraphs)
            .map(|(_, channel)| channel)
            .consolidate()
            .leave_region()
    })
}

fn subgraph_normal<S, D>(
    scope: &mut S,
    channels: &Collection<S, ChannelsEvent, D>,
    subgraphs: &Collection<S, Address, D>,
) -> Collection<S, Channel, D>
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
            .filter_map(|channel| {
                if channel.source.0 != 0 && channel.target.0 != 0 {
                    let mut source_addr = channel.scope_addr.clone();
                    source_addr.push(channel.source.0);

                    let mut target_addr = channel.scope_addr.clone();
                    target_addr.push(channel.target.0);

                    Some((
                        Address::new(source_addr),
                        (channel.id, Address::new(target_addr)),
                    ))
                } else {
                    None
                }
            })
            .antijoin(&subgraphs)
            .map(|(source_addr, (channel_id, target_addr))| {
                (target_addr, (channel_id, source_addr))
            })
            .antijoin(&subgraphs)
            .map(|(target_addr, (channel_id, source_addr))| Channel::Normal {
                channel_id,
                source_addr,
                target_addr,
            })
            .leave_region()
    })
}
