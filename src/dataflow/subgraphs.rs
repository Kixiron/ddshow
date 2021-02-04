use super::{Address, Channel};
use differential_dataflow::{
    difference::{Monoid, Semigroup},
    lattice::Lattice,
    operators::{Consolidate, Join},
    Collection, ExchangeData,
};
use std::ops::{Mul, Neg};
use timely::{
    dataflow::Scope,
    logging::{ChannelsEvent, OperatesEvent},
};

pub fn rewire_channels<S, D>(
    scope: &mut S,
    channels: &Collection<S, ChannelsEvent, D>,
    operators: &Collection<S, OperatesEvent, D>,
    subgraphs: &Collection<S, Address, D>,
) -> Collection<S, Channel, D>
where
    S: Scope,
    S::Timestamp: Lattice,
    D: Semigroup + Monoid + ExchangeData + Mul<Output = D> + Neg<Output = D>,
{
    scope.region_named("Rewire Channels", |region| {
        let (channels, operators, subgraphs) = (
            channels.enter_region(region),
            operators.enter_region(region),
            subgraphs.enter_region(region),
        );

        let subgraph_ingress = subgraph_ingress(region, &channels, &operators, &subgraphs);
        let subgraph_egress = subgraph_egress(region, &channels, &operators, &subgraphs);
        let subgraph_normal = subgraph_normal(region, &channels, &operators, &subgraphs);

        subgraph_ingress
            .concat(&subgraph_egress)
            .concat(&subgraph_normal)
            .consolidate()
            .leave_region()
    })
}

fn subgraph_ingress<S, D>(
    scope: &mut S,
    channels: &Collection<S, ChannelsEvent, D>,
    operators: &Collection<S, OperatesEvent, D>,
    subgraphs: &Collection<S, Address, D>,
) -> Collection<S, Channel, D>
where
    S: Scope,
    S::Timestamp: Lattice,
    D: Semigroup + ExchangeData + Mul<Output = D>,
{
    scope.region_named("Subgraph Ingress", |region| {
        let (channels, operators, subgraphs) = (
            channels.enter_region(region),
            operators.enter_region(region),
            subgraphs.enter_region(region),
        );

        // Channels that enter into a subscope, as seen from the world outside of the subscope
        // Their source (where they come from) is the operator representing the subscope
        let entering_channels = channels
            .map(|channel| (channel.scope_addr.clone(), channel))
            .join_map(
                &operators.map(|operator| (operator.addr, operator.name)),
                |channel_addr, channel, channel_name| {
                    let mut subscope_addr = channel_addr.clone();
                    subscope_addr.push(channel.target.0);

                    (subscope_addr, (channel.clone(), channel_name.to_owned()))
                },
            )
            .semijoin(&subgraphs);

        let ingress_channels = channels.map(|channel| {
            (
                (channel.scope_addr, channel.source),
                (channel.id, channel.target),
            )
        });

        entering_channels
            .map(|(subscope_addr, (channel, channel_name))| {
                (
                    (subscope_addr, (0, channel.target.1)),
                    (channel, channel_name),
                )
            })
            .join_map(
                &ingress_channels,
                |(entrance_addr, _from), (channel, channel_name), (_channel_id, channel_target)| {
                    let mut source_addr = channel.scope_addr.clone();
                    source_addr.push(channel.source.0);

                    let mut target_addr = entrance_addr.clone();
                    target_addr.push(channel_target.0);

                    Channel::ScopeIngress {
                        channel_id: channel.id,
                        channel_addr: entrance_addr.to_owned(),
                        channel_name: channel_name.to_owned(),
                        source_addr,
                        target_addr,
                    }
                },
            )
            .leave_region()
    })
}

fn subgraph_egress<S, D>(
    scope: &mut S,
    channels: &Collection<S, ChannelsEvent, D>,
    operators: &Collection<S, OperatesEvent, D>,
    subgraphs: &Collection<S, Address, D>,
) -> Collection<S, Channel, D>
where
    S: Scope,
    S::Timestamp: Lattice,
    D: Semigroup + ExchangeData + Mul<Output = D>,
{
    scope.region_named("Subgraph Egress", |region| {
        let (channels, operators, subgraphs) = (
            channels.enter_region(region),
            operators.enter_region(region),
            subgraphs.enter_region(region),
        );

        // Channels that exit from a subscope, as seen from the world outside of the subscope
        // Their destination is the operator representing the subscope
        let exiting_channels = channels
            .map(|channel| {
                let mut subscope_addr = channel.scope_addr.clone();
                subscope_addr.push(channel.source.0);

                (subscope_addr, channel)
            })
            .semijoin(&subgraphs)
            .join_map(
                &operators.map(|operator| (operator.addr, operator.name)),
                |addr, channel, channel_name| {
                    (addr.clone(), (channel.clone(), channel_name.to_owned()))
                },
            );

        let egress_channels = channels.map(|channel| {
            (
                (channel.scope_addr, channel.target),
                (channel.id, channel.source),
            )
        });

        exiting_channels
            .map(|(subscope_addr, (channel, channel_name))| {
                (
                    (subscope_addr, (0, channel.source.1)),
                    (channel, channel_name),
                )
            })
            .join_map(
                &egress_channels,
                |(entrance_addr, _to), (channel, channel_name), (_channel_id, from)| {
                    let mut source_addr = channel.scope_addr.clone();
                    source_addr.push(from.0);

                    let mut target_addr = entrance_addr.clone();
                    target_addr.push(channel.target.0);

                    Channel::ScopeEgress {
                        channel_id: channel.id,
                        channel_addr: channel.scope_addr.clone(),
                        channel_name: channel_name.to_owned(),
                        source_addr,
                        target_addr,
                    }
                },
            )
            .leave_region()
    })
}

fn subgraph_normal<S, D>(
    scope: &mut S,
    channels: &Collection<S, ChannelsEvent, D>,
    operators: &Collection<S, OperatesEvent, D>,
    subgraphs: &Collection<S, Address, D>,
) -> Collection<S, Channel, D>
where
    S: Scope,
    S::Timestamp: Lattice,
    D: Semigroup + Monoid + ExchangeData + Mul<Output = D> + Neg<Output = D>,
{
    scope.region_named("Subgraph Normal", |region| {
        let (channels, operators, subgraphs) = (
            channels.enter_region(region),
            operators.enter_region(region),
            subgraphs.enter_region(region),
        );

        channels
            .map(|channel| {
                let mut subscope_addr = channel.scope_addr.clone();
                subscope_addr.push(channel.source.0);

                (subscope_addr, channel)
            })
            .antijoin(&subgraphs)
            .map(|(_, channel)| {
                let mut subscope_addr = channel.scope_addr.clone();
                subscope_addr.push(channel.target.0);

                (subscope_addr, channel)
            })
            .antijoin(&subgraphs)
            .map(|(_, channel)| (channel.scope_addr.clone(), channel))
            .join_map(
                &operators.map(|operator| (operator.addr, operator.name)),
                |_, channel, channel_name| {
                    let mut source_addr = channel.scope_addr.clone();
                    source_addr.push(channel.source.0);

                    let mut target_addr = channel.scope_addr.clone();
                    target_addr.push(channel.target.0);

                    Channel::Normal {
                        channel_id: channel.id,
                        channel_addr: channel.scope_addr.clone(),
                        channel_name: channel_name.to_owned(),
                        source_addr,
                        target_addr,
                    }
                },
            )
            .leave_region()
    })
}
