use crate::dataflow::{
    operators::{InspectExt, IterateExt, JoinArranged},
    send_recv::ChannelAddrs,
    utils::{Diff, Time},
    Channel, FilterMap,
};
use ddshow_types::{timely_logging::ChannelsEvent, ChannelId, OperatorAddr, OperatorId, PortId};
use differential_dataflow::{
    operators::{arrange::ArrangeByKey, JoinCore, Threshold, ThresholdTotal},
    Collection,
};
use timely::dataflow::Scope;

pub fn rewire_channels<S>(
    scope: &mut S,
    channels: &Collection<S, ChannelsEvent, Diff>,
    subgraphs: &ChannelAddrs<S, Diff>,
) -> Collection<S, Channel, Diff>
where
    S: Scope<Timestamp = Time>,
{
    scope.region_named("Rewire Channels", |region| {
        let (channels, subgraphs) = (
            channels.enter_region(region),
            subgraphs.enter_region(region),
        );

        let subgraph_crosses = subgraph_crosses(&channels, &subgraphs);
        let subgraph_normal = subgraph_normal(&channels, &subgraphs);

        subgraph_crosses
            .concat(&subgraph_normal)
            .distinct_total_core()
            .leave_region()
    })
}

fn subgraph_crosses<S>(
    channels: &Collection<S, ChannelsEvent, Diff>,
    subgraphs: &ChannelAddrs<S, Diff>,
) -> Collection<S, Channel, Diff>
where
    S: Scope<Timestamp = Time>,
{
    let channels = channels.map(|channel| {
        let mut source = channel.scope_addr.clone();
        source.push(channel.source[0]);

        let mut target = channel.scope_addr;
        target.push(channel.target[0]);

        (
            (source, channel.source[1]),
            (
                (target, channel.target[1]),
                OperatorAddr::from_elem(OperatorId::new(channel.id.into_inner())),
            ),
        )
    });

    let channels_forward = channels.arrange_by_key_named("ArrangeByKey: Subgraph Channels");
    let channels_reverse = channels
        .map(
            |((source_addr, src_channel), ((target_addr, target_channel), path))| {
                (
                    (target_addr, target_channel),
                    ((source_addr, src_channel), path),
                )
            },
        )
        .arrange_by_key_named("ArrangeByKey: Subgraph Channels Reversed");

    let propagated_channels =
        channels.iterate_named("Propagate Channels Over Scope Boundaries", |links| {
            let (channels_arranged, channels_reverse) = (
                channels_forward.enter(&links.scope()),
                channels_reverse.enter(&links.scope()),
            );

            let ingress_candidates = links
                .map(|((source, channel), (target, path))| {
                    let mut new_target = target.0.clone();
                    new_target.push(PortId::zero());

                    ((new_target, target.1), ((source, channel), path))
                })
                .arrange_by_key_named("ArrangeByKey: Ingress Candidates");

            let egress_candidates = links
                .map(|((mut source, channel), (target, path))| {
                    source.push(PortId::zero());

                    ((source, channel), (target, path))
                })
                .arrange_by_key_named("ArrangeByKey: Egress Candidates");

            let ingress = channels_arranged.join_core(
                &ingress_candidates,
                |_, (inner, inner_vec), (outer, outer_vec)| {
                    if inner_vec != outer_vec {
                        let mut outer_vec = outer_vec.clone();
                        outer_vec.extend(inner_vec.iter());

                        Some((
                            (outer.0.to_owned(), outer.1.to_owned()),
                            (inner.to_owned(), outer_vec),
                        ))
                    } else {
                        None
                    }
                },
            );

            let egress = channels_reverse.join_core(
                &egress_candidates,
                |_, (inner, inner_vec), (outer, outer_vec)| {
                    if inner_vec != outer_vec {
                        let mut inner_vec = inner_vec.to_owned();
                        inner_vec.extend(outer_vec.iter());

                        Some((
                            (inner.0.to_owned(), inner.1.to_owned()),
                            (outer.to_owned(), inner_vec),
                        ))
                    } else {
                        None
                    }
                },
            );

            links
                // TODO: Name this
                .concatenate(vec![ingress, egress])
                .threshold_named(&located!("Distinct"), |_, _| 1)
        });

    propagated_channels
        .filter_map(
            |((source_addr, _source_port), ((target_addr, _target_port), channel_path))| {
                if channel_path.len() >= 2 {
                    let channel = Channel::ScopeCrossing {
                        channel_id: ChannelId::new(channel_path[0].into_inner()),
                        source_addr,
                        target_addr,
                    };

                    Some(((channel.target_addr().to_owned()), channel))
                } else {
                    None
                }
            },
        )
        .antijoin_arranged(subgraphs)
        .map(|(_, channel)| channel)
}

fn subgraph_normal<S>(
    channels: &Collection<S, ChannelsEvent, Diff>,
    subgraphs: &ChannelAddrs<S, Diff>,
) -> Collection<S, Channel, Diff>
where
    S: Scope<Timestamp = Time>,
{
    let channels = channels
        .filter_map(|channel| {
            if channel.source[0] != PortId::zero() && channel.target[0] != PortId::zero() {
                let mut source_addr = channel.scope_addr.clone();
                source_addr.push(channel.source[0]);

                let mut target_addr = channel.scope_addr;
                target_addr.push(channel.target[0]);

                Some((source_addr, (channel.id, target_addr)))
            } else {
                None
            }
        })
        .antijoin_arranged(subgraphs)
        .map(|(source_addr, (channel_id, target_addr))| (target_addr, (channel_id, source_addr)))
        .antijoin_arranged(subgraphs);

    channels
        .map(|(target_addr, (channel_id, source_addr))| Channel::Normal {
            channel_id,
            source_addr,
            target_addr,
        })
        .debug_frontier_with("subgraph_normal output")
}
