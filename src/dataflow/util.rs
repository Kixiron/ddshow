use super::Diff;
use crossbeam_channel::{Receiver, Sender};
use differential_dataflow::{
    difference::{Abelian, Semigroup},
    lattice::Lattice,
    operators::Threshold,
    Collection, Data,
};
use std::{collections::HashMap, hash::Hash, mem};
use timely::dataflow::{
    operators::capture::{Event, EventPusher, Extract},
    Scope,
};

pub trait OperatorExt<G, D, R> {
    fn distinct_named<N: AsRef<str>, R2: Abelian + From<i8>>(&self, name: N) -> Collection<G, D, R2>
    where
        Self: Threshold<G, D, R>,
        G: Scope,
        G::Timestamp: Lattice,
        D: Data,
        R: Semigroup,
    {
        self.threshold_named(name.as_ref(), |_, _| R2::from(1i8))
    }
}

impl<G: Scope, D: Data, R: Semigroup> OperatorExt<G, D, R> for Collection<G, D, R> {}

#[derive(Debug, Clone)]
pub struct CrossbeamPusher<T>(Sender<T>);

impl<T> CrossbeamPusher<T> {
    pub const fn new(sender: Sender<T>) -> Self {
        Self(sender)
    }
}

impl<T, D> EventPusher<T, D> for CrossbeamPusher<Event<T, D>> {
    fn push(&mut self, event: Event<T, D>) {
        // Ignore errors in sending events across the channel
        let _ = self.0.send(event);
    }
}

#[derive(Debug, Clone)]
pub struct CrossbeamExtractor<T>(Receiver<T>);

impl<T> CrossbeamExtractor<T> {
    pub const fn new(receiver: Receiver<T>) -> Self {
        Self(receiver)
    }
}

impl<T, D> CrossbeamExtractor<Event<T, (D, T, Diff)>>
where
    T: Ord + Hash,
    D: Ord + Hash,
{
    pub fn extract_all(self) -> Vec<D> {
        let mut data = HashMap::new();
        for (event, _time, diff) in self.extract().into_iter().flat_map(|(_, data)| data) {
            *data.entry(event).or_insert(0) += diff;
        }

        data.into_iter()
            // TODO: Should this be `diff >= 1`?
            .filter(|&(_, diff)| diff == 1)
            .map(|(data, _)| data)
            .collect()
    }
}

impl<T: Ord, D: Ord> Extract<T, D> for CrossbeamExtractor<Event<T, D>> {
    fn extract(self) -> Vec<(T, Vec<D>)> {
        let mut result = Vec::new();
        for event in self.0.try_iter() {
            if let Event::Messages(time, data) = event {
                result.push((time, data));
            }
        }
        result.sort_by(|(ts1, _), (ts2, _)| ts1.cmp(ts2));

        let mut current = 0;
        for i in 1..result.len() {
            if result[current].0 == result[i].0 {
                let dataz = mem::replace(&mut result[i].1, Vec::new());
                result[current].1.extend(dataz);
            } else {
                current = i;
            }
        }

        result.retain(|(_, data)| !data.is_empty());
        for &mut (_, ref mut data) in &mut result {
            data.sort();
        }

        result
    }
}
