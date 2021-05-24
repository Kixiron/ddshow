use crate::dataflow::Diff;
use crossbeam_channel::{Receiver, Sender};
use differential_dataflow::{
    difference::{Abelian, Semigroup},
    lattice::Lattice,
    operators::Threshold,
    Collection, Data,
};
use std::{collections::HashMap, fmt::Debug, hash::Hash, mem, num::NonZeroUsize};
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
    D: Ord + Hash + Clone,
{
    pub fn extract_all(self) -> Vec<D> {
        let mut data = HashMap::new();
        for (event, _time, diff) in self.extract().into_iter().flat_map(|(_, data)| data) {
            *data.entry(event).or_insert(0) += diff;
        }

        data.into_iter()
            .filter(|&(_, diff)| diff >= 1)
            .flat_map(|(data, diff)| (0..diff).map(move |_| data.clone()))
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
                let data = mem::replace(&mut result[i].1, Vec::new());
                result[current].1.extend(data);
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Fuel {
    fuel: Option<usize>,
    default: Option<usize>,
}

impl Fuel {
    pub const fn limited(fuel: NonZeroUsize) -> Self {
        Self {
            fuel: Some(fuel.get()),
            default: Some(fuel.get()),
        }
    }

    pub const fn unlimited() -> Self {
        Self {
            fuel: None,
            default: None,
        }
    }

    pub fn exert(&mut self, effort: usize) -> bool {
        if let Some(fuel) = self.fuel.as_mut() {
            *fuel = fuel.saturating_sub(effort);
            *fuel == 0
        } else {
            false
        }
    }

    pub fn is_exhausted(&self) -> bool {
        self.fuel.map_or(false, |fuel| fuel == 0)
    }

    pub fn reset(&mut self) {
        self.fuel = self.default;
    }
}

/*
#[derive(Debug)]
pub struct IterWindowsMut<'iter, Item: 'iter, const N: usize> {
    slice: &'iter mut [Item],
    start: usize,
}

impl<'iter, Item: 'iter, const N: usize> IterWindowsMut<'iter, Item, N> {
    pub fn next(&mut self) -> Option<&'_ mut [Item; N]> {
        // Advance for the next iteration
        let start = self.start;
        self.start = start.checked_add(1)?;

        let window = self
            .slice
            .get_mut(start..)?
            .get_mut(..N)?
            .try_into()
            .unwrap_or_else(|_| {
                debug_assert!(false);

                // Safety: The slice will always have a length of `N` and
                //         therefore is valid as an array of length `N`
                unsafe { hint::unreachable_unchecked() }
            });

        Some(window)
    }

    pub fn try_fold<Acc, E, F>(mut self, mut acc: Acc, mut f: F) -> Result<Acc, E>
    where
        F: FnMut(Acc, &'_ mut [Item; N]) -> Result<Acc, E>,
    {
        while let Some(window) = self.next() {
            acc = f(acc, window)?;
        }

        Ok(acc)
    }

    pub fn try_for_each<E, F>(self, mut for_each: F) -> Result<(), E>
    where
        F: FnMut(&mut [Item; N]) -> Result<(), E>,
    {
        self.try_fold((), |(), window| for_each(window))
    }

    pub fn fold<Acc, F>(self, acc: Acc, mut fold: F) -> Acc
    where
        F: FnMut(Acc, &mut [Item; N]) -> Acc,
    {
        self.try_fold(acc, |acc, window| Ok::<_, Infallible>(fold(acc, window)))
            .unwrap_or_else(|unreachable| match unreachable {})
    }

    pub fn for_each<F>(self, mut for_each: F)
    where
        F: FnMut(&mut [Item; N]),
    {
        self.fold((), |(), window| for_each(window))
    }
}

pub trait WindowsMut {
    type Item;

    fn windows_mut<const N: usize>(&mut self) -> IterWindowsMut<'_, Self::Item, N>;
}

impl<Item> WindowsMut for [Item] {
    type Item = Item;

    fn windows_mut<const N: usize>(&mut self) -> IterWindowsMut<'_, Self::Item, N> {
        IterWindowsMut {
            slice: self,
            start: 0,
        }
    }
}
*/
