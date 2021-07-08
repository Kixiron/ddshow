use crossbeam_channel::{Receiver, Sender, TryRecvError};
use differential_dataflow::{
    difference::{Abelian, Semigroup},
    lattice::Lattice,
    operators::Threshold,
    Collection, Data,
};
use std::{collections::HashMap, fmt::Debug, hash::Hash, mem, num::NonZeroUsize};
use timely::{
    dataflow::{
        operators::capture::{Event, EventPusher, Extract},
        Scope,
    },
    progress::ChangeBatch,
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

impl<T, D, R> CrossbeamExtractor<Event<T, (D, T, R)>>
where
    T: Debug + Ord + Hash + Clone,
    D: Ord + Hash + Clone,
    R: Semigroup,
{
    /// Extracts in a non-blocking manner, exerting fuel for any data pulled from the channel
    /// and returning when the fuel is exhausted or the channel is empty. Returns `true` if
    /// channel's sending side disconnects and `false` otherwise
    pub fn extract_with_fuel(
        &self,
        fuel: &mut Fuel,
        sink: &mut HashMap<D, R>,
        consumed: &mut ChangeBatch<T>,
    ) -> bool {
        while !fuel.is_exhausted() {
            // Exert one fuel for the channel receive
            fuel.exert(1);

            match self.0.try_recv() {
                // Throw all data into the given `sink`
                Ok(Event::Messages(_time, mut data)) => {
                    // `.sort_unstable_by()` is `O(n * log(n))` so we get a *very* rough estimate
                    // of the sort call's complexity to let us exert effort roughly proportional
                    // to the work we've actually done
                    //
                    // n * log₁₀(n) = n * ((64 - clz(n)) / 3)
                    let complexity =
                        data.len() * ((64 - data.len().leading_zeros() as u64) as usize / 3);
                    fuel.exert(complexity);

                    // Sort the given data by timestamp, this vaguely protects us
                    // against {over, under}flows caused by out-of-order updates
                    // but may not be strictly necessary
                    data.sort_unstable_by_key(|(_, time, _)| time.clone());

                    // Add all the data to the given sink
                    for (data, _time, diff) in data {
                        sink.entry(data).and_modify(|d| *d += &diff).or_insert(diff);
                    }
                }

                // We pretty much ignore progress events here, no idea if that's bad or not
                Ok(Event::Progress(progress)) => consumed.extend(progress.into_iter()),

                // If the channel is empty then break
                Err(TryRecvError::Empty) => break,

                // If the sending side disconnects then return
                Err(TryRecvError::Disconnected) => return true,
            }
        }

        // If we've got some spare fuel around, do some maintenance on the sink
        // by removing all values with weights of zero
        //
        // Note that this only removes entries with weights *exactly* equal to zero,
        // entries with negative weights won't be touched
        if !fuel.is_exhausted() {
            // Exert fuel for each entry in the sink since `.retain()` visits all of them
            fuel.exert(sink.len());

            // Only retain entries where the weight is zero
            sink.retain(|_, diff| !diff.is_zero());

            // If we've got a bunch of extra allocated capacity, drop it all
            // to keep our memory usage from exploding. This is overly
            // aggressive but there's not an alternative until `.shrink_to()`
            // stabilizes.
            if sink.capacity() > sink.len() * 2 {
                sink.shrink_to_fit();
            }
        }

        false
    }

    #[allow(dead_code)]
    pub fn extract_all(self) -> Vec<D> {
        let mut data = HashMap::new();
        for (event, _time, diff) in self.extract().into_iter().flat_map(|(_, data)| data) {
            data.entry(event)
                .and_modify(|d| *d += &diff)
                .or_insert(diff);
        }

        data.into_iter()
            .filter_map(|(data, diff)| if !diff.is_zero() { Some(data) } else { None })
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
                let data = mem::take(&mut result[i].1);
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
pub enum Fuel {
    Unlimited,
    Limited { fuel: usize, default: NonZeroUsize },
}

impl Fuel {
    pub const fn limited(fuel: NonZeroUsize) -> Self {
        Self::Limited {
            fuel: fuel.get(),
            default: fuel,
        }
    }

    pub const fn unlimited() -> Self {
        Self::Unlimited
    }

    pub const fn is_unlimited(&self) -> bool {
        matches!(self, Self::Unlimited)
    }

    pub fn exert(&mut self, effort: usize) -> bool {
        if let Self::Limited { fuel, .. } = self {
            *fuel = fuel.saturating_sub(effort);
            *fuel == 0
        } else {
            false
        }
    }

    pub const fn is_exhausted(&self) -> bool {
        match *self {
            Self::Unlimited => false,
            Self::Limited { fuel, .. } => fuel == 0,
        }
    }

    pub fn reset(&mut self) {
        if let Self::Limited { fuel, default } = self {
            *fuel = default.get();
        }
    }

    pub const fn remaining(&self) -> Option<usize> {
        if let Self::Limited { fuel, default } = *self {
            Some(default.get() - fuel)
        } else {
            None
        }
    }

    pub const fn used(&self) -> Option<usize> {
        if let Self::Limited { fuel, default } = *self {
            Some(default.get() - (default.get() - fuel))
        } else {
            None
        }
    }
}

macro_rules! located {
    ($name:expr) => {{
        let caller = ::std::panic::Location::caller();
        ::std::format!(
            "{} @ {}:{}:{}",
            $name,
            caller.file(),
            caller.line(),
            caller.column(),
        )
    }};
}
