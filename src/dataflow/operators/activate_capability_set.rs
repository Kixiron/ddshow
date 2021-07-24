#![cfg(test)]

use std::{fmt::Debug, ops::Deref, panic::Location};
use timely::{dataflow::operators::ActivateCapability, progress::Timestamp};

#[derive(Clone)]
pub struct ActivateCapabilitySet<T>
where
    T: Timestamp,
{
    elements: Vec<ActivateCapability<T>>,
}

#[allow(dead_code)]
impl<T> ActivateCapabilitySet<T>
where
    T: Timestamp,
{
    pub fn from_elem(capability: ActivateCapability<T>) -> Self {
        Self {
            elements: vec![capability],
        }
    }

    /// Inserts `capability` into the set, discarding redundant capabilities.
    pub fn insert(&mut self, capability: ActivateCapability<T>) {
        if !self
            .elements
            .iter()
            .any(|cap| cap.time().less_than(capability.time()))
        {
            self.elements
                .retain(|cap| !capability.time().less_than(cap.time()));
            self.elements.push(capability);
        }
    }

    /// Creates a new capability to send data at `time`.
    ///
    /// This method panics if there does not exist a capability in `self.elements` less than `time`.
    #[track_caller]
    pub fn delayed(&self, time: &T) -> ActivateCapability<T> {
        #[cold]
        #[inline(never)]
        fn invalid_delayed_panic(time: &dyn Debug, location: &Location) -> ! {
            panic!(
                "no time less than {:?} could be found within `ActivateCapabilitySet::delayed()` (called from {}:{}:{})",
                time, location.file(), location.line(), location.column(),
            )
        }
        let caller = Location::caller();

        self.elements
            .iter()
            .find(|capability| capability.time().less_than(time))
            .unwrap_or_else(|| invalid_delayed_panic(time, caller))
            .delayed(time)
    }
}

impl<T> From<Vec<ActivateCapability<T>>> for ActivateCapabilitySet<T>
where
    T: Timestamp,
{
    fn from(elements: Vec<ActivateCapability<T>>) -> Self {
        Self { elements }
    }
}

impl<T: Timestamp> Deref for ActivateCapabilitySet<T> {
    type Target = [ActivateCapability<T>];

    fn deref(&self) -> &[ActivateCapability<T>] {
        &self.elements
    }
}
