#![cfg(test)]

use std::ops::Deref;
use timely::{dataflow::operators::ActivateCapability, progress::Timestamp};

#[derive(Clone)]
pub struct ActivateCapabilitySet<T: Timestamp> {
    elements: Vec<ActivateCapability<T>>,
}

impl<T: Timestamp> ActivateCapabilitySet<T> {
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
    pub fn delayed(&self, time: &T) -> ActivateCapability<T> {
        self.elements
            .iter()
            .find(|c| c.time().less_than(time))
            .unwrap()
            .delayed(time)
    }
}

impl<T: Timestamp> Deref for ActivateCapabilitySet<T> {
    type Target = [ActivateCapability<T>];

    fn deref(&self) -> &[ActivateCapability<T>] {
        &self.elements
    }
}
