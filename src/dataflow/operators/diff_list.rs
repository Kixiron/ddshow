use abomonation_derive::Abomonation;
use differential_dataflow::difference::{Monoid, Semigroup};
use std::ops::{Add, AddAssign, Neg, Sub, SubAssign};

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct Nil;

impl Add for Nil {
    type Output = Self;

    fn add(self, _rhs: Self) -> Self::Output {
        Self
    }
}

impl AddAssign for Nil {
    fn add_assign(&mut self, _rhs: Self) {}
}

impl AddAssign<&Self> for Nil {
    fn add_assign(&mut self, _rhs: &Self) {}
}

impl Sub for Nil {
    type Output = Self;

    fn sub(self, _rhs: Self) -> Self::Output {
        Self
    }
}

impl SubAssign for Nil {
    fn sub_assign(&mut self, _rhs: Self) {}
}

impl SubAssign<&Self> for Nil {
    fn sub_assign(&mut self, _rhs: &Self) {}
}

impl Monoid for Nil {
    fn zero() -> Self {
        Self
    }
}

impl Semigroup for Nil {
    fn is_zero(&self) -> bool {
        true
    }
}

impl Neg for Nil {
    type Output = Self;

    fn neg(self) -> Self::Output {
        Self
    }
}
