use abomonation_derive::Abomonation;
#[cfg(feature = "timely-next")]
use differential_dataflow::difference::Multiply;
use differential_dataflow::difference::{Monoid, Semigroup};
use num::Bounded;
use std::{
    cmp,
    fmt::Debug,
    ops::{Add, AddAssign, Mul},
    time::Duration,
};

/// A type for getting the minimum value of a stream
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct Min<T> {
    pub value: T,
}

impl<T> Min<T> {
    pub const fn new(value: T) -> Self {
        Self { value }
    }
}

impl<T> Add<Self> for Min<T>
where
    T: Ord,
{
    type Output = Self;

    fn add(self, rhs: Self) -> Self {
        Self {
            value: cmp::min(self.value, rhs.value),
        }
    }
}

impl<T> AddAssign<Self> for Min<T>
where
    for<'a> &'a T: Ord,
{
    fn add_assign(&mut self, rhs: Self) {
        if &self.value > &rhs.value {
            self.value = rhs.value;
        }
    }
}

impl<T> AddAssign<&Self> for Min<T>
where
    T: Clone,
    for<'a> &'a T: Ord,
{
    fn add_assign(&mut self, rhs: &Self) {
        if &self.value > &rhs.value {
            self.value = rhs.value.clone();
        }
    }
}

#[allow(clippy::suspicious_arithmetic_impl)]
impl<T> Mul<Self> for Min<T>
where
    T: Add<T, Output = T>,
{
    type Output = Self;

    fn mul(self, rhs: Self) -> Self {
        Self {
            value: self.value + rhs.value,
        }
    }
}

impl<T> Mul<isize> for Min<T>
where
    T: Monoid + Bounded,
{
    type Output = Self;

    fn mul(self, rhs: isize) -> Self {
        if rhs < 1 {
            <Self as Monoid>::zero()
        } else {
            self
        }
    }
}

impl<T> Monoid for Min<T>
where
    T: Semigroup + Bounded,
{
    fn zero() -> Self {
        Self {
            value: T::max_value(),
        }
    }
}

impl<T> Semigroup for Min<T>
where
    T: Ord + Bounded + Clone + Debug + 'static,
{
    fn is_zero(&self) -> bool {
        self.value == T::max_value()
    }

    #[cfg(feature = "timely-next")]
    fn plus_equals(&mut self, rhs: &Self) {
        *self += rhs;
    }
}

#[cfg(feature = "timely-next")]
impl<T> Multiply<Self> for Min<T>
where
    T: Add<T, Output = T> + Clone,
{
    type Output = Self;

    fn multiply(self, rhs: &Self) -> Self::Output {
        self * rhs.clone()
    }
}

#[cfg(feature = "timely-next")]
impl<T> Multiply<isize> for Min<T>
where
    T: Monoid + Bounded,
{
    type Output = Self;

    fn multiply(self, &rhs: &isize) -> Self {
        self * rhs
    }
}

/// A type for getting the maximum value of a stream
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct Max<T> {
    pub value: T,
}

impl<T> Max<T> {
    pub const fn new(value: T) -> Self {
        Self { value }
    }

    // pub fn into_inner(self) -> T {
    //     self.value
    // }
}

impl<T> Add<Self> for Max<T>
where
    T: Ord,
{
    type Output = Self;

    fn add(self, rhs: Self) -> Self {
        Self {
            value: cmp::max(self.value, rhs.value),
        }
    }
}

impl<T> AddAssign<Self> for Max<T>
where
    for<'a> &'a T: Ord,
{
    fn add_assign(&mut self, rhs: Self) {
        if &self.value < &rhs.value {
            self.value = rhs.value;
        }
    }
}

impl<T> AddAssign<&Self> for Max<T>
where
    T: Clone,
    for<'a> &'a T: Ord,
{
    fn add_assign(&mut self, rhs: &Self) {
        if &self.value < &rhs.value {
            self.value = rhs.value.clone();
        }
    }
}

#[allow(clippy::suspicious_arithmetic_impl)]
impl<T> Mul<Self> for Max<T>
where
    T: Add<T, Output = T>,
{
    type Output = Self;

    fn mul(self, rhs: Self) -> Self {
        Self {
            value: self.value + rhs.value,
        }
    }
}

impl<T> Mul<isize> for Max<T>
where
    T: Monoid + Bounded,
{
    type Output = Self;

    fn mul(self, rhs: isize) -> Self {
        if rhs < 1 {
            <Self as Monoid>::zero()
        } else {
            self
        }
    }
}

impl<T> Monoid for Max<T>
where
    T: Semigroup + Bounded,
{
    fn zero() -> Self {
        Self {
            value: T::min_value(),
        }
    }
}

impl<T> Semigroup for Max<T>
where
    T: Ord + Bounded + Clone + Debug + 'static,
{
    fn is_zero(&self) -> bool {
        self.value == T::min_value()
    }

    #[cfg(feature = "timely-next")]
    fn plus_equals(&mut self, rhs: &Self) {
        *self += rhs;
    }
}

#[cfg(feature = "timely-next")]
impl<T> Multiply<Self> for Max<T>
where
    T: Add<T, Output = T> + Clone,
{
    type Output = Self;

    fn multiply(self, rhs: &Self) -> Self::Output {
        self * rhs.clone()
    }
}

#[cfg(feature = "timely-next")]
impl<T> Multiply<isize> for Max<T>
where
    T: Monoid + Bounded,
{
    type Output = Self;

    fn multiply(self, &rhs: &isize) -> Self {
        self * rhs
    }
}

/// A utility type to allow using [`Duration`]s within [`Max`] and [`Min`]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct DiffDuration(pub Duration);

impl DiffDuration {
    pub const fn new(duration: Duration) -> Self {
        Self(duration)
    }
}

impl Add<Self> for DiffDuration {
    type Output = Self;

    fn add(self, rhs: Self) -> Self {
        Self(self.0 + rhs.0)
    }
}

impl AddAssign<Self> for DiffDuration {
    fn add_assign(&mut self, rhs: Self) {
        self.0 += rhs.0
    }
}

impl AddAssign<&Self> for DiffDuration {
    fn add_assign(&mut self, rhs: &Self) {
        self.0 += rhs.0
    }
}

impl Mul<isize> for DiffDuration {
    type Output = Self;

    fn mul(self, rhs: isize) -> Self {
        Self(self.0 * rhs as u32)
    }
}

#[cfg(feature = "timely-next")]
impl Multiply<isize> for DiffDuration {
    type Output = Self;

    fn multiply(self, &rhs: &isize) -> Self::Output {
        Self(self.0 * rhs as u32)
    }
}

impl Monoid for DiffDuration {
    fn zero() -> Self {
        Self::new(Duration::from_secs(0))
    }
}

impl Semigroup for DiffDuration {
    fn is_zero(&self) -> bool {
        self.0 == Duration::from_secs(0)
    }

    #[cfg(feature = "timely-next")]
    fn plus_equals(&mut self, rhs: &Self) {
        *self += rhs;
    }
}

impl Bounded for DiffDuration {
    fn min_value() -> Self {
        Self::new(Duration::from_secs(u64::min_value()))
    }

    fn max_value() -> Self {
        Self::new(Duration::from_secs(u64::max_value()))
    }
}
