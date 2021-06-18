use abomonation_derive::Abomonation;
#[cfg(feature = "timely-next")]
use differential_dataflow::difference::Multiply;
use differential_dataflow::difference::{Monoid, Semigroup};
use num::Bounded;
use ordered_float::OrderedFloat;
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DiffDuration(OrderedFloat<f64>);

impl DiffDuration {
    pub fn new(duration: Duration) -> Self {
        Self(OrderedFloat(duration.as_secs_f64()))
    }

    pub fn to_duration(self) -> Duration {
        if self.0 .0.is_infinite() || self.0 .0.is_sign_negative() {
            Duration::from_secs(0)
        } else {
            Duration::from_secs_f64(self.0 .0)
        }
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

    // FIXME: This is *super* hacky
    fn mul(self, rhs: isize) -> Self {
        Self(OrderedFloat(self.0 .0 * rhs as f64))
    }
}

#[cfg(feature = "timely-next")]
impl Multiply<isize> for DiffDuration {
    type Output = Self;

    fn multiply(self, &rhs: &isize) -> Self::Output {
        Self(self * rhs)
    }
}

impl Monoid for DiffDuration {
    fn zero() -> Self {
        Self::new(Duration::from_secs(0))
    }
}

impl Semigroup for DiffDuration {
    fn is_zero(&self) -> bool {
        self == &Self::zero()
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

/// A utility type to allow using [`Option`]s within differences
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub enum Maybe<T> {
    Just(T),
    Nothing,
}

impl<T> Maybe<T> {
    pub const fn just(value: T) -> Self {
        Self::Just(value)
    }

    pub const fn nothing() -> Self {
        Self::Nothing
    }

    /// Returns `true` if the maybe is [`Nothing`].
    pub const fn is_nothing(&self) -> bool {
        matches!(self, Self::Nothing)
    }
}

impl<T> Add<Self> for Maybe<T>
where
    T: Add<T, Output = T>,
{
    type Output = Self;

    fn add(self, rhs: Self) -> Self {
        match (self, rhs) {
            (Self::Just(lhs), Self::Just(rhs)) => Self::Just(lhs + rhs),
            (Self::Just(val), Self::Nothing) | (Self::Nothing, Self::Just(val)) => Self::Just(val),
            (Self::Nothing, Self::Nothing) => Self::Nothing,
        }
    }
}

impl<T> AddAssign<Self> for Maybe<T>
where
    T: AddAssign<T>,
{
    fn add_assign(&mut self, rhs: Self) {
        match (self, rhs) {
            (Self::Just(lhs), Self::Just(rhs)) => *lhs += rhs,
            (this @ Self::Nothing, Self::Just(rhs)) => *this = Self::Just(rhs),
            (Self::Just(_), Self::Nothing) | (Self::Nothing, Self::Nothing) => {}
        }
    }
}

impl<T> AddAssign<&Self> for Maybe<T>
where
    T: for<'a> AddAssign<&'a T> + Clone,
{
    fn add_assign(&mut self, rhs: &Self) {
        match (self, rhs) {
            (Self::Just(lhs), Self::Just(rhs)) => *lhs += rhs,
            (this @ Self::Nothing, Self::Just(rhs)) => *this = Self::Just(rhs.clone()),
            (Self::Just(_), Self::Nothing) | (Self::Nothing, Self::Nothing) => {}
        }
    }
}

impl<T> Mul<isize> for Maybe<T>
where
    T: Mul<isize, Output = T>,
{
    type Output = Self;

    fn mul(self, rhs: isize) -> Self {
        match self {
            Self::Just(val) => Self::Just(val * rhs),
            Self::Nothing => Self::Nothing,
        }
    }
}

#[cfg(feature = "timely-next")]
impl<T> Multiply<isize> for Maybe<T>
where
    T: Multiply<isize, Output = T>,
{
    type Output = Self;

    fn multiply(self, rhs: &isize) -> Self::Output {
        match self {
            Self::Just(val) => Self::Just(val.multiply(rhs)),
            Self::Nothing => Self::Nothing,
        }
    }
}

impl<T> Monoid for Maybe<T>
where
    T: Semigroup,
{
    fn zero() -> Self {
        Self::Nothing
    }
}

impl<T> Semigroup for Maybe<T>
where
    T: Ord + Clone + Debug + for<'a> AddAssign<&'a T> + 'static,
{
    fn is_zero(&self) -> bool {
        self.is_nothing()
    }

    #[cfg(feature = "timely-next")]
    fn plus_equals(&mut self, rhs: &Self) {
        *self += rhs;
    }
}

impl<T> Bounded for Maybe<T>
where
    T: Bounded,
{
    fn min_value() -> Self {
        Self::Nothing
    }

    fn max_value() -> Self {
        Self::Just(T::max_value())
    }
}

impl<T> From<Option<T>> for Maybe<T> {
    fn from(option: Option<T>) -> Self {
        match option {
            Some(value) => Self::Just(value),
            None => Self::Nothing,
        }
    }
}

impl<T> From<Maybe<T>> for Option<T> {
    fn from(val: Maybe<T>) -> Self {
        match val {
            Maybe::Just(value) => Some(value),
            Maybe::Nothing => None,
        }
    }
}

impl abomonation::Abomonation for DiffDuration {}
