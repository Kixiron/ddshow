use abomonation_derive::Abomonation;
use differential_dataflow::difference::{Monoid, Semigroup};
use std::ops::{Add, AddAssign, Neg, Sub, SubAssign};

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Abomonation)]
pub struct DiffList<A, B>(A, B);

impl<A, B> Add for DiffList<A, B>
where
    A: Add<Output = A>,
    B: Add<Output = B>,
{
    type Output = Self;

    fn add(self, Self(a, b): Self) -> Self::Output {
        Self(self.0 + a, self.1 + b)
    }
}

impl<A, B> Add<&Self> for DiffList<A, B>
where
    for<'a> A: Add<&'a A, Output = A>,
    for<'a> B: Add<&'a B, Output = B>,
{
    type Output = Self;

    fn add(self, Self(a, b): &Self) -> Self::Output {
        Self(self.0 + a, self.1 + b)
    }
}

impl<A, B> AddAssign for DiffList<A, B>
where
    A: AddAssign,
    B: AddAssign,
{
    fn add_assign(&mut self, Self(a, b): Self) {
        self.0 += a;
        self.1 += b;
    }
}

impl<A, B> AddAssign<&Self> for DiffList<A, B>
where
    for<'a> A: AddAssign<&'a A>,
    for<'a> B: AddAssign<&'a B>,
{
    fn add_assign(&mut self, Self(a, b): &Self) {
        self.0 += a;
        self.1 += b;
    }
}

impl<A, B> Sub for DiffList<A, B>
where
    A: Sub<Output = A>,
    B: Sub<Output = B>,
{
    type Output = Self;

    fn sub(self, Self(a, b): Self) -> Self::Output {
        Self(self.0 - a, self.1 - b)
    }
}

impl<A, B> Sub<&Self> for DiffList<A, B>
where
    for<'a> A: Sub<&'a A, Output = A>,
    for<'a> B: Sub<&'a B, Output = B>,
{
    type Output = Self;

    fn sub(self, Self(a, b): &Self) -> Self::Output {
        Self(self.0 - a, self.1 - b)
    }
}

impl<A, B> SubAssign for DiffList<A, B>
where
    A: SubAssign,
    B: SubAssign,
{
    fn sub_assign(&mut self, Self(a, b): Self) {
        self.0 -= a;
        self.1 -= b;
    }
}

impl<A, B> SubAssign<&Self> for DiffList<A, B>
where
    for<'a> A: SubAssign<&'a A>,
    for<'a> B: SubAssign<&'a B>,
{
    fn sub_assign(&mut self, Self(a, b): &Self) {
        self.0 -= a;
        self.1 -= b;
    }
}

impl<A, B> Monoid for DiffList<A, B>
where
    A: Monoid,
    B: Monoid,
{
    fn zero() -> Self {
        Self(A::zero(), B::zero())
    }
}

impl<A, B> Semigroup for DiffList<A, B>
where
    A: Semigroup,
    B: Semigroup,
{
    fn is_zero(&self) -> bool {
        self.0.is_zero() && self.1.is_zero()
    }
}

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
