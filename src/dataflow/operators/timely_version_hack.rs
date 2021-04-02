//! A hack to allow switching between `Mul` and `Multiply` based on the timely version

#[cfg(feature = "timely-next")]
pub trait Multiply<T = Self>:
    differential_dataflow::difference::Multiply<T, Output = <Self as Multiply<T>>::Output>
{
    type Output;
}

#[cfg(feature = "timely-next")]
impl<T, Rhs> Multiply<Rhs> for T
where
    T: differential_dataflow::difference::Multiply<Rhs>,
{
    type Output = <T as differential_dataflow::difference::Multiply<Rhs>>::Output;
}

#[cfg(not(feature = "timely-next"))]
pub trait Multiply<T = Self>: std::ops::Mul<T, Output = <Self as Multiply<T>>::Output> {
    type Output;
}

#[cfg(not(feature = "timely-next"))]
impl<T, Rhs> Multiply<Rhs> for T
where
    T: std::ops::Mul<Rhs>,
{
    type Output = <T as std::ops::Mul<Rhs>>::Output;
}
