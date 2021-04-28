use differential_dataflow::{
    difference::{Abelian, Semigroup},
    lattice::Lattice,
    operators::{
        arrange::{ArrangeByKey, Arranged},
        JoinCore,
    },
    trace::{BatchReader, Cursor, TraceReader},
    Collection, Data, ExchangeData, Hashable,
};
use std::ops::Mul;
use timely::dataflow::Scope;

pub trait JoinArranged<S, K, V, R>
where
    S: Scope,
    K: Data,
    V: Data,
    R: Semigroup,
{
    fn semijoin_arranged<R2, T>(
        &self,
        other: &Arranged<S, T>,
    ) -> Collection<S, (K, V), <R as Mul<R2>>::Output>
    where
        S::Timestamp: Lattice,
        K: ExchangeData,
        R2: ExchangeData + Semigroup,
        R: Mul<R2>,
        <R as Mul<R2>>::Output: Semigroup,
        T: TraceReader<Time = S::Timestamp, Key = K, Val = (), R = R2> + Clone + 'static,
        T::Batch: BatchReader<K, (), S::Timestamp, R2> + 'static,
        T::Cursor: Cursor<K, (), S::Timestamp, R2> + 'static;

    fn antijoin_arranged<R2, T>(&self, other: &Arranged<S, T>) -> Collection<S, (K, V), R>
    where
        S::Timestamp: Lattice,
        K: ExchangeData,
        R2: ExchangeData + Semigroup,
        R: Mul<R2, Output = R>,
        R: Abelian,
        T: TraceReader<Time = S::Timestamp, Key = K, Val = (), R = R2> + Clone + 'static,
        T::Batch: BatchReader<K, (), S::Timestamp, R2> + 'static,
        T::Cursor: Cursor<K, (), S::Timestamp, R2> + 'static;
}

impl<S, K, V, R> JoinArranged<S, K, V, R> for Collection<S, (K, V), R>
where
    S: Scope,
    K: ExchangeData + Hashable,
    V: ExchangeData,
    R: Semigroup + ExchangeData,
{
    fn semijoin_arranged<R2, T>(
        &self,
        other: &Arranged<S, T>,
    ) -> Collection<S, (K, V), <R as Mul<R2>>::Output>
    where
        S::Timestamp: Lattice,
        K: ExchangeData,
        R2: ExchangeData + Semigroup,
        R: Mul<R2>,
        <R as Mul<R2>>::Output: Semigroup,
        T: TraceReader<Time = S::Timestamp, Key = K, Val = (), R = R2> + Clone + 'static,
        T::Batch: BatchReader<K, (), S::Timestamp, R2> + 'static,
        T::Cursor: Cursor<K, (), S::Timestamp, R2> + 'static,
    {
        self.arrange_by_key()
            .join_core(other, |k, v, _| Some((k.clone(), v.clone())))
    }

    fn antijoin_arranged<R2, T>(&self, other: &Arranged<S, T>) -> Collection<S, (K, V), R>
    where
        S::Timestamp: Lattice,
        K: ExchangeData,
        R2: ExchangeData + Semigroup,
        R: Mul<R2, Output = R>,
        R: Abelian,
        T: TraceReader<Time = S::Timestamp, Key = K, Val = (), R = R2> + Clone + 'static,
        T::Batch: BatchReader<K, (), S::Timestamp, R2> + 'static,
        T::Cursor: Cursor<K, (), S::Timestamp, R2> + 'static,
    {
        self.concat(&self.semijoin_arranged(other).negate())
    }
}

impl<S, K, V, R, T1> JoinArranged<S, K, V, R> for Arranged<S, T1>
where
    S: Scope,
    S::Timestamp: Lattice,
    K: ExchangeData + Hashable,
    V: ExchangeData,
    R: Semigroup + ExchangeData,
    T1: TraceReader<Time = S::Timestamp, Key = K, Val = V, R = R> + Clone + 'static,
    T1::Batch: BatchReader<K, V, S::Timestamp, R> + 'static,
    T1::Cursor: Cursor<K, V, S::Timestamp, R> + 'static,
{
    fn semijoin_arranged<R2, T>(
        &self,
        other: &Arranged<S, T>,
    ) -> Collection<S, (K, V), <R as Mul<R2>>::Output>
    where
        S::Timestamp: Lattice,
        K: ExchangeData,
        R2: ExchangeData + Semigroup,
        R: Mul<R2>,
        <R as Mul<R2>>::Output: Semigroup,
        T: TraceReader<Time = S::Timestamp, Key = K, Val = (), R = R2> + Clone + 'static,
        T::Batch: BatchReader<K, (), S::Timestamp, R2> + 'static,
        T::Cursor: Cursor<K, (), S::Timestamp, R2> + 'static,
    {
        self.join_core(other, |k, v, _| Some((k.clone(), v.clone())))
    }

    fn antijoin_arranged<R2, T>(&self, other: &Arranged<S, T>) -> Collection<S, (K, V), R>
    where
        S::Timestamp: Lattice,
        K: ExchangeData,
        R2: ExchangeData + Semigroup,
        R: Mul<R2, Output = R>,
        R: Abelian,
        T: TraceReader<Time = S::Timestamp, Key = K, Val = (), R = R2> + Clone + 'static,
        T::Batch: BatchReader<K, (), S::Timestamp, R2> + 'static,
        T::Cursor: Cursor<K, (), S::Timestamp, R2> + 'static,
    {
        self.as_collection(|key, val| (key.clone(), val.clone()))
            .concat(&self.semijoin_arranged(other).negate())
    }
}
