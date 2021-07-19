use differential_dataflow::{difference::Semigroup, Collection, Data};
use std::{fmt::Debug, panic::Location};
use timely::dataflow::{operators::Inspect, Scope, Stream};

pub trait InspectExt {
    type Value;

    fn debug_inspect<F>(&self, inspect: F) -> Self
    where
        F: FnMut(&Self::Value) + 'static;

    #[track_caller]
    fn debug(&self) -> Self
    where
        Self: Sized,
        Self::Value: Debug,
    {
        let location = Location::caller();
        self.debug_inspect(move |value| {
            println!(
                "[{}:{}:{}]: {:?}",
                location.file(),
                location.line(),
                location.column(),
                value,
            );
        })
    }

    #[track_caller]
    fn debug_with(&self, name: &str) -> Self
    where
        Self: Sized,
        Self::Value: Debug,
    {
        let name = name.to_owned();
        self.debug_inspect(move |value| {
            println!("[{}]: {:?}", name, value);
        })
    }
}

impl<S, D, R> InspectExt for Collection<S, D, R>
where
    S: Scope,
    D: Data,
    R: Semigroup,
{
    type Value = (D, S::Timestamp, R);

    fn debug_inspect<F>(&self, inspect: F) -> Self
    where
        F: FnMut(&Self::Value) + 'static,
    {
        if cfg!(debug_assertions) {
            self.inspect(inspect)
        } else {
            self.clone()
        }
    }
}

impl<S, D> InspectExt for Stream<S, D>
where
    S: Scope,
    D: Data,
{
    type Value = D;

    fn debug_inspect<F>(&self, inspect: F) -> Self
    where
        F: FnMut(&Self::Value) + 'static,
    {
        if cfg!(debug_assertions) {
            self.inspect(inspect)
        } else {
            self.clone()
        }
    }
}

impl<S> InspectExt for Option<S>
where
    S: InspectExt,
{
    type Value = <S as InspectExt>::Value;

    fn debug_inspect<F>(&self, inspect: F) -> Self
    where
        F: FnMut(&Self::Value) + 'static,
    {
        self.as_ref().map(|stream| stream.debug_inspect(inspect))
    }
}

// TODO: InspectExt for arrangements

pub trait DebugTuple {
    fn debug(&self) -> Self;
}

macro_rules! impl_tuple {
    ($(($($ty:ident,)+)),* $(,)?) => {
        $(
            #[allow(non_snake_case)]
            impl<$($ty,)+> DebugTuple for ($($ty,)+)
            where
                $($ty: InspectExt,)+
                $(<$ty as InspectExt>::Value: Debug,)+
            {
                fn debug(&self) -> Self {
                    let ($($ty,)+) = self;
                    ($($ty.debug(),)+)
                }
            }
        )*
    };
}

impl_tuple! {
    (A,),
    (A, B,),
    (A, B, C,),
    (A, B, C, D,),
    (A, B, C, D, E,),
    (A, B, C, D, E, F,),
    (A, B, C, D, E, F, G,),
    (A, B, C, D, E, F, G, H,),
    (A, B, C, D, E, F, G, H, I,),
    (A, B, C, D, E, F, G, H, I, J,),
    (A, B, C, D, E, F, G, H, I, J, K,),
    (A, B, C, D, E, F, G, H, I, J, K, L,),
    (A, B, C, D, E, F, G, H, I, J, K, L, M,),
    (A, B, C, D, E, F, G, H, I, J, K, L, M, N,),
    (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O,),
    (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P,),
}
