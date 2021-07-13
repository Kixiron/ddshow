use differential_dataflow::{difference::Semigroup, AsCollection, Collection, Data};
use std::{collections::HashMap, panic::Location};
use timely::{
    dataflow::{channels::pact::Pipeline, operators::Operator, Scope},
    progress::Timestamp,
    PartialOrder,
};

pub trait DelayExt<T> {
    type Output;

    fn delay_fast<F>(&self, delay: F) -> Self::Output
    where
        F: FnMut(&T) -> T + Clone + 'static;
}

impl<S, D, R> DelayExt<S::Timestamp> for Collection<S, D, R>
where
    S: Scope,
    S::Timestamp: Timestamp,
    D: Data,
    R: Semigroup + Data,
{
    type Output = Self;

    #[track_caller]
    fn delay_fast<F>(&self, mut delay: F) -> Self::Output
    where
        F: FnMut(&S::Timestamp) -> S::Timestamp + Clone + 'static,
    {
        let caller = Location::caller();
        let name = format!(
            "Delay @ {}:{}:{}",
            caller.file(),
            caller.line(),
            caller.column()
        );

        let mut elements = HashMap::new();
        let mut idle_buffers = Vec::new();

        self.inner
            .unary_notify(Pipeline, &name, None, move |input, output, notificator| {
                input.for_each(|time, data| {
                    let mut buffer = idle_buffers.pop().unwrap_or_default();
                    data.swap(&mut buffer);

                    // Apply the delay to the timely stream's timestamp
                    let new_time = delay(&time);
                    assert!(time.time().less_equal(&new_time));

                    // Apply the delay to the ddflow collection's timestamps
                    for (_data, time, _diff) in buffer.iter_mut() {
                        let new_time = delay(&*time);
                        debug_assert!(time.less_equal(&new_time));

                        *time = new_time;
                    }

                    elements
                        .entry(new_time.clone())
                        .or_insert_with(|| {
                            notificator.notify_at(time.delayed(&new_time));
                            Vec::new()
                        })
                        .push(buffer);
                });

                // for each available notification, send corresponding set
                notificator.for_each(|time, _, _| {
                    if let Some(mut buffers) = elements.remove(&time) {
                        for mut data in buffers.drain(..) {
                            output.session(&time).give_vec(&mut data);

                            if data.capacity() > 256 {
                                // Give the freshly empty buffer back to the list of
                                // idle buffers we maintain
                                idle_buffers.push(data);
                            }
                        }
                    }
                });

                if elements.capacity() > elements.len() * 4 {
                    elements.shrink_to_fit();
                }

                if idle_buffers.capacity() > idle_buffers.len() * 4 {
                    idle_buffers.shrink_to_fit();
                }
            })
            .as_collection()
    }
}

/*
struct OverAlignedDurationSoA<D, T> {
    // TODO: Manual SoA impl for this
    datum: Vec<D>,
    times: Vec<T>,
    // `seconds` and `nanos` will have the same number of elements,
    // but the lengths of `datum` and `times` indicate the actual
    // number of user elements
    seconds: Vec<u64>,
    nanos: Vec<u32>,
    // There may be more elements in the `seconds`/`nanos` vec than any other
    // to allow operating over it simd-wise
    length: usize,
}

impl<D, T> OverAlignedDurationSoA<D, T> {
    pub const fn new() -> Self {
        Self {
            datum: Vec::new(),
            times: Vec::new(),
            seconds: Vec::new(),
            nanos: Vec::new(),
            length: 0,
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            datum: Vec::with_capacity(capacity),
            times: Vec::with_capacity(capacity),
            seconds: Vec::with_capacity(capacity),
            nanos: Vec::with_capacity(capacity),
            length: 0,
        }
    }
}

#[cfg(target_arch = "x86_64")]
use core::arch::x86_64::{
    __m128i, __m256i, _mm256_add_epi32, _mm256_add_epi64, _mm256_blend_epi32, _mm256_cmpeq_epi32,
    _mm256_cmpgt_epi32, _mm256_set1_epi32, _mm256_set1_epi64x, _pdep_u32,
};

#[cfg(target_arch = "x86_64")]
#[allow(non_camel_case_types)]
type u64x4 = __m256i;

#[cfg(target_arch = "x86_64")]
#[allow(non_camel_case_types)]
type u32x8 = __m256i;

const NANOS_PER_SEC: u32 = 1_000_000_000;
const ABAB_MASK: u32 = 0b10101010101010101010101010101010;
const BABA_MASK: u32 = 0b01010101010101010101010101010101;

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2", enable = "bmi2")]
unsafe fn duration_saturating_add_u64_x86_avx2_bmi2(
    lhs_seconds1: u64x4,
    lhs_seconds2: u64x4,
    lhs_nanos: u32x8,
    rhs_seconds1: u64x4,
    rhs_seconds2: u64x4,
    rhs_nanos: u32x8,
) -> (u64x4, u64x4, u32x8) {
    let nanos = _mm256_add_epi32(lhs_nanos, rhs_nanos);

    // Since x86 doesn't have `>=` for some ungodly reason we have to
    // use a manual implementation of that uses `x > (MAX - 1)`
    let max_nanos = _mm256_set1_epi32((NANOS_PER_SEC - 1) as i32);
    let greater_eq_max_nanos = _mm256_cmpgt_epi32(nanos, max_nanos);

    // We add 1 to the seconds count unconditionally and then blend the two based
    // on the comparison mask
    let one = _mm256_set1_epi64x(1);
    let seconds1 = _mm256_add_epi64(lhs_seconds1, one);
    let seconds2 = _mm256_add_epi64(lhs_seconds2, one);

    // Select between `lhs_seconds1`/`lhs_seconds2` and `seconds1`/`seconds2`
    // based off of `greater_eq_max_nanos`

    todo!()
}
*/
