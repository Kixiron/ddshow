use crate::dataflow::utils::XXHasher;
use differential_dataflow::{consolidation, difference::Semigroup, AsCollection, Collection, Data};
use std::{
    collections::{BinaryHeap, HashMap},
    panic::Location,
};
use timely::{
    dataflow::{channels::pact::Pipeline, operators::Operator, Scope, Stream},
    progress::Timestamp,
    PartialOrder,
};
use tinyvec::TinyVec;

type Element<T, D, R> = TinyVec<[Vec<(D, T, R)>; 16]>;

pub trait DelayExt<T> {
    type Output;

    fn delay_fast<F>(&self, delay: F) -> Self::Output
    where
        F: FnMut(&T) -> T + Clone + 'static;
}

impl<S, D> DelayExt<S::Timestamp> for Stream<S, D>
where
    S: Scope,
    S::Timestamp: Timestamp,
    D: Data,
{
    type Output = Self;

    #[inline]
    #[track_caller]
    fn delay_fast<F>(&self, mut delay: F) -> Self::Output
    where
        F: FnMut(&S::Timestamp) -> S::Timestamp + Clone + 'static,
    {
        let name = located!("Delay");

        let mut elements = HashMap::with_hasher(XXHasher::default());
        let mut idle_buffers = Vec::new();

        self.unary_notify(Pipeline, &name, None, move |input, output, notificator| {
            input.for_each(|time, data| {
                let mut buffer = idle_buffers.pop().unwrap_or_default();
                data.swap(&mut buffer);

                let new_time = delay(&time);
                debug_assert!(time.time().less_equal(&new_time));

                notificator.notify_at(time.delayed(&new_time));
                elements
                    .entry(new_time)
                    .or_insert_with(TinyVec::<[_; 16]>::new)
                    .push(buffer);
            });

            // for each available notification, send corresponding set
            notificator.for_each(|time, _, _| {
                if let Some(data) = elements.remove(&time) {
                    for mut buffer in data.into_iter() {
                        output.session(&time).give_vec(&mut buffer);
                        idle_buffers.push(buffer);
                    }
                }
            });
        })
    }
}

impl<S, D, R> DelayExt<S::Timestamp> for Collection<S, D, R>
where
    S: Scope,
    S::Timestamp: Timestamp,
    D: Data,
    R: Semigroup + Data,
{
    type Output = Self;

    #[inline]
    #[track_caller]
    fn delay_fast<F>(&self, mut delay: F) -> Self::Output
    where
        F: FnMut(&S::Timestamp) -> S::Timestamp + Clone + 'static,
    {
        let caller = Location::caller();
        let name = located!("Delay");

        let mut elements = HashMap::with_hasher(XXHasher::default());
        let mut idle_buffers = Vec::new();
        let mut dirty = BinaryHeap::new();

        self.inner
            .unary_notify(Pipeline, &name, None, move |input, output, notificator| {
                input.for_each(|time, data| {
                    let mut buffer = idle_buffers.pop().unwrap_or_default();
                    data.swap(&mut buffer);

                    // Apply the delay to the timely stream's timestamp
                    let stream_time = delay(&time);
                    debug_assert!(
                        time.time().less_equal(&stream_time),
                        "the delayed time is less than the stream's original time: {:#?} < {:#?}",
                        stream_time,
                        time,
                    );

                    // Apply the delay to the ddflow collection's timestamps
                    // FIXME: We probably want to do this in activation time instead of
                    //        during inputs to try and mitigate input overload
                    for (_data, differential_time, _diff) in buffer.iter_mut() {
                        let new_time = delay(&*differential_time);
                        debug_assert!(
                            differential_time.less_equal(&new_time),
                            "differential time is greater than the differential time: {:#?} < {:#?}",
                            new_time,
                            differential_time,
                        );
                        // The ddflow-level timestamp must be greater than or equal to the
                        // stream's timestamp
                        debug_assert!(
                            stream_time.less_equal(&new_time),
                            "differential time is less than than the stream time: {:#?} < {:#?}",
                            new_time,
                            stream_time,
                        );

                        *differential_time = new_time;
                    }

                    notificator.notify_at(time.delayed(&stream_time));
                    elements
                        .entry(stream_time.clone())
                        .or_insert_with(TinyVec::<[_; 16]>::new)
                        .push(buffer);
                    dirty.push(stream_time);
                });

                // Perform some cleanup on the data we have just sitting around
                compact_delayed_buffers(&mut dirty, &mut elements, &mut idle_buffers);

                // for each available notification, send corresponding set
                notificator.for_each(|time, _, notificator| {
                    tracing::trace!(
                        target: "delay_frontiers",
                        notify_time = ?time.time(),
                        frontier = ?notificator.frontier(0),
                        "DelayFast @ {}:{}:{}",
                        caller.file(),
                        caller.line(),
                        caller.column(),
                    );

                    if let Some(mut buffers) = elements.remove(&time) {
                        for mut data in buffers.drain(..) {
                            output.session(&time).give_vec(&mut data);

                            // Give the freshly empty buffer back to the list of
                            // idle buffers we maintain
                            idle_buffers.push(data);
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

/// Consolidate updates within their timestamp
fn compact_delayed_buffers<T, D, R>(
    dirty: &mut BinaryHeap<T>,
    elements: &mut HashMap<T, Element<T, D, R>, XXHasher>,
    idle_buffers: &mut Vec<Vec<(D, T, R)>>,
) where
    T: Timestamp,
    D: Data,
    R: Semigroup,
{
    // TODO: Maybe add an actual fuel mechanism?
    for _ in 0..50 {
        if let Some(dirty_time) = dirty.pop() {
            // TODO: Maybe remove the entry if it has no update vectors in it?
            if let Some(updates) = elements.get_mut(&dirty_time) {
                if let Some(mut consolidated) = updates.pop() {
                    // Reserve the required space to combine all the buffers
                    consolidated.reserve(updates.iter().map(|updates| updates.len()).sum());

                    // FIXME: A k-way merge would be more efficient
                    for mut updates in updates.drain(..) {
                        consolidated.extend(updates.drain(..));

                        // Give the freshly empty buffer back to the list of
                        // idle buffers we maintain
                        idle_buffers.push(updates);
                    }

                    // Compact the updates within the newly filled vector
                    consolidation::consolidate_updates(&mut consolidated);

                    // Skip pushing the consolidated vector if it's empty
                    if !consolidated.is_empty() {
                        updates.push(consolidated);
                    }
                }
            }
        } else {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::compact_delayed_buffers;
    use crate::dataflow::utils::XXHasher;
    use std::{
        collections::{BinaryHeap, HashMap},
        time::Duration,
    };
    use tinyvec::{tiny_vec, TinyVec};

    #[test]
    fn buffers_are_compacted() {
        let mut dirty = BinaryHeap::new();
        dirty.push(Duration::from_secs(10));
        dirty.push(Duration::from_secs(20));
        dirty.push(Duration::from_secs(6));

        let mut elements = HashMap::with_hasher(XXHasher::default());
        elements.insert(
            Duration::from_secs(10),
            tiny_vec![
                vec![("foobar", Duration::from_secs(11), -2)],
                vec![
                    ("foo", Duration::from_secs(10), 10),
                    ("foo", Duration::from_secs(10), 10463),
                ],
                vec![("foobar", Duration::from_secs(11), 5)]
            ],
        );
        elements.insert(
            Duration::from_secs(20),
            tiny_vec![
                vec![("fo42obar", Duration::from_secs(21), -652)],
                vec![
                    ("ffgdfoo", Duration::from_secs(24), 1560),
                    ("f45yoo", Duration::from_secs(30), 130),
                ],
                vec![("foobar", Duration::from_secs(22), 5)]
            ],
        );
        elements.insert(
            Duration::from_secs(6),
            tiny_vec![
                vec![("ffdsgr", Duration::from_secs(6), -2)],
                vec![
                    ("fgsdfg", Duration::from_secs(8), 100),
                    ("foo", Duration::from_secs(10), 143),
                ],
                vec![("foofdssdfar", Duration::from_secs(6), 57)],
                vec![("foofdssdfar", Duration::from_secs(6), -57)],
                vec![("foofdssdfar", Duration::from_secs(6), 57)],
                vec![("foofdssdfar", Duration::from_secs(6), -60)]
            ],
        );

        let mut idle_buffers = Vec::new();

        compact_delayed_buffers(&mut dirty, &mut elements, &mut idle_buffers);

        let mut expected = HashMap::with_hasher(XXHasher::default());
        expected.insert(
            Duration::from_secs(6),
            tiny_vec![
                [Vec<(&str, Duration, isize)>; 16] => vec![
                    ("ffdsgr", Duration::from_secs(6), -2),
                    ("fgsdfg", Duration::from_secs(8), 100),
                    ("foo", Duration::from_secs(10), 143),
                    ("foofdssdfar", Duration::from_secs(6), -3),
                ],
            ],
        );
        expected.insert(
            Duration::from_secs(10),
            tiny_vec![
                [Vec<(&str, Duration, isize)>; 16] => vec![
                    ("foo", Duration::from_secs(10), 10473),
                    ("foobar", Duration::from_secs(11), 3),
                ],
            ],
        );
        expected.insert(
            Duration::from_secs(20),
            tiny_vec![
                [Vec<(&str, Duration, isize)>; 16] => vec![
                    ("f45yoo", Duration::from_secs(30), 130),
                    ("ffgdfoo", Duration::from_secs(24), 1560),
                    ("fo42obar", Duration::from_secs(21), -652),
                    ("foobar", Duration::from_secs(22), 5),
                ],
            ],
        );

        assert_eq!(elements, expected);
    }

    // Regression test for the case when an element has a single buffer.
    // There was previously a bug where it infinitely looped when this
    // happened
    #[test]
    fn single_buffer_in_element() {
        let mut dirty = BinaryHeap::new();
        dirty.push(Duration::from_secs(10));

        let mut elements = HashMap::with_hasher(XXHasher::default());
        elements.insert(
            Duration::from_secs(10),
            tiny_vec![
                [Vec<(&str, Duration, isize)>; 16] => vec![
                    ("foo", Duration::from_secs(10), 10),
                    ("foo", Duration::from_secs(10), -10),
                ],
            ],
        );

        let mut idle_buffers = Vec::new();

        compact_delayed_buffers(&mut dirty, &mut elements, &mut idle_buffers);

        assert!(dirty.is_empty());
        assert_eq!(elements.len(), 1);
        assert_eq!(elements[&Duration::from_secs(10)], TinyVec::new());
        assert!(idle_buffers.is_empty());
    }

    #[test]
    fn dirty_doesnt_exist() {
        let mut dirty = BinaryHeap::new();
        dirty.push(Duration::from_secs(30));

        let mut elements = HashMap::with_hasher(XXHasher::default());
        elements.insert(
            Duration::from_secs(10),
            tiny_vec![
                [Vec<(&str, Duration, isize)>; 16] => vec![
                    ("foo", Duration::from_secs(10), 10),
                    ("foo", Duration::from_secs(10), -10),
                ],
            ],
        );

        let mut idle_buffers = Vec::new();

        compact_delayed_buffers(&mut dirty, &mut elements, &mut idle_buffers);

        assert!(dirty.is_empty());
        assert_eq!(elements.len(), 1);
        assert_eq!(
            elements[&Duration::from_secs(10)],
            tiny_vec![
                [Vec<(&str, Duration, isize)>; 16] => vec![
                    ("foo", Duration::from_secs(10), 10),
                    ("foo", Duration::from_secs(10), -10),
                ],
            ],
        );
        assert!(idle_buffers.is_empty());
    }

    #[test]
    fn single_buffer_in_element_consolidates() {
        let mut dirty = BinaryHeap::new();
        dirty.push(Duration::from_secs(10));

        let mut elements = HashMap::with_hasher(XXHasher::default());
        elements.insert(
            Duration::from_secs(10),
            tiny_vec![
                vec![("foobar", Duration::from_secs(11), -2)],
                vec![
                    ("foo", Duration::from_secs(10), 10),
                    ("foo", Duration::from_secs(10), 10),
                ],
                vec![("foobar", Duration::from_secs(11), 5)]
            ],
        );

        let mut idle_buffers = Vec::new();

        compact_delayed_buffers(&mut dirty, &mut elements, &mut idle_buffers);

        assert!(dirty.is_empty());
        assert_eq!(elements.len(), 1);
        assert_eq!(
            elements[&Duration::from_secs(10)],
            tiny_vec![
                [_; 16] => vec![
                    ("foo", Duration::from_secs(10), 20),
                    ("foobar", Duration::from_secs(11), 3),
                ],
            ],
        );
    }
}

#[allow(dead_code)]
mod simd {
    #[cfg(target_arch = "x86")]
    use core::arch::x86::{
        __m256i, _mm256_add_epi32, _mm256_add_epi64, _mm256_and_si256, _mm256_andnot_si256,
        _mm256_castsi256_si128, _mm256_cmpgt_epi32, _mm256_cvtepu32_epi64,
        _mm256_extracti128_si256, _mm256_min_epi32, _mm256_or_si256, _mm256_set1_epi32,
        _mm256_set1_epi64x,
    };
    #[cfg(target_arch = "x86_64")]
    use core::arch::x86_64::{
        __m256i, _mm256_add_epi32, _mm256_add_epi64, _mm256_and_si256, _mm256_andnot_si256,
        _mm256_castsi256_si128, _mm256_cmpgt_epi32, _mm256_cvtepu32_epi64,
        _mm256_extracti128_si256, _mm256_min_epi32, _mm256_or_si256, _mm256_set1_epi32,
        _mm256_set1_epi64x,
    };

    #[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
    #[allow(non_camel_case_types)]
    type u64x4 = __m256i;

    #[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
    #[allow(non_camel_case_types)]
    type u32x8 = __m256i;

    #[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
    const NANOS_PER_SEC: u32 = 1_000_000_000;

    #[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
    #[target_feature(enable = "avx2")]
    pub unsafe fn duration_saturating_add_x86_64_avx2(
        lhs_seconds1: u64x4,
        lhs_seconds2: u64x4,
        lhs_nanos: u32x8,
        rhs_seconds1: u64x4,
        rhs_seconds2: u64x4,
        rhs_nanos: u32x8,
    ) -> (u64x4, u64x4, u32x8) {
        let one = _mm256_set1_epi64x(1);

        // Add the left and right hand nanoseconds
        let nanos = _mm256_add_epi32(lhs_nanos, rhs_nanos);
        // Clamp the nanosecond values to be a maximum of `NANOS_PER_SEC`
        let final_nanos = _mm256_min_epi32(nanos, _mm256_set1_epi32(NANOS_PER_SEC as i32));

        // Since x86 doesn't have `>=` for some ungodly reason we have to
        // use a manual implementation of that uses `x > (MAX - 1)`
        let max_nanos = _mm256_set1_epi32((NANOS_PER_SEC - 1) as i32);
        let greater_eq_max_nanos = _mm256_cmpgt_epi32(nanos, max_nanos);

        // Split the higher and lower halves of the comparison lanes
        // and extend them from 32 to 64 bits so that we can blend
        // them with our 64 bit values in `seconds1` & `seconds2`
        let high_greater = _mm256_cvtepu32_epi64(_mm256_extracti128_si256(greater_eq_max_nanos, 1));
        let low_greater = _mm256_cvtepu32_epi64(_mm256_castsi256_si128(greater_eq_max_nanos));

        // We add 1 to the seconds count unconditionally and then blend the two based
        // on the comparison mask
        let seconds1 = _mm256_add_epi64(lhs_seconds1, one);
        let seconds2 = _mm256_add_epi64(lhs_seconds2, one);

        // Select between `lhs_seconds1`/`lhs_seconds2` and `seconds1`/`seconds2`
        // based off of `greater_eq_max_nanos` (`high_greater` & `low_greater`)
        let selected_seconds1 = _mm256_or_si256(
            _mm256_and_si256(high_greater, seconds1),
            _mm256_andnot_si256(high_greater, lhs_seconds1),
        );
        let selected_seconds2 = _mm256_or_si256(
            _mm256_and_si256(low_greater, seconds2),
            _mm256_andnot_si256(low_greater, lhs_seconds2),
        );

        // Add the selected seconds (left hand seconds + 1 if there
        // was a nanosecond overflow) and the right hand seconds
        let final_seconds1 = _mm256_add_epi64(selected_seconds1, rhs_seconds1);
        let final_seconds2 = _mm256_add_epi64(selected_seconds2, rhs_seconds2);

        (final_seconds1, final_seconds2, final_nanos)
    }

    #[cfg(test)]
    mod tests {
        #[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
        use crate::dataflow::operators::delay::simd::{
            duration_saturating_add_x86_64_avx2, u32x8, u64x4,
        };
        #[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
        use std::{intrinsics::transmute, time::Duration};

        #[cfg(target_arch = "x86")]
        use core::arch::x86::{_mm256_set_epi32, _mm256_set_epi64x};
        #[cfg(target_arch = "x86_64")]
        use core::arch::x86_64::{_mm256_set_epi32, _mm256_set_epi64x};

        #[test]
        #[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
        fn x86_64_avx2_add() {
            if is_x86_feature_detected!("avx2") {
                let lhs_durations = vec![
                    Duration::new(0, 5),
                    Duration::new(5, 10),
                    Duration::new(10, 15),
                    Duration::new(50, 55),
                    Duration::new(100, 0),
                    Duration::new(200, 45243),
                    Duration::new(100, 0),
                    Duration::new(2843, 45645644),
                ];
                let rhs_durations = vec![
                    Duration::new(0, 0),
                    Duration::new(5, 5),
                    Duration::new(20, 20),
                    Duration::new(15, 55),
                    Duration::new(1, 0),
                    Duration::new(20, 45),
                    Duration::new(10870, 90),
                    Duration::new(23, 9432195),
                ];

                unsafe {
                    let lhs_seconds1 = _mm256_set_epi64x(
                        lhs_durations[0].as_secs() as _,
                        lhs_durations[1].as_secs() as _,
                        lhs_durations[2].as_secs() as _,
                        lhs_durations[3].as_secs() as _,
                    );
                    let lhs_seconds2 = _mm256_set_epi64x(
                        lhs_durations[4].as_secs() as _,
                        lhs_durations[5].as_secs() as _,
                        lhs_durations[6].as_secs() as _,
                        lhs_durations[7].as_secs() as _,
                    );
                    let lhs_nanos = _mm256_set_epi32(
                        lhs_durations[0].subsec_nanos() as _,
                        lhs_durations[1].subsec_nanos() as _,
                        lhs_durations[2].subsec_nanos() as _,
                        lhs_durations[3].subsec_nanos() as _,
                        lhs_durations[4].subsec_nanos() as _,
                        lhs_durations[5].subsec_nanos() as _,
                        lhs_durations[6].subsec_nanos() as _,
                        lhs_durations[7].subsec_nanos() as _,
                    );

                    let rhs_seconds1 = _mm256_set_epi64x(
                        rhs_durations[0].as_secs() as _,
                        rhs_durations[1].as_secs() as _,
                        rhs_durations[2].as_secs() as _,
                        rhs_durations[3].as_secs() as _,
                    );
                    let rhs_seconds2 = _mm256_set_epi64x(
                        rhs_durations[4].as_secs() as _,
                        rhs_durations[5].as_secs() as _,
                        rhs_durations[6].as_secs() as _,
                        rhs_durations[7].as_secs() as _,
                    );
                    let rhs_nanos = _mm256_set_epi32(
                        rhs_durations[0].subsec_nanos() as _,
                        rhs_durations[1].subsec_nanos() as _,
                        rhs_durations[2].subsec_nanos() as _,
                        rhs_durations[3].subsec_nanos() as _,
                        rhs_durations[4].subsec_nanos() as _,
                        rhs_durations[5].subsec_nanos() as _,
                        rhs_durations[6].subsec_nanos() as _,
                        rhs_durations[7].subsec_nanos() as _,
                    );

                    let (seconds1, seconds2, nanos) = duration_saturating_add_x86_64_avx2(
                        lhs_seconds1,
                        lhs_seconds2,
                        lhs_nanos,
                        rhs_seconds1,
                        rhs_seconds2,
                        rhs_nanos,
                    );
                    let (seconds1, seconds2, nanos) = (
                        transmute::<u64x4, [u64; 4]>(seconds1),
                        transmute::<u64x4, [u64; 4]>(seconds2),
                        transmute::<u32x8, [u32; 8]>(nanos),
                    );

                    let output_seconds = vec![
                        Duration::new(seconds1[3], nanos[7]),
                        Duration::new(seconds1[2], nanos[6]),
                        Duration::new(seconds1[1], nanos[5]),
                        Duration::new(seconds1[0], nanos[4]),
                        Duration::new(seconds2[3], nanos[3]),
                        Duration::new(seconds2[2], nanos[2]),
                        Duration::new(seconds2[1], nanos[1]),
                        Duration::new(seconds2[0], nanos[0]),
                    ];
                    let expected_seconds: Vec<_> = lhs_durations
                        .into_iter()
                        .zip(rhs_durations)
                        .map(|(lhs, rhs)| lhs.checked_add(rhs).unwrap_or(Duration::MAX))
                        .collect();

                    assert_eq!(output_seconds, expected_seconds);
                }
            } else {
                println!("avx2 isn't enabled, skipping test");
            }
        }
    }
}
