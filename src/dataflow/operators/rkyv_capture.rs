#![allow(clippy::clippy::unused_unit)]

use crate::dataflow::operators::EventIterator;
use bytecheck::CheckBytes;
use ddshow_types::Event;
use rkyv::{
    archived_root, check_archived_root, de::deserializers::AllocDeserializer,
    validation::DefaultArchiveValidator, AlignedVec, Archive, Deserialize,
};
use std::{
    convert::TryInto,
    fmt::Debug,
    io::{self, Read},
    marker::PhantomData,
    mem,
};
use timely::dataflow::operators::capture::event::Event as TimelyEvent;

/// A Wrapper for `R: Read` implementing `EventIterator<T, D>`.
#[derive(Debug)]
pub struct RkyvEventReader<T, D, R> {
    reader: R,
    bytes: Vec<u8>,
    buffer1: AlignedVec,
    buffer2: AlignedVec,
    consumed: usize,
    peer_finished: bool,
    __type: PhantomData<(T, D)>,
}

impl<T, D, R> RkyvEventReader<T, D, R> {
    /// Allocates a new `EventReader` wrapping a supplied reader.
    pub fn new(reader: R) -> Self {
        RkyvEventReader {
            reader,
            bytes: vec![0u8; 1 << 20],
            buffer1: AlignedVec::new(),
            buffer2: AlignedVec::new(),
            consumed: 0,
            peer_finished: false,
            __type: PhantomData,
        }
    }
}

impl<T, D, R> EventIterator<T, D> for RkyvEventReader<T, D, R>
where
    R: Read,
    T: Archive + Debug,
    T::Archived: Deserialize<T, AllocDeserializer> + CheckBytes<DefaultArchiveValidator>,
    D: Archive + Debug,
    D::Archived: Deserialize<D, AllocDeserializer> + CheckBytes<DefaultArchiveValidator>,
{
    fn next(&mut self, is_finished: &mut bool) -> io::Result<Option<TimelyEvent<T, D>>> {
        if self.peer_finished
            && self.bytes.is_empty()
            && self.buffer1.is_empty()
            && self.buffer2.is_empty()
        {
            *is_finished = true;
            return Ok(None);
        }

        // Align to read
        let alignment_offset = match self.consumed & 15 {
            0 => 0,
            x => 16 - x,
        };
        let consumed = self.consumed + alignment_offset;

        if let Some(header_slice) = self
            .buffer1
            .get(consumed..consumed + mem::size_of::<u128>())
        {
            let archive_length = u128::from_le_bytes(header_slice.try_into().unwrap()) as usize;

            let archive_start = consumed + mem::size_of::<u128>();

            if let Some(slice) = self
                .buffer1
                .get(archive_start..archive_start + archive_length)
            {
                // let archive = unsafe { archived_root::<Event<T, D>>(slice) };
                // let event = archive
                //     .deserialize(&mut AllocDeserializer)
                //     .unwrap_or_else(|unreachable| match unreachable {});
                //
                // self.consumed += alignment_offset + archive_length + mem::size_of::<u128>();
                // return Ok(Some(event.into()));

                match check_archived_root::<Event<T, D>>(slice) {
                    Ok(archive) => {
                        let event = archive
                            .deserialize(&mut AllocDeserializer)
                            .unwrap_or_else(|unreachable| match unreachable {})
                            .into();

                        self.consumed += alignment_offset + archive_length + mem::size_of::<u128>();

                        return Ok(Some(event));
                    }

                    Err(err) => {
                        let unsafely_archived: TimelyEvent<T, D> =
                            unsafe { archived_root::<Event<T, D>>(slice) }
                                .deserialize(&mut AllocDeserializer)
                                .unwrap_or_else(|unreachable| match unreachable {})
                                .into();

                        tracing::error!(
                            type_name = std::any::type_name::<Event<T, D>>(),
                            consumed = self.consumed,
                            alignment_offset = alignment_offset,
                            archive_length = archive_length,
                            header_size = mem::size_of::<u128>(),
                            new_consumed = self.consumed
                                + alignment_offset
                                + archive_length
                                + mem::size_of::<u128>(),
                            unsafely_archived = ?unsafely_archived,
                            "failed to check archived event: {:?}",
                            err,
                        );

                        panic!("failed to check archived event: {:?}", err);
                    }
                }
            }
        }

        // if we exhaust data we should shift back while preserving our alignment
        // of 16 bytes
        if self.consumed > 15 {
            self.buffer2.clear();
            self.buffer2
                .extend_from_slice(&self.buffer1[self.consumed & !15..]);

            mem::swap(&mut self.buffer1, &mut self.buffer2);
            self.consumed &= 15;
        }

        if let Ok(len) = self.reader.read(&mut self.bytes[..]) {
            if len == 0 {
                self.peer_finished = true;
            }

            self.buffer1.extend_from_slice(&self.bytes[..len]);
        }

        Ok(None)
    }
}

impl<T, D, R> Iterator for RkyvEventReader<T, D, R>
where
    R: Read,
    T: Archive + Debug,
    T::Archived: Deserialize<T, AllocDeserializer> + CheckBytes<DefaultArchiveValidator>,
    D: Archive + Debug,
    D::Archived: Deserialize<D, AllocDeserializer> + CheckBytes<DefaultArchiveValidator>,
{
    type Item = TimelyEvent<T, D>;

    fn next(&mut self) -> Option<Self::Item> {
        EventIterator::next(self, &mut false).ok().flatten()
    }
}

#[cfg(test)]
mod tests {
    use crate::dataflow::{operators::RkyvEventReader, tests::init_test_logging};
    use ddshow_sink::EventWriter;
    use ddshow_types::{
        differential_logging::{DifferentialEvent, MergeEvent},
        timely_logging::{OperatesEvent, TimelyEvent},
        OperatorAddr, OperatorId,
    };
    use std::time::Duration;
    use timely::dataflow::operators::capture::{Event, EventPusher};

    // FIXME: Make this a proptest
    #[test]
    fn timely_roundtrip() {
        init_test_logging();

        let events = vec![
            Event::Progress(vec![
                (Duration::from_secs(0), 1),
                (Duration::from_secs(1), 2),
                (Duration::from_secs(2), 3),
                (Duration::from_secs(3), 4),
                (Duration::from_secs(4), 5),
            ]),
            Event::Messages(
                Duration::from_secs(0),
                vec![TimelyEvent::Operates(OperatesEvent::new(
                    OperatorId::new(0),
                    OperatorAddr::from_elem(OperatorId::new(0)),
                    "foobar".to_owned(),
                ))],
            ),
            Event::Messages(
                Duration::from_secs(40),
                vec![TimelyEvent::Operates(OperatesEvent::new(
                    OperatorId::new(4),
                    OperatorAddr::from(vec![OperatorId::new(0); 100]),
                    "foobarbaz".to_owned(),
                ))],
            ),
        ];

        let mut buffer = Vec::new();

        {
            let mut writer = EventWriter::new(&mut buffer);
            writer.push(events[0].clone());
            writer.push(events[1].clone());
            writer.push(events[2].clone());
        }

        let mut reader = RkyvEventReader::new(&buffer[..]);

        // The first `.next()` call will fill the buffers, `RykvEventReader` doesn't
        // act like a fused iterator
        assert!(reader.next().is_none());

        let first = reader.next().unwrap();
        let second = reader.next().unwrap();
        let third = reader.next().unwrap();

        assert_eq!(events, vec![first, second, third]);
    }

    // FIXME: Make this a proptest
    #[test]
    fn differential_roundtrip() {
        init_test_logging();

        let events = vec![
            Event::Progress(vec![
                (Duration::from_secs(0), 1),
                (Duration::from_secs(1), 2),
                (Duration::from_secs(2), 3),
                (Duration::from_secs(3), 4),
                (Duration::from_secs(4), 5),
            ]),
            Event::Messages(
                Duration::from_secs(3243450),
                vec![
                    DifferentialEvent::Merge(MergeEvent::new(
                        OperatorId::new(2345235),
                        10000,
                        5000,
                        5000,
                        None,
                    )),
                    DifferentialEvent::Merge(MergeEvent::new(
                        OperatorId::new(4235),
                        10000,
                        5000,
                        5000,
                        None,
                    )),
                ],
            ),
            Event::Messages(
                Duration::from_secs(40),
                vec![
                    DifferentialEvent::Merge(MergeEvent::new(
                        OperatorId::new(52345),
                        10000,
                        5000,
                        5000,
                        Some(100000),
                    )),
                    DifferentialEvent::Merge(MergeEvent::new(
                        OperatorId::new(54235),
                        10000,
                        5000,
                        5000,
                        Some(100000),
                    )),
                    DifferentialEvent::Merge(MergeEvent::new(
                        OperatorId::new(765),
                        10000,
                        5000,
                        5000,
                        Some(100000),
                    )),
                    DifferentialEvent::Merge(MergeEvent::new(
                        OperatorId::new(0),
                        10000,
                        5000,
                        5000,
                        Some(100000),
                    )),
                    DifferentialEvent::Merge(MergeEvent::new(
                        OperatorId::new(345643645),
                        10000,
                        5000,
                        5000,
                        Some(100000),
                    )),
                ],
            ),
        ];

        let mut buffer = Vec::new();

        {
            let mut writer = EventWriter::new(&mut buffer);
            writer.push(events[0].clone());
            writer.push(events[1].clone());
            writer.push(events[2].clone());
        }

        let mut reader = RkyvEventReader::new(&buffer[..]);

        // The first `.next()` call will fill the buffers, `RykvEventReader` doesn't
        // act like a fused iterator
        assert!(reader.next().is_none());

        let first = reader.next().unwrap();
        let second = reader.next().unwrap();
        let third = reader.next().unwrap();

        assert_eq!(events, vec![first, second, third]);
    }
}
