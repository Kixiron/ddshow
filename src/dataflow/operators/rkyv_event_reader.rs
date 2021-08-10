use crate::dataflow::operators::EventIterator;
use bytecheck::CheckBytes;
use ddshow_types::Event;
use rkyv::{
    check_archived_root, de::deserializers::SharedDeserializeMap,
    validation::validators::DefaultValidator, AlignedVec, Archive, Deserialize,
};
use std::{
    convert::TryInto,
    fmt::{self, Debug},
    io::{self, Read},
    marker::PhantomData,
    mem,
};
use timely::dataflow::operators::capture::event::Event as TimelyEvent;

/// A Wrapper for `R: Read` implementing `EventIterator<T, D>`.
pub struct RkyvEventReader<T, D, R> {
    reader: R,
    bytes: Vec<u8>,
    buffer1: AlignedVec,
    buffer2: AlignedVec,
    consumed: usize,
    peer_finished: bool,
    retried: u16,
    shared: SharedDeserializeMap,
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
            retried: 0,
            shared: SharedDeserializeMap::new(),
            __type: PhantomData,
        }
    }
}

impl<T, D, R> EventIterator<T, D> for RkyvEventReader<T, D, R>
where
    R: Read,
    T: Archive,
    T::Archived: Deserialize<T, SharedDeserializeMap> + for<'a> CheckBytes<DefaultValidator<'a>>,
    D: Archive,
    D::Archived: Deserialize<D, SharedDeserializeMap> + for<'a> CheckBytes<DefaultValidator<'a>>,
{
    fn next(
        &mut self,
        is_finished: &mut bool,
        bytes_read: &mut usize,
    ) -> io::Result<Option<TimelyEvent<T, D>>> {
        // Align to read
        let alignment_offset = match self.consumed & 15 {
            0 => 0,
            x => 16 - x,
        };
        let consumed = self.consumed + alignment_offset;

        if self.peer_finished
            && (self.retried > 1000
                || self
                    .buffer1
                    .get(consumed..)
                    // We should always be able to access up to `self.consumed`, right?
                    .map_or(true, |buf| buf.is_empty()))
        {
            // Perform some cleanup so that there's less work to be
            // done when everything drops
            if !*is_finished {
                self.bytes = Vec::new();
                self.buffer1 = AlignedVec::new();
                self.buffer2 = AlignedVec::new();
                self.consumed = 0;
            }

            *is_finished = true;
            return Ok(None);

        // FIXME: This could potentially cause some data to be lost
        } else if self.peer_finished {
            self.retried += 1;
        }

        if let Some(header_slice) = self
            .buffer1
            .get(consumed..consumed + mem::size_of::<u128>())
        {
            let archive_length = header_slice
                .try_into()
                .expect("the slice is the length of a u128");
            let archive_length = u128::from_le_bytes(archive_length) as usize;
            let archive_start = consumed + mem::size_of::<u128>();

            if let Some(slice) = self
                .buffer1
                .get(archive_start..archive_start + archive_length)
            {
                match check_archived_root::<Event<T, D>>(slice) {
                    Ok(archive) => match archive.deserialize(&mut self.shared) {
                        Ok(event) => {
                            let event: Event<T, D> = event;
                            let event = TimelyEvent::from(event);

                            self.consumed +=
                                alignment_offset + archive_length + mem::size_of::<u128>();

                            return Ok(Some(event));
                        }

                        Err(err) => {
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
                                "failed to deserialize archived event: {:?}",
                                err,
                            );

                            panic!("failed to check archived event: {:?}", err);
                        }
                    },

                    Err(err) => {
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
            self.buffer2
                .extend_from_slice(&self.buffer1[self.consumed & !15..]);

            mem::swap(&mut self.buffer1, &mut self.buffer2);
            self.consumed &= 15;
            self.buffer2.clear();
        }

        if let Ok(len) = self.reader.read(&mut self.bytes[..]) {
            *bytes_read += len;
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
    T: Archive,
    T::Archived: Deserialize<T, SharedDeserializeMap> + for<'a> CheckBytes<DefaultValidator<'a>>,
    D: Archive,
    D::Archived: Deserialize<D, SharedDeserializeMap> + for<'a> CheckBytes<DefaultValidator<'a>>,
{
    type Item = TimelyEvent<T, D>;

    fn next(&mut self) -> Option<Self::Item> {
        EventIterator::next(self, &mut false, &mut 0).ok().flatten()
    }
}

impl<T, D, R> Debug for RkyvEventReader<T, D, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RkyvEventReader")
            .field("buffer1", &self.buffer1)
            .field("consumed", &self.consumed)
            .field("peer_finished", &self.peer_finished)
            .finish()
    }
}

// FIXME: https://github.com/djkoloski/rkyv/issues/174
unsafe impl<T, D, R> Send for RkyvEventReader<T, D, R>
where
    T: Send,
    D: Send,
    R: Send,
{
}

#[cfg(test)]
mod tests {
    use crate::{args::TerminalColor, dataflow::operators::RkyvEventReader};
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
        crate::logging::init_logging(TerminalColor::Never);

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
        crate::logging::init_logging(TerminalColor::Never);

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
