#![allow(clippy::clippy::unused_unit)]

use crate::dataflow::operators::EventIterator;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use ddshow_types::Event;
use rkyv::{
    archived_root,
    de::deserializers::AllocDeserializer,
    ser::{serializers::AlignedSerializer, Serializer},
    AlignedVec, Archive, Deserialize, Serialize,
};
use std::{
    io::{self, Read, Write},
    marker::PhantomData,
    mem,
};
use timely::dataflow::operators::capture::event::{
    Event as TimelyEvent, EventPusher as TimelyEventPusher,
};

/// A wrapper for `W: Write` implementing `EventPusher<T, D>`.
pub struct RkyvEventWriter<T, D, W> {
    stream: W,
    buffer: AlignedVec,
    __type: PhantomData<(T, D)>,
}

impl<T, D, W> RkyvEventWriter<T, D, W> {
    /// Allocates a new `EventWriter` wrapping a supplied writer.
    pub fn new(stream: W) -> Self {
        RkyvEventWriter {
            stream,
            buffer: AlignedVec::with_capacity(512),
            __type: PhantomData,
        }
    }
}

impl<T, D, W> TimelyEventPusher<T, D> for RkyvEventWriter<T, D, W>
where
    W: Write,
    T: for<'a> Serialize<AlignedSerializer<&'a mut AlignedVec>>,
    D: for<'a> Serialize<AlignedSerializer<&'a mut AlignedVec>>,
{
    fn push(&mut self, event: TimelyEvent<T, D>) {
        let event: Event<T, D> = event.into();

        let archive_len = {
            self.buffer.clear();

            let mut serializer = AlignedSerializer::new(&mut self.buffer);
            serializer
                .serialize_value(&event)
                .unwrap_or_else(|unreachable| match unreachable {});

            serializer.pos()
        };

        if let Err(err) = self.stream.write_u64::<LittleEndian>(archive_len as u64) {
            tracing::error!(
                archive_len = archive_len,
                "failed to write archive length to stream: {:?}",
                err,
            );

            return;
        }

        if let Err(err) = self.stream.write_all(&self.buffer[..archive_len]) {
            tracing::error!("failed to write buffer data to stream: {:?}", err);
            return;
        }
    }
}

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
    T: Archive,
    T::Archived: Deserialize<T, AllocDeserializer>, // + CheckBytes<DefaultArchiveValidator>,
    D: Archive,
    D::Archived: Deserialize<D, AllocDeserializer>, // + CheckBytes<DefaultArchiveValidator>,
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

        let message_header = (&self.buffer1[self.consumed..])
            .read_u64::<LittleEndian>()
            .map(|archive_len| archive_len as usize);

        if let Ok(archive_length) = message_header {
            let archive_start = self.consumed + mem::size_of::<u64>();

            if let Some(slice) = self
                .buffer1
                .get(archive_start..archive_start + archive_length)
            {
                // Safety: This isn't safe whatsoever, get `CheckBytes` implemented for `Duration`
                let event = unsafe { archived_root::<Event<T, D>>(slice) }
                    .deserialize(&mut AllocDeserializer)
                    .unwrap_or_else(|unreachable| match unreachable {});

                self.consumed += archive_length + mem::size_of::<u64>();
                return Ok(Some(event.into()));

                // match check_archived_root::<Event<T, D>>(slice) {
                //     Ok(archive) => {
                //         let event = archive
                //             .deserialize(&mut AllocDeserializer)
                //             .unwrap_or_else(|unreachable| match unreachable {});
                //
                //         self.consumed += archive_length + mem::size_of::<u64>();
                //         return Ok(Some(event.into()));
                //     }
                //
                //     Err(err) => tracing::error!("failed to check archived event: {:?}", err),
                // }
            }
        }

        // if we exhaust data we should shift back (if any shifting to do)
        if self.consumed > 0 {
            self.buffer2.clear();
            self.buffer2
                .extend_from_slice(&self.buffer1[self.consumed..]);

            mem::swap(&mut self.buffer1, &mut self.buffer2);
            self.consumed = 0;
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
    T: Archive,
    T::Archived: Deserialize<T, AllocDeserializer>, // + CheckBytes<DefaultArchiveValidator>,
    D: Archive,
    D::Archived: Deserialize<D, AllocDeserializer>, // + CheckBytes<DefaultArchiveValidator>,
{
    type Item = TimelyEvent<T, D>;

    fn next(&mut self) -> Option<Self::Item> {
        EventIterator::next(self, &mut false).ok().flatten()
    }
}

#[cfg(test)]
mod tests {
    use crate::dataflow::{
        operators::{RkyvEventReader, RkyvEventWriter},
        tests::init_test_logging,
    };
    use ddshow_types::{timely_logging::OperatesEvent, OperatorAddr, OperatorId};
    use std::time::Duration;
    use timely::dataflow::operators::capture::{Event, EventPusher};

    // FIXME: Make this a proptest
    #[test]
    fn roundtrip() {
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
                vec![OperatesEvent::new(
                    OperatorId::new(0),
                    OperatorAddr::from_elem(OperatorId::new(0)),
                    "foobar".to_owned(),
                )],
            ),
        ];

        let mut buffer = Vec::new();

        {
            let mut writer = RkyvEventWriter::new(&mut buffer);
            writer.push(events[0].clone());
            writer.push(events[1].clone());
        }

        let mut reader = RkyvEventReader::new(&buffer[..]);

        assert!(reader.next().is_none());
        let first = reader.next().unwrap();
        let second = reader.next().unwrap();

        assert_eq!(events, vec![first, second]);
    }
}
