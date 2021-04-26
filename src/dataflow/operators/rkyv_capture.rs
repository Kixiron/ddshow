use bytecheck::CheckBytes;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use rkyv::{
    check_archived_value,
    de::deserializers::AllocDeserializer,
    ser::{serializers::AlignedSerializer, Serializer},
    validation::DefaultArchiveValidator,
    AlignedVec, Archive, Deserialize, Serialize,
};
use std::{
    convert::TryFrom,
    io::{Read, Write},
    marker::PhantomData,
    mem,
};
use timely::dataflow::operators::capture::event::{Event, EventPusher};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Archive, Serialize, Deserialize)]
#[archive(strict, derive(CheckBytes))]
pub enum RkyvEvent<T, D> {
    /// Progress received via `push_external_progress`.
    Progress(Vec<(T, i64)>),
    /// Messages received via the data stream.
    Messages(T, Vec<D>),
}

impl<T, D> From<Event<T, D>> for RkyvEvent<T, D> {
    fn from(event: Event<T, D>) -> Self {
        match event {
            Event::Progress(progress) => Self::Progress(progress),
            Event::Messages(time, messages) => Self::Messages(time, messages),
        }
    }
}

impl<T, D> From<RkyvEvent<T, D>> for Event<T, D> {
    fn from(val: RkyvEvent<T, D>) -> Self {
        match val {
            RkyvEvent::Progress(progress) => Self::Progress(progress),
            RkyvEvent::Messages(time, messages) => Self::Messages(time, messages),
        }
    }
}

/// A wrapper for `W: Write` implementing `EventPusher<T, D>`.
pub struct EventWriter<T, D, W> {
    stream: W,
    buffer: AlignedVec,
    __type: PhantomData<(T, D)>,
}

impl<T, D, W> EventWriter<T, D, W> {
    /// Allocates a new `EventWriter` wrapping a supplied writer.
    pub fn new(stream: W) -> Self {
        EventWriter {
            stream,
            buffer: AlignedVec::with_capacity(512),
            __type: PhantomData,
        }
    }
}

impl<T, D, W> EventPusher<T, D> for EventWriter<T, D, W>
where
    W: Write,
    T: for<'a> Serialize<AlignedSerializer<&'a mut AlignedVec>>,
    D: for<'a> Serialize<AlignedSerializer<&'a mut AlignedVec>>,
{
    fn push(&mut self, event: Event<T, D>) {
        let event: RkyvEvent<T, D> = event.into();

        let (archive_end, event_position) = {
            self.buffer.clear();
            let mut serializer = AlignedSerializer::new(&mut self.buffer);

            let event_position = serializer
                .serialize_value(&event)
                .unwrap_or_else(|unreachable| match unreachable {});

            (serializer.pos(), event_position)
        };

        debug_assert!(u32::try_from(archive_end).is_ok());
        self.stream
            .write_u32::<LittleEndian>(archive_end as u32)
            .unwrap();

        debug_assert!(u32::try_from(event_position).is_ok());
        self.stream
            .write_u32::<LittleEndian>(event_position as u32)
            .unwrap();

        self.stream.write_all(&self.buffer[..archive_end]).unwrap();
    }
}

/// A Wrapper for `R: Read` implementing `EventIterator<T, D>`.
pub struct EventReader<T, D, R> {
    reader: R,
    bytes: Vec<u8>,
    buffer1: Vec<u8>,
    buffer2: Vec<u8>,
    consumed: usize,
    __type: PhantomData<(T, D)>,
}

impl<T, D, R> EventReader<T, D, R> {
    /// Allocates a new `EventReader` wrapping a supplied reader.
    pub fn new(reader: R) -> Self {
        EventReader {
            reader,
            bytes: vec![0u8; 1 << 20],
            buffer1: Vec::new(),
            buffer2: Vec::new(),
            consumed: 0,
            __type: PhantomData,
        }
    }
}

impl<T, D, R> Iterator for EventReader<T, D, R>
where
    R: Read,
    T: Archive,
    T::Archived: CheckBytes<DefaultArchiveValidator> + Deserialize<T, AllocDeserializer>,
    D: Archive,
    D::Archived: CheckBytes<DefaultArchiveValidator> + Deserialize<D, AllocDeserializer>,
{
    type Item = Event<T, D>;

    fn next(&mut self) -> Option<Self::Item> {
        let message_header = (&self.buffer1[self.consumed..])
            .read_u32::<LittleEndian>()
            .and_then(|archive_len| {
                (&self.buffer1[self.consumed + mem::size_of::<u32>()..])
                    .read_u32::<LittleEndian>()
                    .map(|event_position| (archive_len as usize, event_position as usize))
            });

        if let Ok((archive_length, event_position)) = message_header {
            let archive_start = self.consumed + mem::size_of::<u32>() * 2;

            match check_archived_value::<RkyvEvent<T, D>>(
                &self.buffer1[archive_start..archive_start + archive_length],
                event_position,
            ) {
                Ok(archive) => {
                    let event = archive
                        .deserialize(&mut AllocDeserializer)
                        .unwrap_or_else(|unreachable| match unreachable {});

                    self.consumed += archive_length + mem::size_of::<u32>() * 2;
                    return Some(event.into());
                }

                Err(err) => tracing::warn!("failed to check archived event: {:?}", err),
            }
        }

        // if we exhaust data we should shift back (if any shifting to do)
        if self.consumed > 0 {
            self.buffer2.clear();
            self.buffer2.extend(&self.buffer1[self.consumed..]);

            mem::swap(&mut self.buffer1, &mut self.buffer2);
            self.consumed = 0;
        }

        if let Ok(len) = self.reader.read(&mut self.bytes[..]) {
            self.buffer1.extend(&self.bytes[..len]);
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use crate::dataflow::{
        operators::{EventReader, EventWriter},
        tests::init_test_logging,
        types::{OperatesEvent, OperatorAddr},
    };
    use timely::dataflow::operators::capture::{Event, EventPusher};

    // TODO: Make this a proptest
    #[test]
    fn roundtrip() {
        init_test_logging();

        let events = vec![
            Event::Progress(vec![(0, 1), (1, 2), (2, 3), (3, 4), (4, 5)]),
            Event::Messages(
                0,
                vec![OperatesEvent::new(
                    0,
                    OperatorAddr::from_elem(0),
                    "foobar".to_owned(),
                )],
            ),
        ];

        let mut buffer = Vec::new();

        {
            let mut writer = EventWriter::new(&mut buffer);
            writer.push(events[0].clone());
            writer.push(events[1].clone());
        }
        println!("{:?}", buffer);

        let mut reader = EventReader::new(&buffer[..]);

        assert!(reader.next().is_none());
        let first = reader.next().unwrap();
        let second = reader.next().unwrap();

        assert_eq!(events, vec![first, second]);
    }
}
