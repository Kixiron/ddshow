use byteorder::{LittleEndian, WriteBytesExt};
use ddshow_types::Event;
use rkyv::{
    ser::{serializers::AlignedSerializer, Serializer},
    AlignedVec, Serialize,
};
use std::{io::Write, marker::PhantomData};
use timely::dataflow::operators::capture::event::{
    Event as TimelyEvent, EventPusher as TimelyEventPusher,
};

/// A wrapper for a writer that serializes [`rkyv`] encoded types that are FFI compatible
#[derive(Debug)]
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

impl<T, D, W> TimelyEventPusher<T, D> for EventWriter<T, D, W>
where
    W: Write,
    T: for<'a> Serialize<AlignedSerializer<&'a mut AlignedVec>>,
    D: for<'a> Serialize<AlignedSerializer<&'a mut AlignedVec>>,
{
    /// Push an event to the event writer
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
            #[cfg(feature = "tracing")]
            _tracing::error!(
                archive_len = archive_len,
                "failed to write archive length to stream: {:?}",
                err,
            );

            return;
        }

        if let Err(err) = self.stream.write_all(&self.buffer[..archive_len]) {
            #[cfg(feature = "tracing")]
            _tracing::error!("failed to write buffer data to stream: {:?}", err);

            return;
        }
    }
}
