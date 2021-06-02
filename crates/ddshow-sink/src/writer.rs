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
    position: usize,
    __type: PhantomData<(T, D)>,
}

impl<T, D, W> EventWriter<T, D, W> {
    /// Allocates a new `EventWriter` wrapping a supplied writer.
    pub fn new(stream: W) -> Self {
        EventWriter {
            stream,
            buffer: AlignedVec::with_capacity(512),
            position: 0,
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
    fn push(&mut self, event: TimelyEvent<T, D>) {
        let event: Event<T, D> = event.into();

        // Align to 16
        const PADDING: [u8; 15] = [0; 15];
        match self.position & 15 {
            0 => (),
            x => {
                let padding = 16 - x;
                self.position += padding;

                if let Err(err) = self.stream.write_all(&PADDING[..padding]) {
                    #[cfg(feature = "tracing")]
                    _tracing::error!(
                        padding_len = padding,
                        "failed to write padding to stream: {:?}",
                        err,
                    );

                    return;
                }
            }
        }

        // Write archive
        self.buffer.clear();

        let mut serializer = AlignedSerializer::new(&mut self.buffer);
        serializer
            .serialize_value(&event)
            .unwrap_or_else(|unreachable| match unreachable {});

        let archive_len = serializer.pos() as u128;

        let result = self
            .stream
            // This will keep 16-byte alignment because archive_len is a u128
            .write_all(&archive_len.to_le_bytes())
            .and_then(|_| self.stream.write_all(&self.buffer));

        self.position += 16 + self.buffer.len();

        if let Err(err) = result {
            #[cfg(feature = "tracing")]
            _tracing::error!(
                archive_len = %archive_len,
                "failed to write buffer data to stream: {:?}",
                err,
            );

            return;
        }
    }
}
