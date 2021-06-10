use bytecheck::CheckBytes;
use ddshow_types::Event;
use rkyv::{
    ser::{serializers::AlignedSerializer, Serializer},
    validation::DefaultArchiveValidator,
    AlignedVec, Serialize,
};
use std::{fmt::Debug, io::Write, marker::PhantomData, mem};
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
    T: for<'a> Serialize<AlignedSerializer<&'a mut AlignedVec>> + Debug,
    T::Archived: CheckBytes<DefaultArchiveValidator>,
    D: for<'a> Serialize<AlignedSerializer<&'a mut AlignedVec>> + Debug,
    D::Archived: CheckBytes<DefaultArchiveValidator>,
{
    fn push(&mut self, event: TimelyEvent<T, D>) {
        let event: Event<T, D> = event.into();

        // Align to 16
        const PADDING: [u8; 15] = [0; 15];
        match self.position & 15 {
            0 => (),
            x => {
                let padding = 16 - x;

                if let Err(err) = self.stream.write_all(&PADDING[..padding]) {
                    #[cfg(feature = "tracing")]
                    tracing_dep::error!(
                        padding_len = padding,
                        "failed to write padding to stream: {:?}",
                        err,
                    );

                    return;
                }

                self.position += padding;
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

        if let Err(err) = result {
            #[cfg(feature = "tracing")]
            tracing_dep::error!(
                archive_len = %archive_len,
                "failed to write buffer data to stream: {:?}",
                err,
            );

            return;
        }

        self.position += mem::size_of::<u128>() + archive_len as usize;
    }
}
