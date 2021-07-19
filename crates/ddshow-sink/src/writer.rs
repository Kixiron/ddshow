use bytecheck::CheckBytes;
use ddshow_types::Event;
use rkyv::{
    ser::{
        serializers::{
            AlignedSerializer, AllocScratch, CompositeSerializer, FallbackScratch, HeapScratch,
        },
        Serializer,
    },
    validation::validators::DefaultValidator,
    AlignedVec, Infallible, Serialize,
};
use std::{
    fmt::{self, Debug},
    io::Write,
    marker::PhantomData,
    mem,
};
use timely::dataflow::operators::capture::event::{
    Event as TimelyEvent, EventPusher as TimelyEventPusher,
};

pub type EventSerializer<'a> = CompositeSerializer<
    AlignedSerializer<&'a mut AlignedVec>,
    FallbackScratch<HeapScratch<2048>, AllocScratch>,
    Infallible,
>;

/// A wrapper for a writer that serializes [`rkyv`] encoded types that are FFI compatible
pub struct EventWriter<T, D, W> {
    stream: W,
    buffer: AlignedVec,
    position: usize,
    scratch: FallbackScratch<HeapScratch<2048>, AllocScratch>,
    __type: PhantomData<(T, D)>,
}

impl<T, D, W> EventWriter<T, D, W> {
    /// Allocates a new `EventWriter` wrapping a supplied writer.
    pub fn new(stream: W) -> Self {
        EventWriter {
            stream,
            buffer: AlignedVec::with_capacity(512),
            position: 0,
            scratch: FallbackScratch::default(),
            __type: PhantomData,
        }
    }
}

impl<T, D, W> TimelyEventPusher<T, D> for EventWriter<T, D, W>
where
    W: Write,
    T: for<'a> Serialize<EventSerializer<'a>> + Debug,
    T::Archived: for<'a> CheckBytes<DefaultValidator<'a>>,
    D: for<'a> Serialize<EventSerializer<'a>> + Debug,
    D::Archived: for<'a> CheckBytes<DefaultValidator<'a>>,
{
    fn push(&mut self, event: TimelyEvent<T, D>) {
        let event: Event<T, D> = event.into();

        // Align to 16
        const PADDING: [u8; 15] = [0; 15];
        match self.position & 15 {
            0 => {}
            x => {
                let padding = 16 - x;

                if let Err(err) = self.stream.write_all(&PADDING[..padding]) {
                    #[cfg(feature = "tracing")]
                    tracing_dep::error!(
                        padding_len = padding,
                        "failed to write padding to stream: {:?}",
                        err,
                    );

                    #[cfg(not(feature = "tracing"))]
                    let _err = err;

                    return;
                }

                self.position += padding;
            }
        }

        // Write archive
        self.buffer.clear();

        let mut serializer = CompositeSerializer::new(
            AlignedSerializer::new(&mut self.buffer),
            // FIXME: Get implementations to allow using `&mut`s within `CompositeSerializer`
            mem::take(&mut self.scratch),
            Infallible,
        );

        if let Err(err) = serializer.serialize_value(&event) {
            #[cfg(feature = "tracing")]
            tracing_dep::error!("failed to serialize event: {:?}", err);

            #[cfg(not(feature = "tracing"))]
            let _err = err;

            return;
        }

        let archive_len = serializer.pos() as u128;
        let (_, scratch, _) = serializer.into_components();
        debug_assert_eq!(self.buffer.len(), archive_len as usize);
        self.scratch = scratch;

        let result = self
            .stream
            // This will keep 16-byte alignment because archive_len is a u128
            .write_all(&archive_len.to_le_bytes())
            .and_then(|()| {
                self.position += mem::size_of::<u128>();
                self.stream.write_all(&self.buffer)
            })
            .map(|()| self.position += archive_len as usize);

        if let Err(err) = result {
            #[cfg(feature = "tracing")]
            tracing_dep::error!(
                archive_len = %archive_len,
                "failed to write buffer data to stream: {:?}",
                err,
            );

            #[cfg(not(feature = "tracing"))]
            let _err = err;
        }
    }
}

// FIXME: https://github.com/djkoloski/rkyv/issues/173
impl<T, D, W> Debug for EventWriter<T, D, W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventWriter")
            .field("stream", &(&self.stream as *const W))
            .field("buffer", &self.buffer)
            .field("position", &self.position)
            .field("fallback", &(&self.scratch as *const _))
            .finish()
    }
}
