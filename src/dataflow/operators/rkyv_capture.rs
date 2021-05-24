#![allow(clippy::clippy::unused_unit)]

use crate::dataflow::{operators::EventIterator, ChannelId, OperatorAddr, OperatorId, PortId};
use abomonation_derive::Abomonation;
use bytecheck::CheckBytes;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
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
    time::Duration,
};
use timely::{
    dataflow::operators::capture::event::{Event, EventPusher as TimelyEventPusher},
    logging::{
        ApplicationEvent, ChannelsEvent, CommChannelKind, CommChannelsEvent, GuardedMessageEvent,
        GuardedProgressEvent, InputEvent, MessagesEvent, OperatesEvent, ParkEvent,
        PushProgressEvent, ScheduleEvent, ShutdownEvent, StartStop, TimelyEvent,
    },
};

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
    fn push(&mut self, event: Event<T, D>) {
        let event: RkyvEvent<T, D> = event.into();

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
    fn next(&mut self, is_finished: &mut bool) -> io::Result<Option<Event<T, D>>> {
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
                let event = unsafe { archived_root::<RkyvEvent<T, D>>(slice) }
                    .deserialize(&mut AllocDeserializer)
                    .unwrap_or_else(|unreachable| match unreachable {});

                self.consumed += archive_length + mem::size_of::<u64>();
                return Ok(Some(event.into()));

                // match check_archived_root::<RkyvEvent<T, D>>(slice) {
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
    type Item = Event<T, D>;

    fn next(&mut self) -> Option<Self::Item> {
        EventIterator::next(self, &mut false).ok().flatten()
    }
}

unsafe impl<T, D, R> Send for RkyvEventReader<T, D, R>
where
    T: Send,
    D: Send,
    R: Send,
{
}

#[cfg(test)]
mod tests {
    use crate::dataflow::{
        operators::{RkyvEventReader, RkyvEventWriter},
        tests::init_test_logging,
        types::{OperatesEvent, OperatorAddr},
        OperatorId,
    };
    use std::time::Duration;
    use timely::dataflow::operators::capture::{Event, EventPusher};

    // TODO: Make this a proptest
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
                    OperatorAddr::from_elem(0),
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

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Ord,
    PartialOrd,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    Serialize,
    Deserialize,
    Archive,
    Abomonation,
)]
#[archive(strict, derive(CheckBytes))]
pub enum RkyvTimelyEvent {
    Operates(RkyvOperatesEvent),
    Channels(RkyvChannelsEvent),
    PushProgress(RkyvPushProgressEvent),
    Messages(RkyvMessagesEvent),
    Schedule(RkyvScheduleEvent),
    Shutdown(RkyvShutdownEvent),
    Application(RkyvApplicationEvent),
    GuardedMessage(RkyvGuardedMessageEvent),
    GuardedProgress(RkyvGuardedProgressEvent),
    CommChannels(RkyvCommChannelsEvent),
    Input(RkyvInputEvent),
    Park(RkyvParkEvent),
    Text(String),
}

impl From<TimelyEvent> for RkyvTimelyEvent {
    fn from(event: TimelyEvent) -> Self {
        match event {
            TimelyEvent::Operates(operates) => Self::Operates(operates.into()),
            TimelyEvent::Channels(channels) => Self::Channels(channels.into()),
            TimelyEvent::PushProgress(progress) => Self::PushProgress(progress.into()),
            TimelyEvent::Messages(inner) => Self::Messages(inner.into()),
            TimelyEvent::Schedule(inner) => Self::Schedule(inner.into()),
            TimelyEvent::Shutdown(inner) => Self::Shutdown(inner.into()),
            TimelyEvent::Application(inner) => Self::Application(inner.into()),
            TimelyEvent::GuardedMessage(inner) => Self::GuardedMessage(inner.into()),
            TimelyEvent::GuardedProgress(inner) => Self::GuardedProgress(inner.into()),
            TimelyEvent::CommChannels(inner) => Self::CommChannels(inner.into()),
            TimelyEvent::Input(inner) => Self::Input(inner.into()),
            TimelyEvent::Park(inner) => Self::Park(inner.into()),
            TimelyEvent::Text(text) => Self::Text(text),
        }
    }
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Ord,
    PartialOrd,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    Serialize,
    Deserialize,
    Archive,
    Abomonation,
)]
#[archive(strict, derive(CheckBytes))]
pub struct RkyvOperatesEvent {
    pub id: OperatorId,
    pub addr: OperatorAddr,
    pub name: String,
}

impl From<OperatesEvent> for RkyvOperatesEvent {
    fn from(event: OperatesEvent) -> Self {
        Self {
            id: OperatorId::new(event.id),
            addr: OperatorAddr::from(event.addr),
            name: event.name,
        }
    }
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Ord,
    PartialOrd,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    Serialize,
    Deserialize,
    Archive,
    Abomonation,
)]
#[archive(strict, derive(CheckBytes))]
pub struct RkyvChannelsEvent {
    pub id: OperatorId,
    pub scope_addr: OperatorAddr,
    pub source: (PortId, PortId),
    pub target: (PortId, PortId),
}

impl From<ChannelsEvent> for RkyvChannelsEvent {
    fn from(event: ChannelsEvent) -> Self {
        Self {
            id: OperatorId::new(event.id),
            scope_addr: OperatorAddr::from(event.scope_addr),
            source: (PortId::new(event.source.0), PortId::new(event.source.1)),
            target: (PortId::new(event.target.0), PortId::new(event.target.1)),
        }
    }
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Ord,
    PartialOrd,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    Serialize,
    Deserialize,
    Archive,
    Abomonation,
)]
#[archive(strict, derive(CheckBytes))]
pub struct RkyvPushProgressEvent {
    pub op_id: OperatorId,
}

impl From<PushProgressEvent> for RkyvPushProgressEvent {
    fn from(event: PushProgressEvent) -> Self {
        Self {
            op_id: OperatorId::new(event.op_id),
        }
    }
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Ord,
    PartialOrd,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    Serialize,
    Deserialize,
    Archive,
    Abomonation,
)]
#[archive(strict, derive(CheckBytes))]
pub struct RkyvMessagesEvent {
    /// `true` if send event, `false` if receive event.
    pub is_send: bool,
    /// Channel identifier
    pub channel: ChannelId,
    /// Source worker index.
    pub source: OperatorId,
    /// Target worker index.
    pub target: OperatorId,
    /// Message sequence number.
    pub seq_no: usize,
    /// Number of typed records in the message.
    pub length: usize,
}

impl From<MessagesEvent> for RkyvMessagesEvent {
    fn from(event: MessagesEvent) -> Self {
        Self {
            is_send: event.is_send,
            channel: ChannelId::new(event.channel),
            source: OperatorId::new(event.source),
            target: OperatorId::new(event.target),
            seq_no: event.seq_no,
            length: event.length,
        }
    }
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Ord,
    PartialOrd,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    Serialize,
    Deserialize,
    Archive,
    Abomonation,
)]
#[archive(strict, derive(CheckBytes))]
pub enum RkyvStartStop {
    /// Operator starts.
    Start,
    /// Operator stops.
    Stop,
}

impl From<StartStop> for RkyvStartStop {
    fn from(start_stop: StartStop) -> Self {
        match start_stop {
            StartStop::Start => Self::Start,
            StartStop::Stop => Self::Stop,
        }
    }
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Ord,
    PartialOrd,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    Serialize,
    Deserialize,
    Archive,
    Abomonation,
)]
#[archive(strict, derive(CheckBytes))]
pub struct RkyvScheduleEvent {
    pub id: OperatorId,
    pub start_stop: RkyvStartStop,
}

impl From<ScheduleEvent> for RkyvScheduleEvent {
    fn from(event: ScheduleEvent) -> Self {
        Self {
            id: OperatorId::new(event.id),
            start_stop: event.start_stop.into(),
        }
    }
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Ord,
    PartialOrd,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    Serialize,
    Deserialize,
    Archive,
    Abomonation,
)]
#[archive(strict, derive(CheckBytes))]
pub struct RkyvShutdownEvent {
    pub id: OperatorId,
}

impl From<ShutdownEvent> for RkyvShutdownEvent {
    fn from(event: ShutdownEvent) -> Self {
        Self {
            id: OperatorId::new(event.id),
        }
    }
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Ord,
    PartialOrd,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    Serialize,
    Deserialize,
    Archive,
    Abomonation,
)]
#[archive(strict, derive(CheckBytes))]
pub struct RkyvApplicationEvent {
    pub id: usize,
    // TODO: Make this a `RkyvStartStop`?
    pub is_start: bool,
}

impl From<ApplicationEvent> for RkyvApplicationEvent {
    fn from(event: ApplicationEvent) -> Self {
        Self {
            id: event.id,
            is_start: event.is_start,
        }
    }
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Ord,
    PartialOrd,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    Serialize,
    Deserialize,
    Archive,
    Abomonation,
)]
#[archive(strict, derive(CheckBytes))]
pub struct RkyvGuardedMessageEvent {
    // TODO: Make this a `RkyvStartStop`?
    pub is_start: bool,
}

impl From<GuardedMessageEvent> for RkyvGuardedMessageEvent {
    fn from(event: GuardedMessageEvent) -> Self {
        Self {
            is_start: event.is_start,
        }
    }
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Ord,
    PartialOrd,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    Serialize,
    Deserialize,
    Archive,
    Abomonation,
)]
#[archive(strict, derive(CheckBytes))]
pub struct RkyvGuardedProgressEvent {
    // TODO: Make this a `RkyvStartStop`?
    pub is_start: bool,
}

impl From<GuardedProgressEvent> for RkyvGuardedProgressEvent {
    fn from(event: GuardedProgressEvent) -> Self {
        Self {
            is_start: event.is_start,
        }
    }
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Ord,
    PartialOrd,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    Serialize,
    Deserialize,
    Archive,
    Abomonation,
)]
#[archive(strict, derive(CheckBytes))]
pub enum RkyvCommChannelKind {
    Progress,
    Data,
}

impl From<CommChannelKind> for RkyvCommChannelKind {
    fn from(channel_kind: CommChannelKind) -> Self {
        match channel_kind {
            CommChannelKind::Progress => Self::Progress,
            CommChannelKind::Data => Self::Data,
        }
    }
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Ord,
    PartialOrd,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    Serialize,
    Deserialize,
    Archive,
    Abomonation,
)]
#[archive(strict, derive(CheckBytes))]
pub struct RkyvCommChannelsEvent {
    pub identifier: usize,
    pub kind: RkyvCommChannelKind,
}

impl From<CommChannelsEvent> for RkyvCommChannelsEvent {
    fn from(event: CommChannelsEvent) -> Self {
        Self {
            identifier: event.identifier,
            kind: event.kind.into(),
        }
    }
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Ord,
    PartialOrd,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    Serialize,
    Deserialize,
    Archive,
    Abomonation,
)]
#[archive(strict, derive(CheckBytes))]
pub struct RkyvInputEvent {
    pub start_stop: RkyvStartStop,
}

impl From<InputEvent> for RkyvInputEvent {
    fn from(event: InputEvent) -> Self {
        Self {
            start_stop: event.start_stop.into(),
        }
    }
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Ord,
    PartialOrd,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    Serialize,
    Deserialize,
    Archive,
    Abomonation,
)]
#[archive(strict, derive(CheckBytes))]
pub enum RkyvParkEvent {
    Park(Option<Duration>),
    Unpark,
}

impl From<ParkEvent> for RkyvParkEvent {
    fn from(park: ParkEvent) -> Self {
        match park {
            ParkEvent::Park(duration) => Self::Park(duration),
            ParkEvent::Unpark => Self::Unpark,
        }
    }
}
