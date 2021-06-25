use std::{marker::PhantomData, time::Duration};
use timely::dataflow::operators::capture::{Event as RawEvent, EventPusher};

/// Logs events from a timely stream, including progress information
/// and logging messages
#[derive(Debug)]
pub struct BatchLogger<Event, Id, Pusher>
where
    Pusher: EventPusher<Duration, (Duration, Id, Event)>,
{
    // None when the logging stream is closed
    time: Duration,
    event_pusher: Pusher,
    __type: PhantomData<(Id, Event)>,
}

impl<Event, Id, Pusher> BatchLogger<Event, Id, Pusher>
where
    Pusher: EventPusher<Duration, (Duration, Id, Event)>,
{
    /// Creates a new batch logger.
    pub fn new(event_pusher: Pusher) -> Self {
        BatchLogger {
            time: Default::default(),
            event_pusher,
            __type: PhantomData,
        }
    }

    /// Publishes a batch of logged events and advances the capability.
    pub fn publish_batch<Id2, Event2>(
        &mut self,
        &time: &Duration,
        data: &mut Vec<(Duration, Id2, Event2)>,
    ) where
        Id: From<Id2>,
        Event: From<Event2>,
    {
        if !data.is_empty() {
            self.event_pusher.push(RawEvent::Messages(
                self.time,
                data.drain(..)
                    .map(|(time, worker, data)| (time, Id::from(worker), Event::from(data)))
                    .collect(),
            ));
        }

        if self.time < time {
            let new_frontier = time;
            let old_frontier = self.time;

            self.event_pusher.push(RawEvent::Progress(vec![
                (new_frontier, 1),
                (old_frontier, -1),
            ]));
        }

        self.time = time;
    }
}

impl<Event, Id, Pusher> Drop for BatchLogger<Event, Id, Pusher>
where
    Pusher: EventPusher<Duration, (Duration, Id, Event)>,
{
    fn drop(&mut self) {
        self.event_pusher
            .push(RawEvent::Progress(vec![(self.time, -1)]));
    }
}
