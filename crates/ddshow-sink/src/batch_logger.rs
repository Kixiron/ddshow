use std::{marker::PhantomData, time::Duration};
use timely::dataflow::operators::capture::{Event as RawEvent, EventPusher};

/// Logs events from a timely stream, including progress information
/// and logging messages
#[derive(Debug)]
pub struct BatchLogger<T, D, P>
where
    P: EventPusher<Duration, (Duration, D, T)>,
{
    // None when the logging stream is closed
    time: Duration,
    event_pusher: P,
    __type: PhantomData<(D, T)>,
}

impl<T, D, P> BatchLogger<T, D, P>
where
    P: EventPusher<Duration, (Duration, D, T)>,
{
    /// Creates a new batch logger.
    pub fn new(event_pusher: P) -> Self {
        BatchLogger {
            time: Default::default(),
            event_pusher,
            __type: PhantomData,
        }
    }

    /// Publishes a batch of logged events and advances the capability.
    pub fn publish_batch<D2, T2>(&mut self, &time: &Duration, data: &mut Vec<(Duration, D2, T2)>)
    where
        D: From<D2>,
        T: From<T2>,
    {
        if !data.is_empty() {
            self.event_pusher.push(RawEvent::Messages(
                self.time,
                data.drain(..)
                    .map(|(time, worker, data)| (time, D::from(worker), T::from(data)))
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

impl<T, E, P> Drop for BatchLogger<T, E, P>
where
    P: EventPusher<Duration, (Duration, E, T)>,
{
    fn drop(&mut self) {
        self.event_pusher
            .push(RawEvent::Progress(vec![(self.time, -1)]));
    }
}
