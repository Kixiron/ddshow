use differential_dataflow::{input::Input, operators::Iterate, AsCollection};
use std::{env, net::TcpStream};
use timely::dataflow::operators::{Map, Probe};

type Time = usize;

fn main() {
    timely::execute_from_args(env::args(), |worker| {
        if let Ok(addr) = env::var("DIFFERENTIAL_LOG_ADDR") {
            if let Ok(stream) = TcpStream::connect(&addr) {
                differential_dataflow::logging::enable(worker, stream);
            } else {
                panic!("Could not connect to differential log address: {:?}", addr);
            }
        }

        let (mut input, probe) = worker.dataflow::<Time, _, _>(|scope| {
            let (input, collection) = scope.new_collection();

            // If we iterate with (purposefully introduced) weird timestamp fluctuations and
            // no consolidating operator we can introduce an infinite hang in the program
            let output = collection.iterate(|collection| {
                collection
                    .inner
                    // Exacerbate the timestamp fluctuations
                    .flat_map(|(data, time, diff)| {
                        vec![(data, time, diff * 2), (data, time, diff * -2)]
                    })
                    .as_collection()
            });

            (input, output.inner.probe())
        });

        // Purposefully introduce really weird timestamp fluctuations
        input.update(100, 5);
        input.update(10, -5);
        input.advance_to(1);

        input.update(100, -5);
        input.update(10, 5);
        input.advance_to(2);

        worker.step_or_park_while(None, || probe.less_than(input.time()));
    })
    .unwrap();
}
