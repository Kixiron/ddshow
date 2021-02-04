use std::env;
use timely::dataflow::{
    operators::{Enter, Exchange, Filter, Input, Inspect, Probe},
    InputHandle, ProbeHandle, Scope,
};

fn main() {
    timely::execute_from_args(env::args(), |worker| {
        timely_viz_hook::set_log_hooks(worker, &Default::default()).unwrap();

        let index = worker.index();
        let mut input = InputHandle::new();
        let mut probe = ProbeHandle::new();

        // create a new input, exchange data, and inspect its output
        worker.dataflow(|scope| {
            let stream = scope
                .input_from(&mut input)
                .exchange(|&x| x)
                .inspect(move |&x| println!("worker {}:\thello {}", index, x))
                .probe_with(&mut probe);

            scope.region(|region| {
                stream
                    .enter(region)
                    .filter(|&x| x != 4)
                    .probe_with(&mut probe);
            })
        });

        // introduce data and watch!
        for round in 0..10 {
            if index == 0 {
                input.send(round);
            }

            input.advance_to(round + 1);
            worker.step_while(|| probe.less_than(input.time()));
        }

        timely_viz_hook::remove_log_hooks(worker);
    })
    .unwrap();
}
