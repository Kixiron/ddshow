use differential_dataflow::{input::Input, operators::arrange::ArrangeBySelf};
use std::env;
use timely::dataflow::{ProbeHandle, Scope};

type Time = usize;
type Diff = isize;

fn main() {
    timely::execute_from_args(env::args(), |worker| {
        let index = worker.index();
        let mut probe = ProbeHandle::new();

        // create a new input, exchange data, and inspect its output
        let (mut input, mut trace) = worker.dataflow::<Time, _, _>(|scope| {
            let (input, stream) = scope.new_collection::<usize, Diff>();

            let stream =
                stream.inspect(move |(x, _time, _diff)| println!("worker {}:\thello {}", index, x));

            let scoped = scope.region_named("a middle region", |scope| {
                let stream = stream.enter_region(scope);

                scope
                    .region_named("An inner region", |region| {
                        stream.enter_region(region).filter(|&x| x != 4).leave()
                    })
                    .leave()
            });

            (input, scoped.arrange_by_self().trace)
        });

        worker.dataflow_named("Arrangement Importer", |scope| {
            let arranged = trace.import_named(scope, "A named import");

            arranged
                .flat_map_ref(|&x, &()| if x % 2 == 0 { Some(x) } else { None })
                .probe_with(&mut probe);
        });

        if index == 0 {
            for elem in 0..100 {
                input.insert(elem);
            }
        }

        input.advance_to(1);
        input.flush();
        while probe.less_than(input.time()) {
            worker.step_or_park(None);
        }
    })
    .unwrap();
}
