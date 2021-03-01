use differential_dataflow::input::Input;
use std::env;
use timely::dataflow::Scope;

type Time = usize;
type Diff = isize;

fn main() {
    timely::execute_from_args(env::args(), |worker| {
        let index = worker.index();

        // create a new input, exchange data, and inspect its output
        let mut input = worker.dataflow::<Time, _, _>(|scope| {
            let (input, stream) = scope.new_collection::<usize, Diff>();
            let (_, stream2) = scope.new_collection::<usize, Diff>();

            scope.region_named("a middle region", |scope| {
                let stream = stream.enter_region(scope);
                let _stream2 = stream2.enter_region(scope);

                scope
                    .region_named("An inner region", |region| {
                        let (_, stream3) = region.new_collection::<usize, Diff>();
                        stream3.leave();

                        stream.enter_region(region).filter(|&x| x != 4).leave()
                    })
                    .leave()
            });

            input
        });

        // worker.dataflow_named("Arrangement Importer", |scope| {
        //     let arranged = trace.import(scope);
        //
        //     arranged
        //         .flat_map_ref(|&x, &()| if x % 2 == 0 { Some(x) } else { None })
        //         .iterate(|stream| {
        //             stream
        //                 .map(|x| x.saturating_sub(1))
        //                 .concat(&stream)
        //                 .distinct()
        //         })
        //         .probe_with(&mut probe);
        // });

        if index == 0 {
            for elem in 0..100 {
                input.insert(elem);
            }
        }

        input.advance_to(1);
        input.flush();
        //while probe.less_than(input.time()) {
        //     worker.step_or_park(None);
        // }
        worker.step();
    })
    .unwrap();
}
