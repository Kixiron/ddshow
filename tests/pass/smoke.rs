use differential_dataflow::{
    input::Input,
    operators::{arrange::ArrangeBySelf, Consolidate, Iterate, Threshold},
    AsCollection,
};
use timely::{
    communication::allocator::Generic,
    dataflow::{operators::Exchange, Scope},
    worker::Worker,
};

type Time = usize;
type Diff = isize;

pub fn smoke(worker: &mut Worker<Generic>) {
    // create a new input, exchange data, and inspect its output
    let (mut input, mut probe, mut trace) = worker.dataflow::<Time, _, _>(|scope| {
        let (input, stream) = scope.new_collection::<usize, Diff>();
        let stream = stream
            .inner
            .exchange(|&(data, _, _)| data as u64)
            .as_collection();

        let (_, stream2) = scope.new_collection::<usize, Diff>();
        let stream2 = stream2
            .inner
            .exchange(|&(data, _, _)| data as u64)
            .as_collection();

        let output_stream = scope.region_named("a middle region", |scope| {
            let stream = stream.enter_region(scope);
            stream2.enter_region(scope);

            let out_of_scope = scope.region_named("An inner region", |region| {
                let (_, stream3) = region.new_collection::<usize, Diff>();
                stream3.leave();

                stream
                    .enter_region(region)
                    .filter(|&x| x != 4)
                    .consolidate()
                    .leave()
            });

            out_of_scope
                .arrange_by_self()
                .as_collection(|&x, &()| x)
                .leave()
        });

        (
            input,
            output_stream.consolidate().probe(),
            output_stream.arrange_by_self().trace,
        )
    });

    worker.dataflow_named("Arrangement Importer", |scope| {
        let arranged = trace.import(scope);

        arranged
            .flat_map_ref(|&x, &()| if x % 2 == 0 { Some(x) } else { None })
            .inner
            .exchange(|&(data, time, _)| (data * time) as u64)
            .as_collection()
            .iterate(|stream| {
                stream
                    .map(|x| x.saturating_sub(1))
                    .concat(&stream)
                    .distinct()
            })
            .consolidate()
            .probe_with(&mut probe);
    });

    for i in 1..100 {
        if worker.index() == 0 {
            for elem in 0..1000 {
                input.insert(elem);
            }
        }

        input.advance_to(i);
    }

    input.advance_to(101);
    input.flush();
    worker.step_or_park_while(None, || probe.less_than(input.time()));
}
