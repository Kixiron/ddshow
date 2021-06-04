use differential_dataflow::{
    input::Input,
    operators::{arrange::ArrangeBySelf, Consolidate, Iterate, Threshold},
    AsCollection,
};
use std::{env, fs, net::TcpStream};
use timely::{
    communication::Allocate,
    dataflow::{operators::Exchange, Scope},
    worker::Worker,
};
use tracing_subscriber::{
    fmt::time::Uptime, prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt,
    EnvFilter,
};

type Time = usize;
type Diff = isize;

fn main() {
    let filter_layer = EnvFilter::from_env("DDSHOW_LOG");
    let fmt_layer = tracing_subscriber::fmt::layer()
        .pretty()
        .with_timer(Uptime::default())
        .with_thread_names(true)
        .with_ansi(false)
        .with_level(false);

    let _ = tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt_layer)
        .try_init();

    if let Ok(dir) = env::var("TIMELY_DISK_LOG") {
        let _ = fs::remove_dir_all(dir);
    }
    if let Ok(dir) = env::var("DIFFERENTIAL_LOG") {
        let _ = fs::remove_dir_all(dir);
    }

    timely::execute_from_args(env::args(), |worker| {
        set_loggers(worker);

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
                for elem in 0..100_000 {
                    input.insert(elem);
                }
            }

            input.advance_to(i);
        }

        input.flush();
        worker.step_or_park_while(None, || probe.less_than(input.time()));
    })
    .unwrap();
}

fn set_loggers<A: Allocate>(worker: &mut Worker<A>) {
    if let Ok(dir) = env::var("TIMELY_DISK_LOG") {
        if !dir.is_empty() {
            ddshow_sink::save_timely_logs_to_disk(worker, &dir).unwrap();
        }
    }

    if let Ok(dir) = env::var("DIFFERENTIAL_DISK_LOG") {
        if !dir.is_empty() {
            ddshow_sink::save_differential_logs_to_disk(worker, &dir).unwrap();
        }
    }

    if let Ok(dir) = env::var("TIMELY_PROGRESS_DISK_LOG") {
        if !dir.is_empty() {
            ddshow_sink::save_timely_progress_to_disk(worker, &dir).unwrap();
        }
    }

    if let Ok(addr) = env::var("DIFFERENTIAL_LOG_ADDR") {
        if !addr.is_empty() {
            if let Ok(stream) = TcpStream::connect(&addr) {
                differential_dataflow::logging::enable(worker, stream);
            } else {
                panic!("Could not connect to differential log address: {:?}", addr);
            }
        }
    }
}
