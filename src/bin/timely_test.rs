use differential_dataflow::{
    input::Input,
    operators::{arrange::ArrangeBySelf, Consolidate, Iterate, Threshold},
    AsCollection,
};
use std::{
    any::Any,
    fmt::{self, Display},
    net::{SocketAddr, TcpStream},
    num::NonZeroUsize,
    str::FromStr,
};
use structopt::StructOpt;
use timely::{
    communication::allocator::{Generic, GenericBuilder},
    dataflow::{operators::Exchange, Scope},
    worker::Worker,
    CommunicationConfig, WorkerConfig,
};
use tracing_subscriber::{
    fmt::time::Uptime, prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt,
    EnvFilter,
};

type Time = usize;
type Diff = isize;

fn main() {
    let args = TestArgs::from_args();

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

    let (builders, others, worker_config) = args.timely_config();
    timely::execute::execute_from(builders, others, worker_config, move |worker| {
        println!(
            "started up worker {}/{}",
            worker.index() + 1,
            worker.peers(),
        );
        args.set_hooks(worker);

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
                output_stream.probe(),
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

        for i in 1..=args.iterations.get() {
            if worker.index() == 0 {
                for elem in 0..args.records.get() {
                    input.insert(elem);
                }
            }

            input.advance_to(i);
            input.flush();

            if worker.index() == 0 {
                println!("ingested epoch {}/{}", i, args.iterations.get());
            }
        }

        println!(
            "worker {}/{} is processing",
            worker.index() + 1,
            worker.peers(),
        );
        worker.step_or_park_while(None, || probe.less_than(input.time()));
        println!("worker {}/{} finished", worker.index() + 1, worker.peers());
    })
    .unwrap();
}

#[derive(Debug, Clone, StructOpt)]
#[structopt(rename_all = "kebab-case")]
struct TestArgs {
    #[structopt(long, short = "w")]
    workers: NonZeroUsize,

    #[structopt(long)]
    differential: bool,

    #[structopt(long, default_value = "abomonation")]
    stream_encoding: StreamEncoding,

    #[structopt(long = "address", default_value = "127.0.0.1:51317")]
    timely_address: SocketAddr,

    #[structopt(long, default_value = "127.0.0.1:51318")]
    differential_address: SocketAddr,

    #[structopt(long, default_value = "100")]
    iterations: NonZeroUsize,

    #[structopt(long, default_value = "10000")]
    records: NonZeroUsize,
}

impl TestArgs {
    pub fn timely_config(&self) -> (Vec<GenericBuilder>, Box<dyn Any + Send>, WorkerConfig) {
        let (builders, others) = match self.workers.get() {
            0 | 1 => CommunicationConfig::Thread,
            workers => CommunicationConfig::Process(workers),
        }
        .try_build()
        .unwrap();

        let worker_conf = WorkerConfig::default();

        (builders, others, worker_conf)
    }

    pub fn set_hooks(&self, worker: &mut Worker<Generic>) {
        match self.stream_encoding {
            StreamEncoding::Abomonation => {
                use differential_dataflow::logging::DifferentialEvent;
                use timely::{
                    dataflow::operators::capture::EventWriter,
                    logging::{BatchLogger, TimelyEvent},
                };

                if let Ok(stream) = TcpStream::connect(&self.timely_address) {
                    println!(
                        "connected to timely abomonated host at {}",
                        self.timely_address,
                    );

                    let writer = EventWriter::new(stream);
                    let mut logger = BatchLogger::new(writer);

                    worker
                        .log_register()
                        .insert::<TimelyEvent, _>("timely", move |time, data| {
                            logger.publish_batch(time, data)
                        });
                } else {
                    panic!(
                        "Could not connect timely abomonated logging stream to: {:?}",
                        self.timely_address,
                    );
                }

                if self.differential {
                    if let Ok(stream) = TcpStream::connect(&self.differential_address) {
                        println!(
                            "connected to differential abomonated host at {}",
                            self.differential_address,
                        );

                        let writer = EventWriter::new(stream);
                        let mut logger = BatchLogger::new(writer);

                        worker.log_register().insert::<DifferentialEvent, _>(
                            "differential/arrange",
                            move |time, data| logger.publish_batch(time, data),
                        );
                    } else {
                        panic!(
                            "Could not connect differential abomonated logging stream to: {:?}",
                            self.differential_address,
                        );
                    }
                }
            }

            StreamEncoding::Rkyv => {
                if let Ok(stream) = TcpStream::connect(&self.timely_address) {
                    println!("connected to timely rkyv host at {}", self.timely_address);
                    ddshow_sink::enable_timely_logging(worker, stream);
                } else {
                    panic!(
                        "Could not connect timely rkyv logging stream to: {:?}",
                        self.timely_address,
                    );
                }

                if self.differential {
                    if let Ok(stream) = TcpStream::connect(&self.differential_address) {
                        println!(
                            "connected to differential rkyv host at {}",
                            self.differential_address,
                        );
                        ddshow_sink::enable_differential_logging(worker, stream);
                    } else {
                        panic!(
                            "Could not connect differential rkyv logging stream to: {:?}",
                            self.differential_address,
                        );
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StreamEncoding {
    Abomonation,
    Rkyv,
}

impl FromStr for StreamEncoding {
    type Err = String;

    fn from_str(string: &str) -> Result<Self, Self::Err> {
        let lowercase = string.to_lowercase();
        match lowercase.as_str() {
            "abomonation" => Ok(Self::Abomonation),
            "rkyv" => Ok(Self::Rkyv),
            _ => Err(format!(
                "invalid terminal color {:?}, only `rkyv` and `abomonation` are supported",
                string,
            )),
        }
    }
}

impl Display for StreamEncoding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Abomonation => f.write_str("abomonation"),
            Self::Rkyv => f.write_str("rkyv"),
        }
    }
}

impl Default for StreamEncoding {
    fn default() -> Self {
        Self::Abomonation
    }
}
