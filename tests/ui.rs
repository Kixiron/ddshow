mod fail;
mod pass;

use anyhow::Result;
use libtest_mimic::{run_tests, Arguments, Outcome, Test};
use std::{iter, net::TcpStream, ops::Deref, process::Command, thread};
use timely::{
    communication::allocator::Generic, worker::Worker, CommunicationConfig, WorkerConfig,
};

#[allow(clippy::type_complexity)]
const PASS: &[(&str, fn(&mut Worker<Generic>))] = &[("smoke", pass::smoke)];

#[allow(clippy::type_complexity)]
const FAIL: &[(&str, fn(&mut Worker<Generic>))] = &[];

fn main() -> Result<()> {
    let args = Arguments::from_args();
    let tests = collect_tests(&args)?;

    if !Command::new("cargo")
        .args(&["build", "--bin", "ddshow"])
        .spawn()?
        .wait()?
        .success()
    {
        anyhow::bail!("failed to build ddshow");
    }

    // TODO: Build ddshow
    // TODO: Don't build & run test files, just run the target on another thread
    //       and connect via tcp
    run_tests(&args, tests, test_runtime).exit()
}

fn test_runtime(test: &Test<TestData>) -> Outcome {
    if test.is_ignored {
        return Outcome::Ignored;
    }

    let target = test.data.target;
    let handle = thread::spawn(move || {
        let (builders, others) = CommunicationConfig::Thread.try_build().unwrap();
        let worker_config = WorkerConfig::default();

        timely::execute::execute_from(builders, others, worker_config, move |worker| {
            let stream = TcpStream::connect("127.0.0.1:51317").unwrap();
            ddshow_sink::enable_timely_logging(worker, stream);

            let stream = TcpStream::connect("127.0.0.1:51318").unwrap();
            ddshow_sink::enable_differential_logging(worker, stream);

            target(worker)
        })
    });

    // TODO: Run ddshow
    let mut ddshow = Command::new(concat!(env!("CARGO_MANIFEST_DIR"), "/target/debug/ddshow"));
    ddshow.args(&test.data.ddshow_args).arg("--differential");
    let ddshow = ddshow.spawn().unwrap();

    handle.join().unwrap().unwrap();
    let output = ddshow.wait_with_output().unwrap();

    if output.status.success() {
        Outcome::Passed
    } else {
        Outcome::Failed {
            msg: Some(format!(
                "failed to run `ddshow{}`\n{}\n{}",
                iter::once("")
                    .chain(test.data.ddshow_args.iter().map(Deref::deref))
                    .collect::<Vec<&str>>()
                    .join(" "),
                String::from_utf8(output.stdout).unwrap(),
                String::from_utf8(output.stderr).unwrap(),
            )),
        }
    }
}

struct TestData {
    target: fn(&mut Worker<Generic>),
    ddshow_args: Vec<String>,
}

fn collect_tests(args: &Arguments) -> Result<Vec<Test<TestData>>> {
    let mut tests = Vec::new();

    for &(name, target) in PASS {
        let is_ignored = args.skip.iter().any(|skip| name.contains(skip));

        tests.push(Test {
            name: name.to_owned(),
            kind: String::from("pass"),
            is_ignored,
            is_bench: false,
            data: TestData {
                target,
                ddshow_args: Vec::new(),
            },
        });
    }

    for &(name, target) in FAIL {
        let is_ignored = args.skip.iter().any(|skip| name.contains(skip));

        tests.push(Test {
            name: name.to_owned(),
            kind: String::from("fail"),
            is_ignored,
            is_bench: false,
            data: TestData {
                target,
                ddshow_args: Vec::new(),
            },
        });
    }

    Ok(tests)
}
