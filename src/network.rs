use anyhow::{Context, Result};
use std::{
    hint,
    io::{self, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    num::NonZeroUsize,
    sync::{
        atomic::{self, AtomicBool, Ordering},
        Arc, Barrier,
    },
    thread,
    time::Duration,
};
use timely::communication::WorkerGuards;

pub type Connections = Vec<TcpStream>;

/// The read timeout to impose on tcp connections
const TCP_READ_TIMEOUT: Option<Duration> = Some(Duration::from_millis(200));

/// Connect to the given address and collect `connections` streams, returning all of them
/// in non-blocking mode
pub fn wait_for_connections(
    timely_addr: SocketAddr,
    connections: NonZeroUsize,
) -> Result<Connections> {
    let timely_listener =
        TcpListener::bind(timely_addr).context("failed to bind to socket address")?;

    let timely_conns = (0..connections.get())
        .zip(timely_listener.incoming())
        .map(|(i, socket)| {
            let socket = socket.context("failed to accept socket connection")?;

            socket
                .set_nonblocking(true)
                .context("failed to set socket to non-blocking mode")?;

            if let Err(err) = socket.set_read_timeout(TCP_READ_TIMEOUT) {
                tracing::error!(
                    "failed to set socket to a read timeout of {:?}: {:?}",
                    TCP_READ_TIMEOUT,
                    err,
                );
            };

            println!("Connected to socket {}/{}", i + 1, connections);
            Ok(socket)
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(timely_conns)
}

/// Wait for user input to terminate the trace replay and wait for all timely
/// workers to terminate
// TODO: Add a "haven't received updates in `n` seconds" thingy to tell the user
//       we're no longer getting data
pub fn wait_for_input(running: &AtomicBool, worker_guards: WorkerGuards<Result<()>>) -> Result<()> {
    let (mut stdin, mut stdout) = (io::stdin(), io::stdout());

    let (send, recv) = crossbeam_channel::bounded(1);
    let barrier = Arc::new(Barrier::new(2));

    let thread_barrier = barrier.clone();
    thread::spawn(move || {
        thread_barrier.wait();

        // Wait for input
        let _ = stdin.read(&mut [0]);

        tracing::info!("stdin thread got input from stdin");
        send.send(()).unwrap();
    });

    // Write a prompt to the terminal for the user
    write!(
        stdout,
        "Press enter to finish collecting trace data (this will crash the source computation if it's currently running)",
    )
    .context("failed to write termination message to stdout")?;
    stdout.flush().context("failed to flush stdout")?;

    barrier.wait();
    let num_threads = worker_guards.guards().len();

    loop {
        hint::spin_loop();

        if !running.load(Ordering::Acquire) || recv.recv_timeout(Duration::from_millis(500)).is_ok()
        {
            tracing::info!(
                num_threads = num_threads,
                running = running.load(Ordering::Acquire),
                "main thread got shutdown",
            );

            break;
        }
    }

    // Terminate the replay
    running.store(false, Ordering::Release);
    atomic::fence(Ordering::Acquire);

    writeln!(stdout, "Processing data...").context("failed to write to stdout")?;
    stdout.flush().context("failed to flush stdout")?;

    for thread in worker_guards.guards() {
        thread.thread().unpark();
    }

    // Join all timely worker threads
    for result in worker_guards.join() {
        result
            .map_err(|err| anyhow::anyhow!("failed to join timely worker threads: {}", err))??;
    }

    Ok(())
}
