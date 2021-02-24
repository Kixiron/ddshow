use anyhow::{Context, Result};
use std::{
    io::{self, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    num::NonZeroUsize,
    sync::atomic::{AtomicBool, Ordering},
};
use timely::communication::WorkerGuards;

pub type Connections = Vec<Option<TcpStream>>;

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

            println!("Connected to socket {}/{}", i + 1, connections);
            Ok(Some(socket))
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

    // Write a prompt to the terminal for the user
    write!(
        stdout,
        "Press enter to finish collecting trace data (this will crash the source computation if it's currently running)",
    )
    .context("failed to write termination message to stdout")?;
    stdout.flush().context("failed to flush stdout")?;

    // Wait for input
    stdin
        .read(&mut [0])
        .context("failed to get input from stdin")?;

    // Terminate the replay
    running.store(false, Ordering::Release);

    writeln!(stdout, "Processing data...").context("failed to write to stdout")?;
    stdout.flush().context("failed to flush stdout")?;

    // Join all timely worker threads
    for result in worker_guards.join() {
        result
            .map_err(|err| anyhow::anyhow!("failed to join timely worker threads: {}", err))??;
    }

    Ok(())
}
