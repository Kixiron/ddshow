# DDShow

Visualization for Timely Dataflow and Differential Dataflow programs

# Getting started with `ddshow`

First, install `ddshow` via [`cargo`](https://github.com/rust-lang/cargo/). As of now ddshow is not published
to [crates.io](https://crates.io/), but it will be at a future date. Until then, the recommended way to
install ddshow is by using the `--git` option with [`cargo install`](https://doc.rust-lang.org/cargo/commands/cargo-install.html#install-options)

```sh
cargo install --git https://github.com/Kixiron/ddshow
```

Next you need to set the `TIMELY_WORKER_LOG_ADDR` environmental variable for your target program. This should be
set to the same address that ddshow is pointed (`127.0.0.1:51317` by default) to so that they can communicate over TCP.

```sh
# Bash
set TIMELY_WORKER_LOG_ADDR=127.0.0.1:51317
```

```ps
# Powershell
$env:TIMELY_WORKER_LOG_ADDR = "127.0.0.1:51317"
```

```cmd
:: CMD
set TIMELY_WORKER_LOG_ADDR=127.0.0.1:51317
```

After setting the environmental variable you can now run ddshow. The `--connections` argument
should be set to the number of timely workers that the target computation has spun up, defaulting
to `1` if it's not given and the `--address` argument for setting the address ddshow should connect to.
Note that `--address` should be the same as whatever you set the `TIMELY_WORKER_LOG_ADDR` variable to,
otherwise ddshow won't be able to connect.

```sh
ddshow --connections 1 --address 127.0.0.1:51317
```

This will create the `dataflow-graph/` directory which contains everything that ddshow's UI needs
to operate offline. Opening `dataflow-graph/graph.html` in a browser will allow viewing the graphed dataflow

The full list of arguments ddshow supports and their options can be retrieved by running

```sh
ddshow --help
```

For basic usage 

## Showcase

![](https://raw.githubusercontent.com/Kixiron/ddshow/master/assets/ddshow-large.png)

![](https://raw.githubusercontent.com/Kixiron/ddshow/master/assets/ddshow.png)

![](https://raw.githubusercontent.com/Kixiron/ddshow/master/assets/ddshow-tooltip.png)

![](https://raw.githubusercontent.com/Kixiron/ddshow/master/assets/citations.png)

## Debugging tips:

If the output is empty when it shouldn't be, make sure you aren't overwriting the default
loggers set by Timely and DDflow by using `Worker::log_register()` with a `Logger`
implementation that doesn't forward logging events.

Another common problem is a mismatch of timely versions. Because timely/ddflow don't have
up-to-date publishes on crates.io and use [`abomonation`](https://docs.rs/abomonation/0.7.3/abomonation/)
for sending events, their structure isn't consistent across timely versions which can cause
errors and incompatibilities. The only known solution for this is to make sure ddshow and
the target program use the same versions of timely and ddflow, but I'm working on a more stable
solution.

When looking for Differential Dataflow insights, make sure you have this (or an equivalent)
snippet somewhere within your code in order to forward Differential Dataflow logs

```rust
if let Ok(addr) = std::env::var("DIFFERENTIAL_LOG_ADDR") {
    if let Ok(stream) = std::net::TcpStream::connect(&addr) {
        differential_dataflow::logging::enable(worker, stream);
    } else {
        panic!("Could not connect to differential log address: {:?}", addr);
    }
}
```
