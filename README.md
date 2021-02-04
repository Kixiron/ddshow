## Debugging tips:

If the output is empty when it shouldn't be, make sure you aren't overwriting the default
loggers set by Timely and DDflow by using `Worker::log_register()` with a `Logger`
implementation that doesn't forward logging events

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
