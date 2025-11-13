# Redis Stream Bus

`redis_stream_bus` is a Rust library that wraps the Redis Streams API to make it simple to publish, read, and acknowledge events when working with consumer groups.
It builds on top of the [`redis`](https://crates.io/crates/redis) crate, exposes an async-friendly API, and provides helpers for converting between Redis values and strongly typed data structures.

## Examples

See `examples/` for basic usage examples.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request. Include tests for new functionality and ensure existing checks continue to pass.