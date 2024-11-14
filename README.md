# Pravega Rust Benchmark

Simple benchmark to measure the IO performance of Pravega Rust client.

# Compile

```
cargo build
```

# Run

```
./target/debug/pravega-rust-benchmark config.yaml
```

# Configuration

| Parameter    | Description   | Example | Optional |
| ------------ | ------------- | ------- | -------- |
| name         | Benchmark test name | Test1 | No |
| address      | Prave IP address and Port | "localhost:9090" | No |
| payload_file | File with the payload data | "payload/payload-100b.data" | No |
| message_num  | x | x | No |
| scope        | x | x | Yes |
| stream       | x | x | Yes |

# Metrics

To Do
