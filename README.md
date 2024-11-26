# Pravega Rust Benchmark

Simple benchmark to measure the IO performance of Pravega systems using the Pravega Rust client.

# Compile

Compilation is made using the cargo build system and package manager.

```
cargo build
```

# Run

To run the benchmark is necessary a set the configuration file (YAML format) as a command line parameter, following section describe the parameters required in the configuration file.

```
./target/debug/pravega-rust-benchmark config.yaml
```

# Configuration

| Parameter              | Description                              | Optional | Default |
| ---------------------- | ---------------------------------------- | -------- | ------- |
| name                   | Benchmark test name.                     | No       |         |
| address                | Pravega IP address and Port.             | No       |         |
| payload_file           | Path of the payload file (sending data). | No       |         |
| message_num            | Number of messages to send.              | No       |         |
| producer_rate          | Messages per second.                     | No       |         |
| scope                  | Pravega Scope.                           | Yes      | "scope" + timestamp |
| stream                 | Pravega Stream.                          | Yes      | "stream" + timestamp |
| retention_time         | Pravega retention time.                  | Yes      | 10 |
| scale_target_rate      | Pravega scale target rate.               | Yes      | 1 |
| scale_factor           | Pravega scale factor.                    | Yes      | 0 |
| scale_min_num_segments | Pravea scale minimum number of segments. | Yes      | 1 |

# Result Output

Benchmark results are stored in an output JSON file that contains the following data and metrics:

Data
- **name**: name of the test set in the configuration file.
- **message_num**: Number of sent messages.
- **message_size**: Size in bits of the payload file.
- **scope**: Generated or set in the configuration file scope.
- **stream**: Generated or set in the configuration file stream.
- **duration**: Total duration of the writing messages.

Metrics
- **write_latency_50pct**: Write latency at 50%.
- **write_latency_75pct**: Write latency at 75%.
- **write_latency_95pct**: Write latency at 95%.
- **write_latency_99pct**: Write latency at 99%.
- **write_latencies**: Vector with all write latencies.
- **read_latencies**: Vector with all read latencies.
- **throughput**: Throughput = Total Output Data / Total Time in Miliseconds.

# To Do

- Get historgram of latencies instead of all latencies.
- Stress test.
- Validate current metrics.
- Evaluate possible metrics to add.
