FROM ubuntu:latest

ADD ./payload /benchmark
ADD ./target/debug/pravega-rust-benchmark /benchmark
ADD ./config.yaml /benchmark

WORKDIR /benchmark
ENTRYPOINT ["tail", "-f", "/dev/null"]
