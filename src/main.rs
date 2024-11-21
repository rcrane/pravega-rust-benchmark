mod config;
mod result;
mod channel_data;

use std::env;
use std::thread;
use std::process;
use config::Config;
use std::sync::mpsc;
use result::TestResult;
use chrono::DateTime;
use std::time::Duration;
use chrono::prelude::Utc;
use channel_data::ChannelData;
use pravega_client_shared::Scope;
use pravega_client_shared::Stream;
use pravega_client_shared::Scaling;
use pravega_client_shared::Retention;
use pravega_client_shared::ScaleType;
use pravega_client_shared::ScopedStream;
use pravega_client_shared::RetentionType;
use pravega_client_shared::StreamConfiguration;
use pravega_client_config::ClientConfigBuilder;
use pravega_client::client_factory::ClientFactory;

const START_CONSTANT:  i32 = 95;
const WARMUP_MESSAGES: u32 = 5;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Getting the workload file configuration form command line parameters
    let args: Vec<String> = env::args().collect();
    if args.len() <= 1 {
        println!("Arguments missing.\nUsage: {} <Yaml config file>", args[0]);
        process::exit(1);
    }
    
    // Getting config and payload content
    let conf = Config::load_from_file( &args[1].clone() ).expect("Could not read config file.");

    // Starting Threads
    let (tx1, rx1) = mpsc::channel(); // Start Signal
    let (tx2, rx2) = mpsc::channel(); // Latencies

    let tx3 = tx2.clone();
    let config_cpy = conf.clone();
    let handler_snd = thread::spawn(move || {
        sender_handler(tx1, tx2, config_cpy);
    });

    let config_cpy = conf.clone();
    let handler_rcv = thread::spawn(move || { 
        receiver_handler(rx1, tx3, config_cpy);
    });

    handler_snd.join().unwrap();
    handler_rcv.join().unwrap();

    // get ouput data from threads
    let mut result = TestResult::new(conf);
    for received in rx2 {
        match received {
            ChannelData::WriteLatency(value) => result.write_latencies.push(value),
            ChannelData::ReadLatency(value)  => result.read_latencies.push(value)
        }
    }
    result.calculate_metrics();
    result.to_file().expect("Failed to write results.");
    println!("Benchmark finished");
    Ok(())
}

fn create_client(address: String) -> ClientFactory {
    let pravega_conf = ClientConfigBuilder::default()
        .controller_uri( address )
        .build()
        .unwrap();
    let client_factory = ClientFactory::new(pravega_conf);
    client_factory
}

fn get_latency(start_time: DateTime<chrono::Utc>, ends_time: DateTime<chrono::Utc>) -> f64 {
    let difference = ends_time - start_time;
    let latency = difference.num_milliseconds() as f64 + (difference.num_microseconds().unwrap() % 1000) as f64 / 1000.0;
    latency
}

fn get_scoped_stream(conf_scope: String, conf_stream: String) -> ScopedStream {
    let scoped_stream = ScopedStream {
        scope:  Scope::from( conf_scope ),
        stream: Stream::from( conf_stream ),
    };
    scoped_stream
}

fn sender_handler(signal: mpsc::Sender<i32>, out: mpsc::Sender<ChannelData>, conf: Config) {
    println!("Connecting to Pravega {}", conf.address);
    let payload = conf.message.to_string().into_bytes();
    let client_factory = create_client(conf.address);
    
    println!("Init Environtment");
    client_factory.runtime().block_on(async {
        let controller_client = client_factory.controller_client();
        // create a scope
        let scope = Scope::from(conf.scope.to_owned());
        controller_client.create_scope(&scope)
            .await
            .expect("create scope");
        println!("\t scope {} created", conf.scope);

        // create a stream containing only one segment
        let stream = Stream::from(conf.stream.to_owned());
        let stream_config = StreamConfiguration {
            scoped_stream: ScopedStream {
                scope: scope.clone(),
                stream: stream.clone(),
            },
            scaling: Scaling {
                scale_type: ScaleType::FixedNumSegments,
                target_rate: 0,
                scale_factor: 0,
                min_num_segments: 1,
            },
            retention: Retention {
                retention_type: RetentionType::None,
                retention_param: 0,
            },
            tags: None,
        };
        controller_client
            .create_stream(&stream_config)
            .await
            .expect("create stream");
        println!("\t stream {} created", conf.stream);

        println!("Starting WarmUp");
        let scoped_stream = get_scoped_stream(conf.scope, conf.stream);
        let mut event_writer = client_factory.create_event_writer(scoped_stream);
        for i in 1..=WARMUP_MESSAGES {
            let result = event_writer.write_event(payload.clone()).await;
            assert!(result.await.is_ok());
            println!("\t + Send Msg {}", i);
        }

        signal.send(START_CONSTANT).unwrap();
        println!("Starting Benchmark");
        for i in 1..=conf.message_num {
            let time1 = Utc::now();
            let result = event_writer.write_event(payload.clone()).await;
            assert!(result.await.is_ok());
            let time2 = Utc::now();
            let latency = get_latency(time1, time2);
            println!("\t + Msg {} Write Latency {} ms", i, latency);
            out.send(ChannelData::WriteLatency(latency)).unwrap();
        }
    });
}

fn receiver_handler(signal: mpsc::Receiver<i32>, out: mpsc::Sender<ChannelData>, conf: Config) {
    let client_factory = create_client(conf.address);
    // Pause before everything is working
    loop {
        if let Ok(msg) = signal.try_recv() {
            if msg == START_CONSTANT {
                break;
            }
        }
        thread::sleep(Duration::from_millis(10));
    }
    // Start Reading Messages
    let mut i = 0;
    let scoped_stream = get_scoped_stream(conf.scope, conf.stream);
    client_factory.runtime().block_on(async {
        let rg = client_factory.create_reader_group("rg".to_string(), scoped_stream).await;
        let mut reader = rg.create_reader("r1".to_string()).await;
        
        if let Some(mut slice) = reader
            .acquire_segment()
            .await
            .expect("Failed to acquire segment since the reader is offline")
        {
            loop {
                let time1 = Utc::now();
                let read_event = slice.next();
                let time2 = Utc::now();
                if read_event.is_some() {
                    i += 1;
                    let latency = get_latency(time1, time2);
                    let event_len = read_event.unwrap().value.as_slice().len();
                    assert_eq!(event_len, conf.message_size);
                    println!("\t - Msg {} Len {} Read Latency {} ms", i, event_len, latency);
                    if i > WARMUP_MESSAGES {
                        out.send(ChannelData::ReadLatency(latency)).unwrap();
                    }
                } else {
                    reader.release_segment(slice).await.unwrap();
                    if let Some(new_slice) = reader
                        .acquire_segment()
                        .await
                        .expect("Failed to acquire segment since the reader is offline")
                    {
                        slice = new_slice;
                    } else {
                        println!("\t - No more data to read");
                        break;
                    }
                }
            }
        }
        reader
            .reader_offline()
            .await
            .expect("failed to mark the reader offline");
    });
}
