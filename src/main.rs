use pravega_client::client_factory::ClientFactory;
use pravega_client_config::ClientConfigBuilder;
use pravega_client_shared::{
    Retention, RetentionType, ScaleType, Scaling, Scope,
    ScopedStream, Stream, StreamConfiguration,
};

use std::env;
use std::thread;
use std::process;
use std::sync::mpsc;
use chrono::DateTime;
use std::time::Duration;
use chrono::prelude::Utc;

mod config;
use config::Config;
/*
mod stats;
use stats::Stats;
*/

const START_CONSTANT:  i32 = 5;
const WARMUP_MESSAGES: i32 = 5;

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
    let (tx, rx) = mpsc::channel();
    let config_cpy = conf.clone();
    let handler_snd = thread::spawn(move || {
        sender_handler(tx, config_cpy);
    });

    let config_cpy = conf.clone();
    let handler_rcv = thread::spawn(move || { 
        receiver_handler(rx, config_cpy);
    });

    handler_snd.join().unwrap();
    handler_rcv.join().unwrap();
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

fn sender_handler(tx: mpsc::Sender<i32>, conf: Config) {
    println!("Connecting to Pravega {}", conf.address);
    let payload = conf.message.to_string().into_bytes();
    let client_factory = create_client(conf.address);
    

    println!("Init Environtment");
    client_factory.runtime().block_on(async {
        let controller_client = client_factory.controller_client();
        // create a scope
        let scope = Scope::from(conf.scope.to_owned());
        controller_client
            .create_scope(&scope)
            .await
            .expect("create scope");
        println!("scope {} created", conf.scope);

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
        println!("stream {} created", conf.stream);
    });
    let scostr = ScopedStream {
        scope:  Scope::from( conf.scope ),
        stream: Stream::from( conf.stream ),
    };
    let mut event_writer = client_factory.create_event_writer(scostr);

    println!("Starting WarmUp");
    client_factory.runtime().block_on(async {
        for i in 1..=WARMUP_MESSAGES {
            let result = event_writer.write_event(payload.clone()).await;
            assert!(result.await.is_ok());
            println!("\t + Send Msg {}", i);
        }
    });
    tx.send(START_CONSTANT).unwrap();

    println!("Starting Benchmark");
    client_factory.runtime().block_on(async {
        for i in 1..=conf.message_num {
            let time1 = Utc::now();
            let result = event_writer.write_event(payload.clone()).await;
            assert!(result.await.is_ok());
            let time2 = Utc::now();
            let latency = get_latency(time1, time2);
            println!("\t + Msg {} Write Latency {} ms", i, latency);
        }
    });
    thread::sleep(Duration::from_secs(1));
}

fn receiver_handler(rx: mpsc::Receiver<i32>, conf: Config) {
    let client_factory = create_client(conf.address);
    // Pause before everything is working
    loop {
        // Check for pause signal
        if let Ok(msg) = rx.try_recv() {
            if msg == START_CONSTANT {
                break;
            }
        }
        //thread::sleep(Duration::from_secs(1));
        thread::sleep(Duration::from_millis(10));
    }

    let scostr = ScopedStream {
        scope:  Scope::from( conf.scope ),
        stream: Stream::from( conf.stream ),
    };
    let mut i = 0;
    client_factory.runtime().block_on(async {
        // create event stream reader
        let rg = client_factory.create_reader_group("rg".to_string(), scostr).await;
        let mut reader = rg.create_reader("r1".to_string()).await;
        
        if let Some(mut segment_slice) = reader.acquire_segment().await.expect("Failed to acquire segment since the reader is offline") {
            thread::sleep(Duration::from_secs(1));
            loop {
                let time1 = Utc::now();
                if let Some(event) = segment_slice.next() {
                    let time2 = Utc::now();
                    let latency = get_latency(time1, time2);
                    i += 1;
                    println!("\t - Msg {} Len {} Read Latency {} ms", i, event.value.len(), latency);
                    thread::sleep(Duration::from_millis(50));
                } else {
                    break;
                }
            }
        }
        reader
            .reader_offline()
            .await
            .expect("failed to mark the reader offline");
    });
}
