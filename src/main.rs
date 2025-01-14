mod config;
mod result;
mod channel_data;

use std::env;
use std::thread;
use std::process;

use config::Config;
use std::sync::Arc;
use std::sync::mpsc;
use async_std::task;
use std::sync::Mutex;
use chrono::DateTime;
use result::TestResult;
use std::time::Duration;
use chrono::prelude::Utc;
use threadpool::ThreadPool;
use channel_data::ChannelData;
use pravega_client_shared::Scope;
use pravega_client_shared::Stream;
use pravega_client_shared::Scaling;
use pravega_client_shared::Retention;
use pravega_client_shared::ScaleType;
use pravega_client::event::EventWriter;
use pravega_client_shared::ScopedStream;
use pravega_client_shared::RetentionType;
use pravega_client_shared::StreamConfiguration;
use pravega_client_config::ClientConfigBuilder;
use pravega_client::client_factory::ClientFactory;

const START_CONSTANT:  i32 = 95;

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
            ChannelData::WriteLatency(value)  => result.write_latencies.push(value),
            ChannelData::ReadLatency(value)   => result.read_latencies.push(value),
            ChannelData::WriteDuration(value) => result.duration = value
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

fn get_difference(start_time: DateTime<chrono::Utc>, ends_time: DateTime<chrono::Utc>) -> f64 {
    let difference = ends_time - start_time;
    let time = difference.num_milliseconds() as f64 + (difference.num_microseconds().unwrap() % 1000) as f64 / 1000.0;
    time
}

fn get_scoped_stream(conf_scope: String, conf_stream: String) -> ScopedStream {
    let scoped_stream = ScopedStream {
        scope:  Scope::from( conf_scope ),
        stream: Stream::from( conf_stream ),
    };
    scoped_stream
}

fn get_stream_config(conf: Config, scope: Scope) -> StreamConfiguration {
    let stream = Stream::from(conf.stream.to_owned());
    let stream_config = StreamConfiguration {
        scoped_stream: ScopedStream {
            scope:  scope.clone(),
            stream: stream.clone(),
        },
        scaling: Scaling {
            scale_type:       ScaleType::ByRateInEventsPerSec,
            target_rate:      conf.scale_target_rate,
            scale_factor:     conf.scale_factor,
            min_num_segments: conf.scale_min_num_segments,
        },
        retention: Retention {
            retention_type:  RetentionType::Time,
            retention_param: conf.retention_time,
        },
        tags: None,
    };
    stream_config
}

async fn write_one_event(arc_event_writer: Arc<Mutex<EventWriter>>, payload: Vec<u8>) -> f64 {
    let mut event_writer = arc_event_writer.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    let start_time = Utc::now();
    let result = event_writer.write_event(payload).await;
    if !result.await.is_ok() {
        return -1.0
    }

    let end_time = Utc::now();
    let latency = get_difference(start_time, end_time);
    latency
}

fn sender_handler(signal: mpsc::Sender<i32>, out: mpsc::Sender<ChannelData>, conf: Config) {
    println!("Connecting to Pravega {}", conf.address);
    let payload = conf.message.to_string().into_bytes();
    let client_factory = create_client(conf.address.clone());
    
    println!("Init Environment");
    client_factory.runtime().block_on(async {
        let controller_client = client_factory.controller_client();
        // create a scope
        let scope = Scope::from(conf.scope.to_owned());
        controller_client.create_scope(&scope)
            .await
            .expect("create scope");
        println!("\t Scope {} created", conf.scope);

        // create a stream containing only one segment
        let stream_config = get_stream_config(conf.clone(), scope.clone());
        controller_client
            .create_stream(&stream_config)
            .await
            .expect("create stream");
        println!("\t Stream {} created", conf.stream);

        println!("Starting WarmUp");
        let scoped_stream = get_scoped_stream(conf.scope, conf.stream);
        let event_writer = client_factory.create_event_writer(scoped_stream);
        let shared_event_writer = Arc::new(Mutex::new(event_writer));

        for i in 1..=conf.message_warmup {
            let payload          = payload.clone();
            let arc_event_writer = Arc::clone(&shared_event_writer);
            write_one_event(arc_event_writer, payload).await;
            if i % conf.producer_rate == 0 {
                thread::sleep(Duration::from_secs(1));
            }
        }

        println!("Starting Benchmark");
        signal.send(START_CONSTANT).unwrap();
        
        /*
         * Create a thread for each message to send, and when the created threads are
         * equal to produce rate wait for a second. This ensure the produce rate per second
         * requirement.
         */
        let pool      = ThreadPool::new(conf.producer_rate as usize);
        let ben_start = Utc::now();
        for i in 1..=conf.message_num {
            let out_cloned       = out.clone();
            let payload_cloned   = payload.clone();
            let arc_event_writer = Arc::clone(&shared_event_writer);
            pool.execute(move || {
                let result = task::block_on( write_one_event(arc_event_writer, payload_cloned) );
                out_cloned.send(ChannelData::WriteLatency(result)).unwrap();
            });
            if i % conf.producer_rate == 0 {
                println!("\t + Messages Sent {}", i);
                thread::sleep(Duration::from_secs(1));
            }
        }
        if conf.message_num % conf.producer_rate != 0 {
            println!("\t + Messages Sent {}", conf.message_num);
            thread::sleep(Duration::from_secs(1));
        }

        // Wait for the threads to finish and calculate the total time for benchmark sending.
        let ben_ends = Utc::now();
        let duration = get_difference(ben_start, ben_ends);
        out.send(ChannelData::WriteDuration(duration)).unwrap();
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
                    let latency = get_difference(time1, time2);
                    let event_len = read_event.unwrap().value.as_slice().len();
                    assert_eq!(event_len, conf.message_size);
                    if i > conf.message_warmup {
                        out.send(ChannelData::ReadLatency(latency)).unwrap();
                    }
                    if i % conf.producer_rate == 0 {
                        println!("\t - Messages Read {}", i);
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
