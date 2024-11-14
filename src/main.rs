use pravega_client::client_factory::ClientFactory;
use pravega_client_config::ClientConfigBuilder;
use pravega_client_shared::{
    Retention, RetentionType, ScaleType, Scaling, Scope, ScopedStream, 
    Stream, StreamConfiguration,
};

use std::env;
use std::thread;
use std::process;
use std::time::Duration;

mod config;
use config::Config;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Getting the workload file configuration form command line parameters
    let args: Vec<String> = env::args().collect();
    if args.len() <= 1 {
        println!("Arguments missing.\nUsage: {} <Yaml config file>", args[0]);
        process::exit(1);
    }
    
    // Getting config and payload content
    let conf = Config::load_from_file( &args[1].clone() ).expect("Could not read config file.");
    
    // Prepare Benchmark
    println!("Connecting to Pravega {}", conf.address);
    let client_ini = create_client(conf.clone());
    let client_snd = create_client(conf.clone());
    let client_rcv = create_client(conf.clone());
    
    let scostr = init_environment(conf.clone(), client_ini);
    
    // Starting Benchmark
    println!("Starting Benchmark");
    
    // Sender Thread
    let config_cpy = conf.clone();
    let scostr_cpy = scostr.clone();
    let handler_snd = thread::spawn(move || {
        handler_sender(config_cpy, client_snd, scostr_cpy);
    });
    
    // Receiver Thread
    let scostr_cpy = scostr.clone();
    let handler_rcv = thread::spawn(move || { 
        handler_receiver(client_rcv, scostr_cpy);
    });
    
    // Finishing Benchmark
    handler_snd.join().unwrap();
    handler_rcv.join().unwrap();
    println!("Benchmark finished");
    Ok(())
}

fn create_client(conf: Config) -> ClientFactory {
    let pravega_conf = ClientConfigBuilder::default()
        .controller_uri( conf.address )
        .build()
        .unwrap();
    let client_factory = ClientFactory::new(pravega_conf);
    client_factory
}

fn init_environment(conf: Config, client_factory: ClientFactory) -> ScopedStream {
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
    
    let stream = ScopedStream {
        scope:  Scope::from( conf.scope ),
        stream: Stream::from( conf.stream ),
    };
    stream
}

fn handler_sender(conf: Config, client_factory: ClientFactory, scostr: ScopedStream) {
    let payload = conf.message.to_string().into_bytes();
    client_factory.runtime().block_on(async {
        let mut event_writer = client_factory.create_event_writer(scostr);
        
        for i in 1..=conf.message_num {
            let result = event_writer.write_event(payload.clone()).await;
            assert!(result.await.is_ok());
            println!("\t + event sent and flushed {}", i);
        }
    });
    thread::sleep(Duration::from_secs(1));
}

fn handler_receiver(client_factory: ClientFactory, scostr: ScopedStream) {
    client_factory.runtime().block_on(async {
        // create event stream reader
        let rg = client_factory.create_reader_group("rg".to_string(), scostr).await;
        let mut reader = rg.create_reader("r1".to_string()).await;
        
        thread::sleep(Duration::from_secs(1));
        if let Some(mut segment_slice) =  reader.acquire_segment().await.expect("Failed to acquire segment since the reader is offline") {
            while let Some(event) = segment_slice.next() {
                println!("\t - read an event {} ", event.value.len() );
                thread::sleep(Duration::from_millis(10));
            }
        }
        reader
            .reader_offline()
            .await
            .expect("failed to mark the reader offline");
    });
}






