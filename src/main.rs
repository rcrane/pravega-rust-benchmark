use pravega_client::client_factory::ClientFactory;
use pravega_client::event::EventWriter;
use pravega_client_config::ClientConfigBuilder;
use pravega_client_shared::{
    Retention, RetentionType, ScaleType, Scaling, Scope, ScopedStream, 
    Stream, StreamConfiguration,
};

use std::io;
use std::env;
use std::thread;
use std::process;
use std::io::Read;
use std::fs::File;
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
    let message = get_payload(conf.payload_file.clone()).expect("Failed to read the payload file.");
    let payload = message.to_string().into_bytes();
    
    // Create scope and stream for the sending
    init_environment(conf.clone());
    
    // Sender Thread
    let config_cpy  = conf.clone();
    let handler_snd = thread::spawn(move || {
        let pravega_config = ClientConfigBuilder::default()
            .controller_uri( config_cpy.address )
            .build()
            .unwrap();
        
        let client_factory = ClientFactory::new(pravega_config);
        client_factory.runtime().block_on(async {
            // create event stream writer
            let stream = ScopedStream::from(config_cpy.from.as_str());
            //let stream = ScopedStream::from(config_cpy.from.as_str());
            let event_writer = client_factory.create_event_writer(stream.clone());
            println!("event writer created");
            
            write_payload(event_writer, payload).await;
            println!("event writer sent and flushed data");
        });
        thread::sleep(Duration::from_secs(1));
    });
    
    let config_cpy = conf.clone();
    let handler_rcv = thread::spawn(move || {
        thread::sleep(Duration::from_secs(1));
        let pravega_config = ClientConfigBuilder::default()
            .controller_uri( config_cpy.address )
            .build()
            .unwrap();
        
        let client_factory = ClientFactory::new(pravega_config);
        client_factory.runtime().block_on(async {
            // create event stream writer
            let stream = ScopedStream::from(config_cpy.from.as_str());
            
            // create event stream reader
            let rg = client_factory.create_reader_group("rg".to_string(), stream).await;
            let mut reader = rg.create_reader("r1".to_string()).await;
            println!("event reader created");
            
            // read from segment
            if let Some(mut slice) = reader
                .acquire_segment()
                .await
                .expect("Failed to acquire segment since the reader is offline")
            {
                let read_event = slice.next();
                assert!(read_event.is_some(), "event slice should have event to read");
                assert_eq!(message.to_string().as_bytes(), read_event.unwrap().value.as_slice());
                println!("event reader read data");
            } else {
                println!("no data to read from the Pravega stream");
                panic!("read should return the written event.")
            }

            reader
                .reader_offline()
                .await
                .expect("failed to mark the reader offline");
            println!("event write and read example finished");
        });
        thread::sleep(Duration::from_secs(1));
    });
    
    handler_snd.join().unwrap();
    handler_rcv.join().unwrap();
    Ok(())
}

fn get_payload(path: String) -> Result<String, io::Error> {
    let mut file = File::open(path)?;
    let mut content = String::new();
    
    file.read_to_string(&mut content)?;
    Ok(content)
}

async fn write_payload(mut event_writer: EventWriter, payload: Vec<u8>) {
    let result = event_writer.write_event(payload).await;
    assert!(result.await.is_ok());
}

fn init_environment(conf: Config) {
    // Connect to Pravega and send data
    println!("Connecting to Pravega {}", conf.address);
    let pravega_config = ClientConfigBuilder::default()
        .controller_uri( conf.address )
        .build()
        .unwrap();
    
    let client_factory = ClientFactory::new(pravega_config);
    println!("client factory created");
    
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
}

