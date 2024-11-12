use pravega_client::client_factory::ClientFactory;
use pravega_client_config::ClientConfigBuilder;
use pravega_client_shared::{
    Retention, RetentionType, ScaleType, Scaling, Scope, ScopedStream, Stream, StreamConfiguration,
};

use std::env;
use std::process;
use std::fs::File;
use std::io::{self, Read};

use serde::{Deserialize, Serialize};
use serde_yaml::{self};

#[derive(Debug, Serialize, Deserialize)]
struct Config {
    name: String,
    address: String,
    payload_file: String,
    message_size: u32,
    producer_rate: u32,
    test_duration_minutes: u32
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Getting the workload file configuration form command line parameters
    let args: Vec<String> = env::args().collect();
    if args.len() <= 1 {
        println!("Arguments missing.\nUsage: {} <Yaml config file>", args[0]);
        process::exit(1);
    }
    
    let pfile = std::fs::File::open( args[1].clone() ).expect("Could not open file.");
    let config: Config = serde_yaml::from_reader(pfile).expect("Could not read values.");
    // Getting the data from the payload file
    let message = get_payload(config.payload_file).unwrap_or_else(|e| {
        eprintln!("Failed to read the payload file: {}", e);
        String::new()
    });
    let payload = message.to_string().into_bytes();
    
    
    
    // Connect to Pravega and send data
    println!("start event write and read example");
    let config = ClientConfigBuilder::default()
        .controller_uri( config.address )
        .build()
        .unwrap();
    
    let client_factory = ClientFactory::new(config);
    println!("client factory created");
    
    client_factory.runtime().block_on(async {
        let controller_client = client_factory.controller_client();
        
        // create a scope
        let scope = Scope::from("fooScope".to_owned());
        controller_client
            .create_scope(&scope)
            .await
            .expect("create scope");
        println!("scope created");

        // create a stream containing only one segment
        let stream = Stream::from("barStream".to_owned());
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
        println!("stream created");

        // create event stream writer
        let stream = ScopedStream::from("fooScope/barStream");
        let mut event_writer = client_factory.create_event_writer(stream.clone());
        println!("event writer created");

        // write payload
        let result = event_writer.write_event(payload).await;
        assert!(result.await.is_ok());
        println!("event writer sent and flushed data");
        
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
    Ok(())
}

fn get_payload(path: String) -> Result<String, io::Error> {
    let mut file = File::open(path)?;
    let mut content = String::new();
    
    file.read_to_string(&mut content)?;
    Ok(content)
}


