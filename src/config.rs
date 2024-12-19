use std::io;
use std::io::Read;
use std::fs::File;
use serde_yaml::{self};
use chrono::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct ConfigYaml {
    pub name:                   String,
    pub address:                String,
    pub payload_file:           String,
    pub message_num:            u32,
    pub message_warmup:         Option<u32>,
    pub producer_rate:          u32,
    pub scope:                  Option<String>,
    pub stream:                 Option<String>,
    pub retention_time:         Option<i64>,
    pub scale_target_rate:      Option<i32>,
    pub scale_factor:           Option<i32>,
    pub scale_min_num_segments: Option<i32>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Config {
    pub name:                   String,
    pub address:                String,
    pub payload_file:           String,
    pub message:                String,
    pub message_size:           usize,
    pub message_num:            u32,
    pub message_warmup:         u32,
    pub scope:                  String,
    pub stream:                 String,
    pub producer_rate:          u32,
    pub retention_time:         i64,
    pub scale_target_rate:      i32,
    pub scale_factor:           i32,
    pub scale_min_num_segments: i32,
}

impl Config {
    pub fn new() -> Self {
        Config {
            name:                   "".to_string(),
            address:                "".to_string(),
            payload_file:           "".to_string(),
            message:                "".to_string(),
            message_size:           0,
            message_num:            0,
            message_warmup:         5,
            scope:                  "".to_string(),
            stream:                 "".to_string(),
            retention_time:         10,
            producer_rate:          0,
            scale_target_rate:      1,
            scale_factor:           0,
            scale_min_num_segments: 1,
        }
    }

    pub fn load_from_file(file_path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let file = std::fs::File::open(file_path)?;
        let mut conf: Config = Self::new();
        let conf_yaml: ConfigYaml = serde_yaml::from_reader(file)?;
        
        conf.name          = conf_yaml.name;
        conf.address       = conf_yaml.address;
        conf.payload_file  = conf_yaml.payload_file;
        conf.message_num   = conf_yaml.message_num;
        conf.message       = Self::get_payload(conf.payload_file.clone()).expect("Failed to read the payload file.");
        conf.message_size  = conf.message.len();
        conf.producer_rate = conf_yaml.producer_rate;
        
        if conf_yaml.scope == None {
            conf.scope = Self::generate_name("scope".to_string());
        } else {
            conf.scope = conf_yaml.scope.unwrap_or("scope".to_string());
        }
        if conf_yaml.stream == None {
            conf.stream = Self::generate_name("stream".to_string());
        } else {
            conf.stream = conf_yaml.stream.unwrap_or("stream".to_string());
        }

        if conf_yaml.message_warmup != None {
            conf.message_warmup = conf_yaml.message_warmup.unwrap_or(conf.message_warmup);
        }
        if conf_yaml.retention_time != None {
            conf.retention_time = conf_yaml.retention_time.unwrap_or(conf.retention_time);
        }
        if conf_yaml.scale_target_rate != None {
            conf.scale_target_rate = conf_yaml.scale_target_rate.unwrap_or(conf.scale_target_rate);
        }
        if conf_yaml.scale_factor != None {
            conf.scale_factor = conf_yaml.scale_factor.unwrap_or(conf.scale_factor);
        }
        if conf_yaml.scale_min_num_segments != None {
            conf.scale_min_num_segments = conf_yaml.scale_min_num_segments.unwrap_or(conf.scale_min_num_segments);
        }
        Ok(conf)
    }
    
    fn generate_name(init: String) -> String {
        let now = Utc::now();
        let name = init.to_owned() + &now.timestamp().to_string();
        name
    }
    
    fn get_payload(path: String) -> Result<String, io::Error> {
        let mut file = File::open(path)?;
        let mut content = String::new();
        
        file.read_to_string(&mut content)?;
        Ok(content)
    }
}
