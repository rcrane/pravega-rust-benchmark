
use serde_yaml::{self};
use chrono::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct ConfigYaml {
    pub name:         String,
    pub address:      String,
    pub payload_file: String,
    pub message_num:  u32,
    pub scope:        Option<String>,
    pub stream:       Option<String>
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Config {
    pub name:         String,
    pub address:      String,
    pub payload_file: String,
    pub message_num:  u32,
    pub scope:        String,
    pub stream:       String,
    pub from:         String
}

impl Config {
    pub fn new() -> Self {
        Config {
            name:         "".to_string(),
            address:      "".to_string(),
            payload_file: "".to_string(),
            message_num:  0,
            scope:        "".to_string(),
            stream:       "".to_string(),
            from:         "".to_string()
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
        
        conf.from = conf.scope.clone() + "/" + &conf.stream.clone();
        Ok(conf)
    }
    
    fn generate_name(init: String) -> String {
        let now = Utc::now();
        let name = init.to_owned() + &now.timestamp().to_string();
        name
    }
}
