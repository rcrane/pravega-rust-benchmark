
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Stats {
    pub write_latency: Vec<i32>,
    pub read_latency:  Vec<i32>
}


