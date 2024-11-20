use std::fs::File;
use std::io::Write;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Stats {
    pub write_latency_mean: f64,
    pub write_latencies: Vec<f64>,
    pub throughput: f64
}

impl Stats {
    // Associated function (constructor)
    pub fn new() -> Stats {
        Stats { 
            write_latencies: Vec::new(),
            write_latency_mean: 0.0,
            throughput: 0.0
        }
    }

    pub fn calculate_metrics(&mut self, messages_num: u32, message_size: usize) {
        let mut time = 0.0;
        for latency in &self.write_latencies {
            time += latency;
        }

        let data_sent = messages_num * message_size as u32;
        self.write_latency_mean = time / self.write_latencies.len() as f64;
        /*
        Throughput = Total Output / Total Time
        Total Output: sended data (usually measured in bytes or bits)
        Total Time:   10 seconds  (usually measured in seconds)
        */
        self.throughput = data_sent as f64 / time;
    }

    pub fn to_file(&self) -> std::io::Result<()> {
        let json = serde_json::to_string(&self).unwrap();
        let mut file = File::create("results.json")?;
        file.write_all(json.as_bytes())?;
        Ok(())
    }
}


