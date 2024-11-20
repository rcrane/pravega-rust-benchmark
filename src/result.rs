use std::fs::File;
use std::io::Write;
use serde::{Deserialize, Serialize};

use crate::config::Config;

#[derive(Serialize, Deserialize)]
pub struct TestResult {
    // Test Configuration
    pub name:         String,
    pub message_num:  u32,
    pub message_size: usize,
    pub scope:        String,
    pub stream:       String,
    pub duration:     f64,
    // Metrics
    pub mean_write_latency:  f64,
    pub write_latency_50pct: f64,
    pub write_latency_75pct: f64,
    pub write_latency_95pct: f64,
    pub write_latency_99pct: f64,
    pub write_latencies:     Vec<f64>,
    pub read_latencies:     Vec<f64>,
    pub throughput:          f64
}

impl TestResult {
    // Associated function (constructor)
    pub fn new(conf: Config) -> TestResult {
        TestResult { 
            name:                conf.name,
            message_num:         conf.message_num,
            message_size:        conf.message_size,
            scope:               "".to_string(),
            stream:              "".to_string(),
            duration:            0.0,
            mean_write_latency:  0.0,
            write_latency_50pct: 0.0,
            write_latency_75pct: 0.0,
            write_latency_95pct: 0.0,
            write_latency_99pct: 0.0,
            write_latencies:     Vec::new(),
            read_latencies:      Vec::new(),
            throughput:          0.0
        }
    }

    fn percentile(data: &Vec<f64>, percentile: f64) -> f64 {
        let rank = percentile / 100.0 * (data.len() - 1) as f64;
        let lower = rank.floor() as usize;
        let upper = rank.ceil() as usize;
        let weight = rank - lower as f64;
    
        if lower == upper {
            data[lower]
        } else {
            data[lower] * (1.0 - weight) + data[upper] * weight
        }
    }

    pub fn calculate_metrics(&mut self) {
        // Sort latencies
        self.write_latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
        // Calculate latency percentiles
        self.write_latency_50pct = Self::percentile(&self.write_latencies, 50.0);
        self.write_latency_75pct = Self::percentile(&self.write_latencies, 75.0);
        self.write_latency_95pct = Self::percentile(&self.write_latencies, 95.0);
        self.write_latency_99pct = Self::percentile(&self.write_latencies, 99.0);
        // Duration and mean latency
        self.duration = self.write_latencies.iter().sum();
        self.mean_write_latency = self.duration / self.write_latencies.len() as f64;
        /*
        Throughput = Total Output / Total Time
        Total Output: total bits sent (messages sent x message size)
        Total Time:   total duration in miliseconds
        */
        let data_sent = self.message_num * self.message_size as u32;
        self.throughput = data_sent as f64 / self.duration;
    }

    pub fn to_file(&self) -> std::io::Result<()> {
        let json = serde_json::to_string(&self).unwrap();
        let mut file = File::create("results.json")?;
        file.write_all(json.as_bytes())?;
        Ok(())
    }
}