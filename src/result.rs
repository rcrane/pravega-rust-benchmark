use std::fs::File;
use std::io::Write;
use chrono::prelude::*;
use crate::config::Config;
use serde::{Deserialize, Serialize};

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
    pub write_latency_50pct: f64,
    pub write_latency_75pct: f64,
    pub write_latency_95pct: f64,
    pub write_latency_99pct: f64,
    pub write_latencies:     Vec<f64>,
    pub read_latencies:      Vec<f64>,
    pub throughput:          f64
}

impl TestResult {
    pub fn new(conf: Config) -> TestResult {
        TestResult { 
            name:                conf.name,
            message_num:         conf.message_num,
            message_size:        conf.message_size,
            scope:               conf.scope,
            stream:              conf.stream,
            duration:            0.0,
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
        self.read_latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
        // Calculate latency percentiles
        self.write_latency_50pct = Self::percentile(&self.write_latencies, 50.0);
        self.write_latency_75pct = Self::percentile(&self.write_latencies, 75.0);
        self.write_latency_95pct = Self::percentile(&self.write_latencies, 95.0);
        self.write_latency_99pct = Self::percentile(&self.write_latencies, 99.0);

        //self.write_latencies.clear();
        //self.read_latencies.clear();
        /*
        Throughput = Total Output / Total Time
        Total Output: total bits sent (messages sent x message size)
        Total Time:   total duration in miliseconds
        */
        let data_sent = self.message_num * self.message_size as u32;
        self.throughput = data_sent as f64 / self.duration;
    }

    pub fn to_file(&self) -> std::io::Result<()> {
        // Format name
        let now  = Utc::now();
        let formatted_date = now.format("%Y%m%d_%H%M%S").to_string();
        let name = format!("result_{}.json", formatted_date);
        // Create file and write results
        let json = serde_json::to_string(&self).unwrap();
        let mut file = File::create(name)?;
        file.write_all(json.as_bytes())?;
        Ok(())
    }
}