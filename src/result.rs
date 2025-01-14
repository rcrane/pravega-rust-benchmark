use std::fs::File;
use std::io::Write;
use chrono::prelude::*;
use serde::Serialize;
use serde::Deserialize;
use crate::config::Config;
use statrs::statistics::Max;
use statrs::statistics::Data;
use std::collections::HashMap;
use statrs::statistics::Distribution;
use statrs::statistics::OrderStatistics;

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
    pub write_latency_avg:   f64,
    pub write_latency_max:   f64,
    pub write_latency_hist:  HashMap<u32, u32>,
    pub read_latency_hist:   HashMap<u32, u32>,
    pub throughput:          f64,
    pub sent_data:           f64,
    #[serde(skip_serializing)]
    pub write_latencies:     Vec<f64>,
    #[serde(skip_serializing)]
    pub read_latencies:      Vec<f64>
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
            write_latency_avg:   0.0,
            write_latency_max:   0.0,
            write_latencies:     Vec::new(),
            read_latencies:      Vec::new(),
            write_latency_hist:  HashMap::new(),
            read_latency_hist:   HashMap::new(),
            throughput:          0.0,
            sent_data:           0.0
        }
    }

    fn round3(value: f64) -> f64 {
        let rounded_value = (value * 1000.0).round() / 1000.0;
        rounded_value
    }

    pub fn calculate_metrics(&mut self) {
        // Remove write failures
        let latencies = self.write_latencies.clone();
        self.write_latencies.clear();
        for &latency in &latencies {
            if latency > 0.0 {
                self.write_latencies.push(latency);
            }
        }
        self.message_num = self.write_latencies.len() as u32;
        // Calculate latency percentiles
        let mut data = Data::new(self.write_latencies.clone());
        self.write_latency_50pct = Self::round3(data.percentile(50));
        self.write_latency_75pct = Self::round3(data.percentile(75));
        self.write_latency_95pct = Self::round3(data.percentile(95));
        self.write_latency_99pct = Self::round3(data.percentile(99));
        self.write_latency_avg   = Self::round3(data.mean().unwrap());
        self.write_latency_max   = Self::round3(data.max());
        // Write Histogram
        self.write_latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
        for &latency in &self.write_latencies {
            *self.write_latency_hist.entry(latency as u32).or_insert(0) += 1;
        }
        // Read Histogram
        self.read_latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
        for &latency in &self.read_latencies {
            *self.read_latency_hist.entry(latency as u32).or_insert(0) += 1;
        }
        /*
         * Throughput = Total Output / Total Time (MB/s)
         * where:
         *   Total Output = total MB sent
         *   Total Time   = total duration in seconds
         */
        self.duration   = self.duration / 1000.0;
        self.sent_data  = (self.message_num * self.message_size as u32) as f64 / 1000000.0;
        self.throughput = self.sent_data / self.duration;
    }

    pub fn to_file(&self) -> std::io::Result<()> {
        // Format name
        let now  = Utc::now();
        let formatted_date = now.format("%Y%m%d_%H%M%S").to_string();
        let name = format!("result_{}.json", formatted_date);
        // Create file and write results
        let json = serde_json::to_string(&self).unwrap();
        let mut file = File::create(name.clone())?;
        file.write_all(json.as_bytes())?;

        println!("Results writen in {}", name);
        Ok(())
    }
}