
pub enum ChannelData {
    Scope(String),
    Stream(String),
    WriteLatency(f64),
    ReadLatency(f64)
}
