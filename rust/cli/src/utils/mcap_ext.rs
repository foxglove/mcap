// Extensions to the MCAP library for CLI operations

use std::collections::HashMap;

pub struct McapInfo {
    pub channel_count: usize,
    pub message_count: u64,
    pub chunk_count: usize,
    pub attachment_count: usize,
    pub metadata_count: usize,
    pub start_time: Option<u64>,
    pub end_time: Option<u64>,
    pub topics: HashMap<String, ChannelInfo>,
}

pub struct ChannelInfo {
    pub id: u16,
    pub topic: String,
    pub message_encoding: String,
    pub schema_name: String,
    pub message_count: u64,
}

impl McapInfo {
    pub fn new() -> Self {
        Self {
            channel_count: 0,
            message_count: 0,
            chunk_count: 0,
            attachment_count: 0,
            metadata_count: 0,
            start_time: None,
            end_time: None,
            topics: HashMap::new(),
        }
    }
}
