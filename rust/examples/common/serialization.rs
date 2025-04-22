use mcap::records::Record;

use std::collections::BTreeMap;

use serde_json::{json, Value};

// We don't want to force Serde on users just for the sake of the conformance tests.
// (In what context would you want to serialize individual records of a MCAP?)
// Stamp out and stringify them ourselves:

fn get_type(rec: &Record<'_>) -> &'static str {
    match rec {
        Record::Header(_) => "Header",
        Record::Footer(_) => "Footer",
        Record::Schema { .. } => "Schema",
        Record::Channel(_) => "Channel",
        Record::Message { .. } => "Message",
        Record::Chunk { .. } => "Chunk",
        Record::MessageIndex(_) => "MessageIndex",
        Record::ChunkIndex(_) => "ChunkIndex",
        Record::Attachment { .. } => "Attachment",
        Record::AttachmentIndex(_) => "AttachmentIndex",
        Record::Statistics(_) => "Statistics",
        Record::Metadata(_) => "Metadata",
        Record::MetadataIndex(_) => "MetadataIndex",
        Record::SummaryOffset(_) => "SummaryOffset",
        Record::DataEnd(_) => "DataEnd",
        Record::Unknown { opcode, .. } => {
            panic!("Unknown record in conformance test: (op {opcode})")
        }
    }
}

fn get_fields(rec: &Record<'_>) -> Value {
    fn b2s(bytes: &[u8]) -> Vec<String> {
        bytes.iter().map(|b| b.to_string()).collect()
    }
    fn m2s(map: &BTreeMap<u16, u64>) -> BTreeMap<String, String> {
        map.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    match rec {
        Record::Header(h) => json!([["library", h.library], ["profile", h.profile]]),
        Record::Footer(f) => json!([
            ["summary_crc", f.summary_crc.to_string()],
            ["summary_offset_start", f.summary_offset_start.to_string()],
            ["summary_start", f.summary_start.to_string()]
        ]),
        Record::Schema { header, data } => json!([
            ["data", b2s(data)],
            ["encoding", header.encoding],
            ["id", header.id.to_string()],
            ["name", header.name]
        ]),
        Record::Channel(c) => json!([
            ["id", c.id.to_string()],
            ["message_encoding", c.message_encoding],
            ["metadata", c.metadata],
            ["schema_id", c.schema_id.to_string()],
            ["topic", c.topic]
        ]),
        Record::Message { header, data } => json!([
            ["channel_id", header.channel_id.to_string()],
            ["data", b2s(data)],
            ["log_time", header.log_time.to_string()],
            ["publish_time", header.publish_time.to_string()],
            ["sequence", header.sequence.to_string()]
        ]),
        Record::Chunk { .. } => unreachable!("Chunks are flattened"),
        Record::MessageIndex(_) => unreachable!("MessageIndexes are skipped"),
        Record::ChunkIndex(i) => json!([
            ["chunk_length", i.chunk_length.to_string()],
            ["chunk_start_offset", i.chunk_start_offset.to_string()],
            ["compressed_size", i.compressed_size.to_string()],
            ["compression", i.compression],
            ["message_end_time", i.message_end_time.to_string()],
            ["message_index_length", i.message_index_length.to_string()],
            ["message_index_offsets", m2s(&i.message_index_offsets)],
            ["message_start_time", i.message_start_time.to_string()],
            ["uncompressed_size", i.uncompressed_size.to_string()]
        ]),
        Record::Attachment { header, data, .. } => json!([
            ["create_time", header.create_time.to_string()],
            ["data", b2s(data)],
            ["log_time", header.log_time.to_string()],
            ["media_type", header.media_type],
            ["name", header.name],
        ]),
        Record::AttachmentIndex(i) => json!([
            ["create_time", i.create_time.to_string()],
            ["data_size", i.data_size.to_string()],
            ["length", i.length.to_string()],
            ["log_time", i.log_time.to_string()],
            ["media_type", i.media_type],
            ["name", i.name],
            ["offset", i.offset.to_string()]
        ]),
        Record::Statistics(s) => json!([
            ["attachment_count", s.attachment_count.to_string()],
            ["channel_count", s.channel_count.to_string()],
            ["channel_message_counts", m2s(&s.channel_message_counts)],
            ["chunk_count", s.chunk_count.to_string()],
            ["message_count", s.message_count.to_string()],
            ["message_end_time", s.message_end_time.to_string()],
            ["message_start_time", s.message_start_time.to_string()],
            ["metadata_count", s.metadata_count.to_string()],
            ["schema_count", s.schema_count.to_string()]
        ]),
        Record::Metadata(m) => json!([["metadata", m.metadata], ["name", m.name]]),
        Record::MetadataIndex(i) => json!([
            ["length", i.length.to_string()],
            ["name", i.name],
            ["offset", i.offset.to_string()]
        ]),
        Record::SummaryOffset(s) => json!([
            ["group_length", s.group_length.to_string()],
            ["group_opcode", s.group_opcode.to_string()],
            ["group_start", s.group_start.to_string()]
        ]),
        Record::DataEnd(d) => json!([["data_section_crc", d.data_section_crc.to_string()]]),
        Record::Unknown { opcode, .. } => {
            panic!("Unknown record in conformance test: (op {opcode})")
        }
    }
}

pub fn as_json(view: &Record<'_>) -> Value {
    let typename = get_type(view);
    let fields = get_fields(view);
    json!({"type": typename, "fields": fields})
}
