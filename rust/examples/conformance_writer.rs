use std::{borrow::Cow, collections::HashMap, env, io::Write, sync::Arc};

#[path = "common/conformance_writer_spec.rs"]
mod conformance_writer_spec;

fn write_file(spec: &conformance_writer_spec::WriterSpec) {
    let mut tmp = tempfile::NamedTempFile::new().expect("Couldn't open file");
    let tmp_path = tmp.path().to_owned();
    let out_buffer = std::io::BufWriter::new(&mut tmp);
    let mut writer = mcap::WriteOptions::new()
        .compression(None)
        .profile("")
        .create(out_buffer)
        .expect("Couldn't create writer");

    let mut channels = HashMap::<u16, mcap::Channel>::new();
    let mut schemas = HashMap::<u64, mcap::Schema>::new();

    for record in &spec.records {
        match record.record_type.as_str() {
            "Attachment" => {
                let attachment = mcap::Attachment {
                    name: record.get_field_str("name").to_owned(),
                    create_time: record.get_field_u64("create_time"),
                    log_time: record.get_field_u64("log_time"),
                    data: Cow::from(record.get_field_data("data")),
                    media_type: record.get_field_str("media_type").to_owned(),
                };
                writer
                    .attach(&attachment)
                    .expect("Couldn't write attachment");
            }
            "AttachmentIndex" => {
                // written automatically
            }
            "Channel" => {
                let id = record.get_field_u16("id");
                let schema_id = record.get_field_u64("schema_id");
                let topic = record.get_field_str("topic");
                let message_encoding = record.get_field_str("message_encoding");
                let schema = schemas.get(&schema_id).expect("Missing schema");
                let channel = mcap::Channel {
                    schema: Some(Arc::new(schema.to_owned())),
                    topic: topic.to_string(),
                    message_encoding: message_encoding.to_string(),
                    metadata: std::collections::BTreeMap::new(),
                };
                writer
                    .add_channel(&channel)
                    .expect("Couldn't write channel");
                channels.insert(id, channel);
            }
            "ChunkIndex" => {
                // written automatically
            }
            "DataEnd" => {
                let data_section_crc = record.get_field_u32("data_section_crc");
                let _data_end = mcap::records::DataEnd { data_section_crc };
                // write data end
            }
            "Footer" => {
                let summmary_offet_start = record.get_field_u64("summary_start");
                let summmary_crc = record.get_field_u32("summary_crc");
                let summmary_start = record.get_field_u64("summary_start");
                let _footer = mcap::records::Footer {
                    summary_crc: summmary_crc,
                    summary_offset_start: summmary_offet_start,
                    summary_start: summmary_start,
                };
                // write footer
            }
            "Header" => {
                let library = record.get_field_str("library");
                let profile = record.get_field_str("profile");
                let _header = mcap::records::Header {
                    library: library.to_string(),
                    profile: profile.to_string(),
                };
                // write header
            }
            "Message" => {
                let channel_id = record.get_field_u16("channel_id");
                let channel = channels.get(&channel_id).expect("Unknown channel");
                let message = mcap::Message {
                    channel: Arc::new(channel.to_owned()),
                    data: Cow::from(record.get_field_data("data")),
                    log_time: record.get_field_u64("log_time"),
                    publish_time: record.get_field_u64("publish_time"),
                    sequence: record.get_field_u32("sequence"),
                };
                writer.write(&message).expect("Write message failed");
            }
            "Metadata" => {
                let name = record.get_field_str("name");
                let fields = record.get_field_meta("metadata");
                let meta = mcap::records::Metadata {
                    name: name.to_string(),
                    metadata: fields,
                };
                writer.write_metadata(&meta).expect("Can't write metadata");
            }
            "Schema" => {
                let name = record.get_field_str("name");
                let encoding = record.get_field_str("encoding");
                let id = record.get_field_u64("id");
                let data: Vec<u8> = record.get_field_data("data");
                let schema = mcap::Schema {
                    name: name.to_owned(),
                    encoding: encoding.to_owned(),
                    data: Cow::from(data),
                };
                schemas.insert(id, schema);
            }
            "Statistics" => {
                // written automatically
            }
            "SummaryOffset" => {
                // written automatically
            }
            _ => panic!("Unrecognzed record type: {}", record.record_type),
        }
        eprintln!("{}: {:?}\n", record.record_type, record);
    }

    writer.finish().expect("Couldn't finish");

    let contents = std::fs::read(tmp_path).expect("Couldn't read output");
    std::io::stdout()
        .write_all(&contents)
        .expect("Couldn't write output");
}

pub fn main() {
    let args: Vec<String> = env::args().collect();
    let input_text =
        std::fs::read_to_string(&args[1]).expect("Should have been able to read the file");

    let spec: conformance_writer_spec::WriterSpec =
        serde_json::from_str(&input_text).expect("Invalid json");

    write_file(&spec);
}
