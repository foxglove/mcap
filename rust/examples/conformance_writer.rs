use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    env,
};

#[path = "common/conformance_writer_spec.rs"]
mod conformance_writer_spec;

const USE_CHUNKS: &str = "ch";
const USE_MESSAGE_INDEX: &str = "mx";
const USE_STATISTICS: &str = "st";
const USE_REPEATED_SCHEMAS: &str = "rsh";
const USE_REPEATED_CHANNEL_INFOS: &str = "rch";
const USE_ATTACHMENT_INDEX: &str = "ax";
const USE_METADATA_INDEX: &str = "mdx";
const USE_CHUNK_INDEX: &str = "chx";
const USE_SUMMARY_OFFSET: &str = "sum";
const ADD_EXTRA_DATA_TO_RECORDS: &str = "pad";

fn write_file(spec: &conformance_writer_spec::WriterSpec) {
    let mut write_options = mcap::WriteOptions::new()
        .compression(None)
        .profile("")
        .library("")
        .disable_seeking(true)
        .output_summary(false)
        .output_summary_offsets(false)
        .repeat_channels(false)
        .repeat_schemas(false)
        .output_message_indexes(false)
        .use_chunks(false)
        .output_chunk_indexes(false);

    for feature in spec.meta.variant.features.iter() {
        write_options = match feature.as_str() {
            USE_CHUNKS => write_options.use_chunks(true),
            USE_STATISTICS => write_options.output_summary(true).output_statistics(true),
            USE_SUMMARY_OFFSET => write_options
                .output_summary(true)
                .output_summary_offsets(true),
            _ => unimplemented!("unknown or unimplemented feature: {}", feature),
        }
    }

    let mut writer = write_options
        .create(binrw::io::NoSeek::new(std::io::stdout()))
        .expect("Couldn't create writer");

    let mut channel_ids = HashMap::new();
    let mut schema_ids = HashMap::new();

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
                let schema_id = record.get_field_u16("schema_id");
                let output_schema_id = match schema_id {
                    0 => 0,
                    input_schema_id => {
                        *schema_ids.get(&input_schema_id).expect("unknown schema ID")
                    }
                };
                let topic = record.get_field_str("topic");
                let message_encoding = record.get_field_str("message_encoding");
                let returned_id = writer
                    .add_channel(output_schema_id, topic, message_encoding, &BTreeMap::new())
                    .expect("Couldn't write channel");
                channel_ids.insert(id, returned_id);
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
                let data = record.get_field_data("data");
                let log_time = record.get_field_u64("log_time");
                let publish_time = record.get_field_u64("publish_time");
                let sequence = record.get_field_u32("sequence");
                writer
                    .write_to_known_channel(
                        &mcap::records::MessageHeader {
                            channel_id: *channel_ids
                                .get(&channel_id)
                                .expect("message on unexpected channel ID"),
                            log_time,
                            publish_time,
                            sequence,
                        },
                        &data,
                    )
                    .expect("Write message failed");
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
                let id = record.get_field_u16("id");
                let data: Vec<u8> = record.get_field_data("data");
                let returned_id = writer
                    .add_schema(name, encoding, &data)
                    .expect("cannot write schema");
                schema_ids.insert(id, returned_id);
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
}

pub fn main() {
    let args: Vec<String> = env::args().collect();
    let input_text =
        std::fs::read_to_string(&args[1]).expect("Should have been able to read the file");

    let spec: conformance_writer_spec::WriterSpec =
        serde_json::from_str(&input_text).expect("Invalid json");

    write_file(&spec);
}
