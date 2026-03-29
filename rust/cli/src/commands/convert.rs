use std::{
    collections::HashMap,
    io::{BufReader, BufWriter, Read},
};

use anyhow::{Context, Result};
use mcap::records::Record;

use crate::{
    cli::ConvertArgs,
    commands::transform_common::{compression_from_str, input_bytes_from_optional_file},
};

const ROS1_BAG_MAGIC: &[u8] = b"#ROSBAG V2.0";
const SQLITE3_MAGIC: &[u8] = b"SQLite format 3\0";

enum InputType {
    Mcap,
    Ros1Bag,
    Ros2Db3,
    Unknown,
}

pub fn run(args: ConvertArgs) -> Result<()> {
    let input_type = detect_input_type(&args.input)?;
    match input_type {
        InputType::Mcap => convert_mcap_to_mcap(args),
        InputType::Ros1Bag => {
            anyhow::bail!(
                "ros1 bag conversion is not implemented in Rust CLI yet; use the Go CLI for now"
            )
        }
        InputType::Ros2Db3 => {
            anyhow::bail!(
                "ros2 db3 conversion is not implemented in Rust CLI yet; use the Go CLI for now"
            )
        }
        InputType::Unknown => anyhow::bail!(
            "unrecognized input format (expected .mcap, rosbag v2, or sqlite3/db3 input)"
        ),
    }
}

fn convert_mcap_to_mcap(args: ConvertArgs) -> Result<()> {
    if args.ament_prefix_path.is_some() {
        anyhow::bail!("--ament-prefix-path is only valid for ROS2 db3 inputs");
    }

    let input_bytes = input_bytes_from_optional_file(Some(&args.input))?;
    let compression = compression_from_str(&args.compression)?;
    let (profile, library) = extract_header_fields(&input_bytes);
    let output_file = std::fs::File::create(&args.output)
        .with_context(|| format!("failed to create output {}", args.output.display()))?;
    let mut writer = mcap::WriteOptions::new()
        .profile(profile)
        .library(library)
        .compression(compression)
        .chunk_size(Some(args.chunk_size))
        .use_chunks(args.chunked)
        .calculate_chunk_crcs(args.include_crc)
        .create(BufWriter::new(output_file))
        .with_context(|| format!("failed to initialize output {}", args.output.display()))?;

    let mut schema_id_map: HashMap<u16, u16> = HashMap::new();
    let mut channel_id_map: HashMap<u16, u16> = HashMap::new();
    let mut records = mcap::read::ChunkFlattener::new(&input_bytes)
        .with_context(|| format!("failed to read records from {}", args.input.display()))?;

    while let Some(record) = records.next() {
        let record = record.context("failed to parse record while converting")?;
        match record {
            Record::Header(_) => {}
            Record::Schema { header, data } => {
                let new_schema_id = writer
                    .add_schema(&header.name, &header.encoding, &data)
                    .with_context(|| format!("failed writing schema {}", header.name))?;
                schema_id_map.insert(header.id, new_schema_id);
            }
            Record::Channel(channel) => {
                let mapped_schema_id = if channel.schema_id == 0 {
                    0
                } else {
                    *schema_id_map.get(&channel.schema_id).ok_or_else(|| {
                        anyhow::anyhow!(
                            "channel '{}' references unknown schema {}",
                            channel.topic,
                            channel.schema_id
                        )
                    })?
                };
                let new_channel_id = writer
                    .add_channel(
                        mapped_schema_id,
                        &channel.topic,
                        &channel.message_encoding,
                        &channel.metadata,
                    )
                    .with_context(|| format!("failed writing channel {}", channel.topic))?;
                channel_id_map.insert(channel.id, new_channel_id);
            }
            Record::Message { header, data } => {
                let mapped_channel_id =
                    *channel_id_map.get(&header.channel_id).ok_or_else(|| {
                        anyhow::anyhow!("message references unknown channel {}", header.channel_id)
                    })?;
                let mapped_header = mcap::records::MessageHeader {
                    channel_id: mapped_channel_id,
                    sequence: header.sequence,
                    log_time: header.log_time,
                    publish_time: header.publish_time,
                };
                writer
                    .write_to_known_channel(&mapped_header, &data)
                    .with_context(|| {
                        format!("failed writing message for channel {}", header.channel_id)
                    })?;
            }
            Record::Attachment { header, data, .. } => {
                writer
                    .attach(&mcap::Attachment {
                        log_time: header.log_time,
                        create_time: header.create_time,
                        name: header.name,
                        media_type: header.media_type,
                        data,
                    })
                    .context("failed writing attachment while converting")?;
            }
            Record::Metadata(metadata) => {
                writer
                    .write_metadata(&metadata)
                    .with_context(|| format!("failed writing metadata {}", metadata.name))?;
            }
            Record::DataEnd(_) => break,
            _ => {}
        }
    }

    writer
        .finish()
        .with_context(|| format!("failed finalizing {}", args.output.display()))?;
    Ok(())
}

fn extract_header_fields(bytes: &[u8]) -> (String, String) {
    let mut reader = match mcap::read::LinearReader::new(bytes) {
        Ok(reader) => reader,
        Err(_) => return (String::new(), String::new()),
    };
    match reader.next() {
        Some(Ok(Record::Header(header))) => (header.profile, header.library),
        _ => (String::new(), String::new()),
    }
}

fn detect_input_type(path: &std::path::Path) -> Result<InputType> {
    let file = std::fs::File::open(path)
        .with_context(|| format!("failed to open input file {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut prefix = [0u8; 16];
    let n = reader
        .read(&mut prefix)
        .with_context(|| format!("failed reading input header {}", path.display()))?;
    let bytes = &prefix[..n];

    if bytes.starts_with(mcap::MAGIC) {
        return Ok(InputType::Mcap);
    }
    if bytes.starts_with(ROS1_BAG_MAGIC) {
        return Ok(InputType::Ros1Bag);
    }
    if bytes.starts_with(SQLITE3_MAGIC) {
        return Ok(InputType::Ros2Db3);
    }
    Ok(InputType::Unknown)
}
