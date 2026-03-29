use std::collections::HashMap;

use anyhow::{Context, Result};
use enumset::enum_set;
use mcap::records::{MessageHeader, Record};

use crate::{
    cli::RecoverArgs,
    commands::transform_common::{compression_from_str, input_bytes_from_optional_file},
};

pub fn run(args: RecoverArgs) -> Result<()> {
    let input = input_bytes_from_optional_file(Some(&args.file))?;
    crate::cli_io::ensure_local_path(&args.output)?;
    let output = std::fs::File::create(&args.output)
        .with_context(|| format!("failed to create output {}", args.output.display()))?;
    let compression = compression_from_str(&args.compression)?;
    let mut writer = mcap::WriteOptions::new()
        .compression(compression)
        .chunk_size(Some(args.chunk_size))
        .create(std::io::BufWriter::new(output))
        .with_context(|| format!("failed to initialize writer for {}", args.output.display()))?;

    let mut schema_id_map: HashMap<u16, u16> = HashMap::new();
    let mut channel_id_map: HashMap<u16, u16> = HashMap::new();
    let mut recovered_messages = 0u64;
    let mut recovered_attachments = 0u64;
    let mut recovered_metadata = 0u64;
    let mut warnings = 0u64;

    let mut iter = mcap::read::ChunkFlattener::new_with_options(
        &input,
        enum_set!(mcap::read::Options::IgnoreEndMagic),
    )
    .with_context(|| format!("failed to create record stream for {}", args.file.display()))?;

    while let Some(record_result) = iter.next() {
        let record = match record_result {
            Ok(record) => record,
            Err(err) => {
                warnings += 1;
                eprintln!("Warning: recover stopped at parse error: {err}");
                break;
            }
        };

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
                let Some(mapped_channel_id) = channel_id_map.get(&header.channel_id).copied()
                else {
                    continue;
                };

                let mapped_header = MessageHeader {
                    channel_id: mapped_channel_id,
                    sequence: header.sequence,
                    log_time: header.log_time,
                    publish_time: header.publish_time,
                };
                writer
                    .write_to_known_channel(&mapped_header, &data)
                    .context("failed writing recovered message")?;
                recovered_messages += 1;
            }
            Record::Metadata(metadata) => {
                writer
                    .write_metadata(&metadata)
                    .with_context(|| format!("failed writing metadata {}", metadata.name))?;
                recovered_metadata += 1;
            }
            Record::Attachment { header, data, .. } => {
                let attachment = mcap::Attachment {
                    log_time: header.log_time,
                    create_time: header.create_time,
                    name: header.name,
                    media_type: header.media_type,
                    data,
                };
                writer
                    .attach(&attachment)
                    .with_context(|| format!("failed writing attachment {}", attachment.name))?;
                recovered_attachments += 1;
            }
            _ => {}
        }
    }

    writer
        .finish()
        .with_context(|| format!("failed finalizing {}", args.output.display()))?;
    eprintln!(
        "Recovered {recovered_messages} messages, {recovered_attachments} attachments, and {recovered_metadata} metadata records ({warnings} warning(s))."
    );
    Ok(())
}
