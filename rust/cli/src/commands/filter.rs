use std::collections::HashMap;

use anyhow::{Context, Result};
use mcap::records::{MessageHeader, Record};
use regex::Regex;

use crate::{
    cli::FilterArgs,
    commands::transform_common::{
        compression_from_str, input_bytes_from_optional_file, parse_optional_time_bound,
        write_output_bytes,
    },
};

fn compile_patterns(patterns: &[String]) -> Result<Vec<Regex>> {
    patterns
        .iter()
        .map(|pattern| Regex::new(pattern).with_context(|| format!("invalid regex: {pattern}")))
        .collect()
}

fn topic_matches(topic: &str, include: &[Regex], exclude: &[Regex]) -> bool {
    if !include.is_empty() && !include.iter().any(|re| re.is_match(topic)) {
        return false;
    }
    if !exclude.is_empty() && exclude.iter().any(|re| re.is_match(topic)) {
        return false;
    }
    true
}

pub fn run(args: FilterArgs) -> Result<()> {
    let input = input_bytes_from_optional_file(args.file.as_ref())?;

    let include_topic_regex = compile_patterns(&args.include_topic_regex)?;
    let exclude_topic_regex = compile_patterns(&args.exclude_topic_regex)?;
    if !include_topic_regex.is_empty() && !exclude_topic_regex.is_empty() {
        anyhow::bail!("can only use one of --include-topic-regex and --exclude-topic-regex");
    }

    let start = parse_optional_time_bound(args.start.as_deref())?.unwrap_or(0);
    let end = parse_optional_time_bound(args.end.as_deref())?.unwrap_or(u64::MAX);
    if end < start {
        anyhow::bail!("invalid time range query, end-time is before start-time");
    }

    let output_compression = compression_from_str(&args.output_compression)?;
    let output = std::io::Cursor::new(Vec::new());
    let mut writer = mcap::WriteOptions::new()
        .compression(output_compression)
        .chunk_size(Some(args.chunk_size))
        .use_chunks(!args.unchunked)
        .create(output)?;

    let mut schema_id_map: HashMap<u16, u16> = HashMap::new();
    let mut channel_id_map: HashMap<u16, u16> = HashMap::new();
    let mut channel_topic_map: HashMap<u16, String> = HashMap::new();

    let mut iter = mcap::read::ChunkFlattener::new(&input)
        .context("failed to create record stream for filtering")?;
    while let Some(record) = iter.next() {
        match record.context("failed to parse record while filtering")? {
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
                channel_topic_map.insert(channel.id, channel.topic);
                channel_id_map.insert(channel.id, new_channel_id);
            }
            Record::Message { header, data } => {
                if header.log_time < start || header.log_time >= end {
                    continue;
                }
                let Some(topic) = channel_topic_map.get(&header.channel_id) else {
                    continue;
                };
                if !topic_matches(topic, &include_topic_regex, &exclude_topic_regex) {
                    continue;
                }
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
                    .context("failed writing message")?;
            }
            Record::Metadata(metadata) if args.include_metadata => {
                writer
                    .write_metadata(&metadata)
                    .with_context(|| format!("failed writing metadata {}", metadata.name))?;
            }
            Record::Attachment { header, data, .. } if args.include_attachments => {
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
            }
            _ => {}
        }
    }

    let _summary = writer
        .finish()
        .context("failed to finalize filtered output")?;
    let output_bytes = writer.into_inner().into_inner();
    write_output_bytes(args.output.as_ref(), &output_bytes)?;
    Ok(())
}
