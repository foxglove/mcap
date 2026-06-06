use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use mcap::records::{self, Record};

const FOOTER_RECORD_LEN: usize = 1 + 8 + 8 + 8 + 4;
pub(crate) const FOOTER_RECORD_AND_END_MAGIC_LEN: usize = FOOTER_RECORD_LEN + mcap::MAGIC.len();

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedSchema {
    pub header: records::SchemaHeader,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ParsedMcap {
    pub header: Option<records::Header>,
    pub statistics: Option<records::Statistics>,
    pub channels: std::collections::BTreeMap<u16, records::Channel>,
    pub schemas: std::collections::BTreeMap<u16, ParsedSchema>,
    pub chunk_indexes: Vec<records::ChunkIndex>,
    pub attachment_indexes: Vec<records::AttachmentIndex>,
    pub metadata_indexes: Vec<records::MetadataIndex>,
}

pub fn parse_mcap(mcap: &[u8]) -> Result<ParsedMcap> {
    let header = read_header(mcap)?;
    if let Some(parsed_from_summary) = parse_mcap_from_summary(mcap, header.clone())? {
        return Ok(parsed_from_summary);
    }

    eprintln!(
        "Warning: summary section not available; full scan may be slow. Run `mcap doctor` for details."
    );
    parse_mcap_linear(mcap, header)
}

pub(crate) fn read_header(mcap: &[u8]) -> Result<Option<records::Header>> {
    let mut reader = mcap::read::LinearReader::new(mcap)?;
    match reader.next() {
        Some(Ok(Record::Header(header))) => Ok(Some(header)),
        Some(Ok(_)) | None => Ok(None),
        Some(Err(err)) => Err(err.into()),
    }
}

fn parse_mcap_from_summary(
    mcap: &[u8],
    header: Option<records::Header>,
) -> Result<Option<ParsedMcap>> {
    let footer = mcap::read::footer(mcap)?;
    if footer.summary_start == 0 {
        return Ok(None);
    }

    let footer_start = mcap
        .len()
        .checked_sub(FOOTER_RECORD_AND_END_MAGIC_LEN)
        .context("input is too short to contain a footer")?;
    let summary_start =
        usize::try_from(footer.summary_start).context("summary offset is too large")?;
    if summary_start > footer_start {
        return Err(mcap::McapError::UnexpectedEof.into());
    }

    Ok(Some(parsed_mcap_from_summary_section(
        header,
        &mcap[summary_start..footer_start],
    )?))
}

pub(crate) fn parsed_mcap_from_summary_section(
    header: Option<records::Header>,
    summary: &[u8],
) -> Result<ParsedMcap> {
    let mut out = ParsedMcap {
        header,
        ..ParsedMcap::default()
    };
    for record in mcap::read::LinearReader::sans_magic(summary) {
        collect_record(&mut out, record?)?;
    }
    Ok(out)
}

fn parse_mcap_linear(mcap: &[u8], header: Option<records::Header>) -> Result<ParsedMcap> {
    let mut out = ParsedMcap {
        header,
        ..ParsedMcap::default()
    };
    for record in mcap::read::LinearReader::new(mcap)? {
        let record = record?;
        if let Record::Chunk { header, data } = record {
            for nested_record in mcap::read::ChunkReader::new(header, data.as_ref())? {
                collect_record(&mut out, nested_record?)?;
            }
        } else {
            collect_record(&mut out, record)?;
        }
    }

    Ok(out)
}

fn collect_record(out: &mut ParsedMcap, record: Record<'_>) -> Result<()> {
    match record {
        Record::Header(header) => {
            if let Some(existing) = &out.header {
                if existing != &header {
                    bail!("conflicting MCAP header records");
                }
            } else {
                out.header = Some(header);
            }
        }
        Record::Statistics(statistics) => {
            out.statistics = Some(statistics);
        }
        Record::Channel(channel) => {
            if let Some(existing) = out.channels.get(&channel.id) {
                if existing != &channel {
                    bail!("conflicting channel definition for id {}", channel.id);
                }
            } else {
                out.channels.insert(channel.id, channel);
            }
        }
        Record::Schema { header, data } => {
            let schema = ParsedSchema {
                header,
                data: data.into_owned(),
            };
            if let Some(existing) = out.schemas.get(&schema.header.id) {
                if existing != &schema {
                    bail!("conflicting schema definition for id {}", schema.header.id);
                }
            } else {
                out.schemas.insert(schema.header.id, schema);
            }
        }
        Record::ChunkIndex(index) => out.chunk_indexes.push(index),
        Record::AttachmentIndex(index) => out.attachment_indexes.push(index),
        Record::MetadataIndex(index) => out.metadata_indexes.push(index),
        _ => {}
    }
    Ok(())
}

// TODO: keep this in sync with mcap::sans_io::SummaryReader and mcap::read::ChannelAccumulator.
// A future mcap crate range-summary API should replace this CLI-local parser.
pub(crate) fn parse_summary_section(summary: &[u8]) -> Result<mcap::Summary> {
    let mut out = mcap::Summary::default();
    let mut schemas = HashMap::<u16, Arc<mcap::Schema<'static>>>::new();
    let mut channel_defs = HashMap::<u16, records::Channel>::new();

    for record in mcap::read::LinearReader::sans_magic(summary) {
        match record? {
            Record::AttachmentIndex(index) => out.attachment_indexes.push(index),
            Record::MetadataIndex(index) => out.metadata_indexes.push(index),
            Record::Statistics(statistics) => out.stats = Some(statistics),
            Record::ChunkIndex(index) => out.chunk_indexes.push(index),
            Record::Schema { header, data } => {
                if header.id == 0 {
                    return Err(mcap::McapError::InvalidSchemaId.into());
                }
                let schema = Arc::new(mcap::Schema {
                    id: header.id,
                    name: header.name,
                    encoding: header.encoding,
                    data: Cow::Owned(data.into_owned()),
                });
                match schemas.entry(schema.id) {
                    std::collections::hash_map::Entry::Occupied(entry) => {
                        let existing = entry.get();
                        if existing.name != schema.name
                            || existing.encoding != schema.encoding
                            || existing.data.as_ref() != schema.data.as_ref()
                        {
                            return Err(
                                mcap::McapError::ConflictingSchemas(schema.name.clone()).into()
                            );
                        }
                    }
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        entry.insert(schema);
                    }
                }
            }
            Record::Channel(channel) => match channel_defs.entry(channel.id) {
                std::collections::hash_map::Entry::Occupied(entry) => {
                    let existing = entry.get();
                    if existing != &channel {
                        return Err(
                            mcap::McapError::ConflictingChannels(channel.topic.clone()).into()
                        );
                    }
                }
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(channel);
                }
            },
            _ => {}
        }
    }
    out.channels = channel_defs
        .into_iter()
        .map(|(id, channel)| {
            let schema = if channel.schema_id == 0 {
                None
            } else {
                schemas.get(&channel.schema_id).cloned()
            };
            (
                id,
                Arc::new(mcap::Channel {
                    id: channel.id,
                    topic: channel.topic,
                    schema,
                    message_encoding: channel.message_encoding,
                    metadata: channel.metadata,
                }),
            )
        })
        .collect();
    out.schemas = schemas;
    Ok(out)
}

// TODO: keep these exact-record parsers in sync with mcap::read::metadata and
// mcap::read::attachment. They duplicate the mcap crate helpers so remote range callers can
// parse owned records without holding a full-file byte slice alive.
pub(crate) fn parse_metadata_record(bytes: &[u8]) -> Result<mcap::records::Metadata> {
    let mut reader = mcap::read::LinearReader::sans_magic(bytes);
    let metadata = match reader.next().ok_or(mcap::McapError::BadIndex)?? {
        mcap::records::Record::Metadata(metadata) => metadata,
        _ => return Err(mcap::McapError::BadIndex.into()),
    };
    if reader.next().is_some() {
        return Err(mcap::McapError::BadIndex.into());
    }
    Ok(metadata)
}

pub(crate) fn parse_attachment_record(bytes: &[u8]) -> Result<mcap::Attachment<'static>> {
    let mut reader = mcap::read::LinearReader::sans_magic(bytes);
    let attachment = match reader.next().ok_or(mcap::McapError::BadIndex)?? {
        mcap::records::Record::Attachment { header, data, .. } => mcap::Attachment {
            log_time: header.log_time,
            create_time: header.create_time,
            name: header.name,
            media_type: header.media_type,
            data: Cow::Owned(data.into_owned()),
        },
        _ => return Err(mcap::McapError::BadIndex.into()),
    };
    if reader.next().is_some() {
        return Err(mcap::McapError::BadIndex.into());
    }
    Ok(attachment)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{parse_mcap, parse_mcap_from_summary, parse_summary_section};
    use mcap::records;

    #[test]
    fn parse_mcap_collects_channels_and_schemas() {
        let mut buffer = Vec::new();
        let (schema_id, channel_id) = {
            let mut writer = mcap::Writer::new(std::io::Cursor::new(&mut buffer)).expect("writer");
            let schema_id = writer
                .add_schema("demo_schema", "jsonschema", br#"{"type":"object"}"#)
                .expect("schema");
            let channel_id = writer
                .add_channel(schema_id, "/demo", "json", &BTreeMap::new())
                .expect("channel");
            writer
                .write_to_known_channel(
                    &records::MessageHeader {
                        channel_id,
                        sequence: 1,
                        log_time: 10,
                        publish_time: 11,
                    },
                    br#"{"k":"v"}"#,
                )
                .expect("write message");
            writer.finish().expect("finish writer");
            (schema_id, channel_id)
        };

        let parsed = parse_mcap(&buffer).expect("parse mcap");
        assert!(parsed.header.is_some());
        assert!(parsed.channels.contains_key(&channel_id));
        assert!(parsed.schemas.contains_key(&schema_id));
    }

    #[test]
    fn parse_mcap_falls_back_for_summaryless_files() {
        let mut buffer = Vec::new();
        let (schema_id, channel_id) = {
            let mut writer = mcap::WriteOptions::new()
                .emit_summary_records(false)
                .emit_summary_offsets(false)
                .create(std::io::Cursor::new(&mut buffer))
                .expect("writer");
            let schema_id = writer
                .add_schema("demo_schema", "jsonschema", br#"{"type":"object"}"#)
                .expect("schema");
            let channel_id = writer
                .add_channel(schema_id, "/demo", "json", &BTreeMap::new())
                .expect("channel");
            writer
                .write_to_known_channel(
                    &records::MessageHeader {
                        channel_id,
                        sequence: 1,
                        log_time: 10,
                        publish_time: 11,
                    },
                    br#"{"k":"v"}"#,
                )
                .expect("write message");
            writer.finish().expect("finish writer");
            (schema_id, channel_id)
        };

        let parsed = parse_mcap(&buffer).expect("parse mcap");
        assert!(parsed.header.is_some());
        assert!(parsed.channels.contains_key(&channel_id));
        assert!(parsed.schemas.contains_key(&schema_id));
    }

    #[test]
    fn parse_mcap_preserves_missing_summary_schema_id() {
        let mut buffer = Vec::new();
        let (schema_id, channel_id) = {
            let mut writer = mcap::WriteOptions::new()
                .repeat_schemas(false)
                .repeat_channels(true)
                .create(std::io::Cursor::new(&mut buffer))
                .expect("writer");
            let schema_id = writer
                .add_schema("demo_schema", "jsonschema", br#"{"type":"object"}"#)
                .expect("schema");
            let channel_id = writer
                .add_channel(schema_id, "/demo", "json", &BTreeMap::new())
                .expect("channel");
            writer
                .write_to_known_channel(
                    &records::MessageHeader {
                        channel_id,
                        sequence: 1,
                        log_time: 10,
                        publish_time: 11,
                    },
                    br#"{"k":"v"}"#,
                )
                .expect("write message");
            writer.finish().expect("finish writer");
            (schema_id, channel_id)
        };

        let parsed = parse_mcap(&buffer).expect("parse mcap");
        let channel = parsed
            .channels
            .get(&channel_id)
            .expect("channel should be read from summary");
        assert_eq!(channel.schema_id, schema_id);
        assert!(!parsed.schemas.contains_key(&schema_id));
    }

    #[test]
    fn parse_summary_section_accepts_channel_with_missing_schema() {
        let mut buffer = Vec::new();
        let (schema_id, channel_id) = {
            let mut writer = mcap::WriteOptions::new()
                .repeat_schemas(false)
                .repeat_channels(true)
                .create(std::io::Cursor::new(&mut buffer))
                .expect("writer");
            let schema_id = writer
                .add_schema("demo_schema", "jsonschema", br#"{"type":"object"}"#)
                .expect("schema");
            let channel_id = writer
                .add_channel(schema_id, "/demo", "json", &BTreeMap::new())
                .expect("channel");
            writer
                .write_to_known_channel(
                    &records::MessageHeader {
                        channel_id,
                        sequence: 1,
                        log_time: 10,
                        publish_time: 11,
                    },
                    br#"{"k":"v"}"#,
                )
                .expect("write message");
            writer.finish().expect("finish writer");
            (schema_id, channel_id)
        };

        let footer = mcap::read::footer(&buffer).expect("footer");
        let footer_start = buffer.len() - super::FOOTER_RECORD_AND_END_MAGIC_LEN;
        let summary = parse_summary_section(&buffer[footer.summary_start as usize..footer_start])
            .expect("summary should parse without repeated schema");
        let channel = summary
            .channels
            .get(&channel_id)
            .expect("channel should be preserved");
        assert_eq!(channel.id, channel_id);
        assert!(channel.schema.is_none());
        assert!(!summary.schemas.contains_key(&schema_id));
    }

    #[test]
    fn parse_mcap_from_summary_accepts_empty_summary() {
        let mut buffer = Vec::new();
        {
            let mut writer = mcap::Writer::new(std::io::Cursor::new(&mut buffer)).expect("writer");
            writer.finish().expect("finish writer");
        }

        let parsed = parse_mcap_from_summary(&buffer, None).expect("parse from summary");
        assert!(parsed.is_some());
        let parsed = parsed.expect("parsed summary output");
        if let Some(stats) = &parsed.statistics {
            assert_eq!(stats.message_count, 0);
            assert_eq!(stats.channel_count, 0);
            assert_eq!(stats.attachment_count, 0);
            assert_eq!(stats.metadata_count, 0);
        }
        assert!(parsed.channels.is_empty());
        assert!(parsed.schemas.is_empty());
        assert!(parsed.chunk_indexes.is_empty());
        assert!(parsed.attachment_indexes.is_empty());
        assert!(parsed.metadata_indexes.is_empty());
    }
}
