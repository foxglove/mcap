use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{bail, Context as _, Result};
use mcap::records::{self, Record};
use mcap::sans_io::{LinearReadEvent, LinearReader as SansIoReader, LinearReaderOptions};

const FOOTER_RECORD_LEN: usize = 1 + 8 + 8 + 8 + 4;
const RECORD_PREFIX_LEN: u64 = 1 + 8;
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

pub(crate) fn summary_section_has_chunk_indexes(mcap: &[u8]) -> Result<bool> {
    Ok(parse_mcap_from_summary(mcap, None)?
        .is_some_and(|summary| !summary.chunk_indexes.is_empty()))
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
        collect_record(&mut out, record?, None)?;
    }
    Ok(out)
}

fn parse_mcap_linear(mcap: &[u8], header: Option<records::Header>) -> Result<ParsedMcap> {
    let mut out = ParsedMcap {
        header,
        ..ParsedMcap::default()
    };
    scan_top_level_records(mcap, |record, offset, length| {
        if let Record::Chunk { header, data } = record {
            for nested_record in mcap::read::ChunkReader::new(header, data.as_ref())? {
                collect_record(&mut out, nested_record?, None)?;
            }
        } else {
            collect_record(&mut out, record, Some((offset, length)))?;
        }
        Ok(())
    })?;

    Ok(out)
}

pub(crate) fn collect_attachment_indexes_linear(
    mcap: &[u8],
) -> Result<Vec<records::AttachmentIndex>> {
    let mut indexes = Vec::new();
    scan_top_level_records(mcap, |record, offset, length| {
        if let Record::Attachment { header, data, .. } = record {
            indexes.push(records::AttachmentIndex {
                offset,
                length,
                log_time: header.log_time,
                create_time: header.create_time,
                data_size: data.len() as u64,
                name: header.name,
                media_type: header.media_type,
            });
        }
        Ok(())
    })?;
    Ok(indexes)
}

pub(crate) fn collect_metadata_indexes_linear(mcap: &[u8]) -> Result<Vec<records::MetadataIndex>> {
    let mut indexes = Vec::new();
    scan_top_level_records(mcap, |record, offset, length| {
        if let Record::Metadata(metadata) = record {
            indexes.push(records::MetadataIndex {
                offset,
                length,
                name: metadata.name,
            });
        }
        Ok(())
    })?;
    Ok(indexes)
}

pub(crate) fn attachment_indexes_need_scan(parsed: &ParsedMcap) -> bool {
    match &parsed.statistics {
        Some(statistics) => statistics.attachment_count as usize > parsed.attachment_indexes.len(),
        None => parsed.attachment_indexes.is_empty(),
    }
}

pub(crate) fn metadata_indexes_need_scan(parsed: &ParsedMcap) -> bool {
    match &parsed.statistics {
        Some(statistics) => statistics.metadata_count as usize > parsed.metadata_indexes.len(),
        None => parsed.metadata_indexes.is_empty(),
    }
}

pub(crate) fn warn_index_scan(record_kind: &str) {
    eprintln!("Warning: {record_kind} indexes not available; full scan may be slow.");
}

fn scan_top_level_records<F>(mcap: &[u8], mut process: F) -> Result<()>
where
    F: FnMut(Record<'_>, u64, u64) -> Result<()>,
{
    let mut reader = SansIoReader::new_with_options(
        LinearReaderOptions::default()
            .with_emit_chunks(true)
            .with_record_length_limit(mcap.len()),
    );
    let mut remaining = mcap;
    let mut next_record_offset = mcap::MAGIC.len() as u64;

    while let Some(event) = reader.next_event() {
        match event? {
            LinearReadEvent::ReadRequest(need) => {
                let read = need.min(remaining.len());
                let dst = reader.insert(read);
                dst.copy_from_slice(&remaining[..read]);
                reader.notify_read(read);
                remaining = &remaining[read..];
            }
            LinearReadEvent::Record { opcode, data } => {
                let record_offset = next_record_offset;
                let record_length = RECORD_PREFIX_LEN + data.len() as u64;
                next_record_offset += record_length;
                process(
                    mcap::parse_record(opcode, data)?,
                    record_offset,
                    record_length,
                )?;
            }
        }
    }

    Ok(())
}

fn collect_record(
    out: &mut ParsedMcap,
    record: Record<'_>,
    position: Option<(u64, u64)>,
) -> Result<()> {
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
        Record::Attachment { header, data, .. } => {
            if let Some((offset, length)) = position {
                out.attachment_indexes.push(records::AttachmentIndex {
                    offset,
                    length,
                    log_time: header.log_time,
                    create_time: header.create_time,
                    data_size: data.len() as u64,
                    name: header.name,
                    media_type: header.media_type,
                });
            }
        }
        Record::Metadata(metadata) => {
            if let Some((offset, length)) = position {
                out.metadata_indexes.push(records::MetadataIndex {
                    offset,
                    length,
                    name: metadata.name,
                });
            }
        }
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

pub(crate) fn collect_chunk_definitions_from_mcap(
    mcap: &[u8],
    index: &records::ChunkIndex,
    schemas: &mut HashMap<u16, Arc<mcap::Schema<'static>>>,
    channel_defs: &mut HashMap<u16, records::Channel>,
) -> Result<()> {
    let start = usize::try_from(index.chunk_start_offset).with_context(|| {
        format!(
            "chunk offset out of range for this platform: {}",
            index.chunk_start_offset
        )
    })?;
    let length = usize::try_from(index.chunk_length).with_context(|| {
        format!(
            "chunk length out of range for this platform: {}",
            index.chunk_length
        )
    })?;
    let end = start.checked_add(length).ok_or_else(|| {
        anyhow::anyhow!("chunk read overflow at offset {}", index.chunk_start_offset)
    })?;
    let chunk = mcap.get(start..end).ok_or_else(|| {
        anyhow::anyhow!(
            "chunk read out of bounds at offset {} length {}",
            index.chunk_start_offset,
            length
        )
    })?;
    collect_chunk_definitions_from_record_bytes(chunk, schemas, channel_defs)
}

pub(crate) fn collect_chunk_definitions_from_record_bytes(
    chunk: &[u8],
    schemas: &mut HashMap<u16, Arc<mcap::Schema<'static>>>,
    channel_defs: &mut HashMap<u16, records::Channel>,
) -> Result<()> {
    if chunk.len() < 9 || chunk[0] != records::op::CHUNK {
        return Err(mcap::McapError::BadIndex.into());
    }
    let body_len = usize::try_from(u64::from_le_bytes(chunk[1..9].try_into()?))
        .context("chunk body length out of range for this platform")?;
    if chunk.len() != 9 + body_len {
        return Err(mcap::McapError::BadIndex.into());
    }

    let (header, data) = match mcap::parse_record(records::op::CHUNK, &chunk[9..])? {
        Record::Chunk { header, data } => (header, data),
        _ => return Err(mcap::McapError::BadIndex.into()),
    };

    for record in mcap::read::ChunkReader::new(header, data.as_ref())? {
        collect_definition_record(record?, schemas, channel_defs);
    }
    Ok(())
}

fn collect_definition_record(
    record: Record<'_>,
    schemas: &mut HashMap<u16, Arc<mcap::Schema<'static>>>,
    channel_defs: &mut HashMap<u16, records::Channel>,
) {
    match record {
        Record::Schema { header, data } => {
            schemas.entry(header.id).or_insert_with(|| {
                Arc::new(mcap::Schema {
                    id: header.id,
                    name: header.name,
                    encoding: header.encoding,
                    data: Cow::Owned(data.into_owned()),
                })
            });
        }
        Record::Channel(channel) => {
            channel_defs.entry(channel.id).or_insert(channel);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::collections::BTreeMap;

    use super::{
        collect_attachment_indexes_linear, collect_metadata_indexes_linear, parse_mcap,
        parse_mcap_from_summary, parse_summary_section,
    };
    use mcap::records;

    fn write_unindexed_attachment_and_metadata(emit_summary_records: bool) -> Vec<u8> {
        let mut buffer = Vec::new();
        {
            let mut writer = mcap::WriteOptions::new()
                .use_chunks(false)
                .emit_summary_records(emit_summary_records)
                .emit_summary_offsets(emit_summary_records)
                .emit_attachment_indexes(false)
                .emit_metadata_indexes(false)
                .create(std::io::Cursor::new(&mut buffer))
                .expect("writer");
            writer
                .attach(&mcap::Attachment {
                    log_time: 10,
                    create_time: 11,
                    name: "demo.bin".to_string(),
                    media_type: "application/octet-stream".to_string(),
                    data: Cow::Borrowed(b"hello"),
                })
                .expect("attachment");
            writer
                .write_metadata(&records::Metadata {
                    name: "demo".to_string(),
                    metadata: BTreeMap::from([("key".to_string(), "value".to_string())]),
                })
                .expect("metadata");
            writer.finish().expect("finish writer");
        }
        buffer
    }

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
    fn parse_mcap_linear_collects_unindexed_attachment_and_metadata_offsets() {
        let buffer = write_unindexed_attachment_and_metadata(false);
        let parsed = parse_mcap(&buffer).expect("parse mcap");

        assert_eq!(parsed.attachment_indexes.len(), 1);
        assert_eq!(parsed.metadata_indexes.len(), 1);
        let attachment = mcap::read::attachment(&buffer, &parsed.attachment_indexes[0])
            .expect("attachment can be read from synthesized index");
        assert_eq!(attachment.name, "demo.bin");
        assert_eq!(attachment.data.as_ref(), b"hello");
        let metadata = mcap::read::metadata(&buffer, &parsed.metadata_indexes[0])
            .expect("metadata can be read from synthesized index");
        assert_eq!(metadata.name, "demo");
        assert_eq!(metadata.metadata["key"], "value");
    }

    #[test]
    fn collect_linear_indexes_finds_records_when_summary_indexes_are_omitted() {
        let buffer = write_unindexed_attachment_and_metadata(true);
        let parsed = parse_mcap(&buffer).expect("parse mcap");
        assert!(parsed.attachment_indexes.is_empty());
        assert!(parsed.metadata_indexes.is_empty());

        let attachment_indexes =
            collect_attachment_indexes_linear(&buffer).expect("attachment indexes");
        let metadata_indexes = collect_metadata_indexes_linear(&buffer).expect("metadata indexes");
        assert_eq!(attachment_indexes.len(), 1);
        assert_eq!(metadata_indexes.len(), 1);
        assert_eq!(attachment_indexes[0].name, "demo.bin");
        assert_eq!(metadata_indexes[0].name, "demo");
        assert!(mcap::read::attachment(&buffer, &attachment_indexes[0]).is_ok());
        assert!(mcap::read::metadata(&buffer, &metadata_indexes[0]).is_ok());
    }

    #[test]
    fn collect_linear_attachment_indexes_tracks_offsets_after_chunks() {
        let mut buffer = Vec::new();
        {
            let mut writer = mcap::WriteOptions::new()
                .emit_summary_records(false)
                .emit_summary_offsets(false)
                .emit_attachment_indexes(false)
                .create(std::io::Cursor::new(&mut buffer))
                .expect("writer");
            let schema_id = writer.add_schema("demo", "json", b"{}").expect("schema");
            let channel_id = writer
                .add_channel(schema_id, "/demo", "json", &BTreeMap::new())
                .expect("channel");
            writer
                .write_to_known_channel(
                    &records::MessageHeader {
                        channel_id,
                        sequence: 1,
                        log_time: 1,
                        publish_time: 1,
                    },
                    br#"{"k":"v"}"#,
                )
                .expect("message");
            writer
                .attach(&mcap::Attachment {
                    log_time: 10,
                    create_time: 11,
                    name: "after-chunk.bin".to_string(),
                    media_type: "application/octet-stream".to_string(),
                    data: Cow::Borrowed(b"chunk-safe"),
                })
                .expect("attachment");
            writer.finish().expect("finish writer");
        }

        let indexes = collect_attachment_indexes_linear(&buffer).expect("attachment indexes");
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].name, "after-chunk.bin");
        let attachment = mcap::read::attachment(&buffer, &indexes[0]).expect("attachment");
        assert_eq!(attachment.data.as_ref(), b"chunk-safe");
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
