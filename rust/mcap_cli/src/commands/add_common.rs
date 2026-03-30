use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use anyhow::{bail, Context, Result};
use binrw::prelude::*;
use mcap::records::{self, op, Record};

const FOOTER_RECORD_LEN: u64 = 1 + 8 + 20;
const DATA_END_RECORD_LEN: u64 = 1 + 8 + 4;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AttachmentToAdd {
    pub(crate) log_time: u64,
    pub(crate) create_time: u64,
    pub(crate) name: String,
    pub(crate) media_type: String,
    pub(crate) data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedSchema {
    header: records::SchemaHeader,
    data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct ExistingSummaryData {
    statistics: Option<records::Statistics>,
    channels: BTreeMap<u16, records::Channel>,
    schemas: BTreeMap<u16, ParsedSchema>,
    chunk_indexes: Vec<records::ChunkIndex>,
    attachment_indexes: Vec<records::AttachmentIndex>,
    metadata_indexes: Vec<records::MetadataIndex>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExistingLayout {
    footer: records::Footer,
    old_data_end_offset: u64,
    emit_summary_offsets: bool,
    data_crc_enabled: bool,
    summary_crc_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SummaryBuild {
    bytes: Vec<u8>,
    summary_start: u64,
    summary_offset_start: u64,
}

pub(crate) fn amend_mcap_file(
    file: &Path,
    attachments: &[AttachmentToAdd],
    metadata: &[records::Metadata],
) -> Result<()> {
    let input = fs::read(file).with_context(|| format!("failed to read '{}'", file.display()))?;
    let output = amend_mcap_bytes(&input, attachments, metadata)?;
    fs::write(file, output).with_context(|| format!("failed to write '{}'", file.display()))?;
    Ok(())
}

fn amend_mcap_bytes(
    input: &[u8],
    attachments: &[AttachmentToAdd],
    metadata: &[records::Metadata],
) -> Result<Vec<u8>> {
    let layout = parse_existing_layout(input)?;
    let mut existing_summary = collect_existing_summary(input)?;

    let mut output = Vec::with_capacity(input.len() + 1024);
    output.extend_from_slice(&input[..layout.old_data_end_offset as usize]);

    let mut data_hasher = layout.data_crc_enabled.then(|| {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&output);
        hasher
    });

    let mut new_attachment_indexes = Vec::with_capacity(attachments.len());
    for attachment in attachments {
        let offset = output.len() as u64;
        let record_bytes = build_attachment_record(attachment)?;
        if let Some(hasher) = &mut data_hasher {
            hasher.update(&record_bytes);
        }
        output.extend_from_slice(&record_bytes);
        new_attachment_indexes.push(records::AttachmentIndex {
            offset,
            length: record_bytes.len() as u64,
            log_time: attachment.log_time,
            create_time: attachment.create_time,
            data_size: attachment.data.len() as u64,
            name: attachment.name.clone(),
            media_type: attachment.media_type.clone(),
        });
    }

    let mut new_metadata_indexes = Vec::with_capacity(metadata.len());
    for metadata_record in metadata {
        let offset = output.len() as u64;
        let record_bytes = build_metadata_record(metadata_record)?;
        if let Some(hasher) = &mut data_hasher {
            hasher.update(&record_bytes);
        }
        output.extend_from_slice(&record_bytes);
        new_metadata_indexes.push(records::MetadataIndex {
            offset,
            length: record_bytes.len() as u64,
            name: metadata_record.name.clone(),
        });
    }

    let data_section_crc = data_hasher.map(|hasher| hasher.finalize()).unwrap_or(0);
    append_data_end_record(&mut output, data_section_crc)?;

    if let Some(statistics) = &mut existing_summary.statistics {
        statistics.attachment_count = statistics
            .attachment_count
            .saturating_add(new_attachment_indexes.len() as u32);
        statistics.metadata_count = statistics
            .metadata_count
            .saturating_add(new_metadata_indexes.len() as u32);
    }

    let summary_start = output.len() as u64;
    let summary = build_summary_bytes(
        &existing_summary,
        &new_attachment_indexes,
        &new_metadata_indexes,
        summary_start,
        layout.emit_summary_offsets,
    )?;
    output.extend_from_slice(&summary.bytes);

    let summary_crc = if layout.summary_crc_enabled {
        compute_summary_crc(
            &summary.bytes,
            summary.summary_start,
            summary.summary_offset_start,
        )
    } else {
        0
    };
    append_footer_record(
        &mut output,
        summary.summary_start,
        summary.summary_offset_start,
        summary_crc,
    )?;
    output.extend_from_slice(mcap::MAGIC);
    Ok(output)
}

fn parse_existing_layout(input: &[u8]) -> Result<ExistingLayout> {
    let footer = mcap::read::footer(input).context("failed to read footer")?;
    let footer_start = input
        .len()
        .checked_sub(mcap::MAGIC.len() + FOOTER_RECORD_LEN as usize)
        .context("input is too short to contain a footer")? as u64;
    let old_data_end_offset = if footer.summary_start > 0 {
        footer
            .summary_start
            .checked_sub(DATA_END_RECORD_LEN)
            .context("summary start is before data end record")?
    } else {
        footer_start
            .checked_sub(DATA_END_RECORD_LEN)
            .context("footer starts before data end record")?
    };

    let data_end = parse_data_end(input, old_data_end_offset)?;
    Ok(ExistingLayout {
        emit_summary_offsets: footer.summary_offset_start != 0,
        data_crc_enabled: data_end.data_section_crc != 0,
        summary_crc_enabled: footer.summary_crc != 0,
        old_data_end_offset,
        footer,
    })
}

fn parse_data_end(input: &[u8], offset: u64) -> Result<records::DataEnd> {
    let start = offset as usize;
    let Some(slice) = input.get(start..) else {
        bail!("data end offset out of range: {offset}");
    };
    let mut reader = mcap::read::LinearReader::sans_magic(slice);
    let record = reader
        .next()
        .context("missing data end record")?
        .context("failed to parse data end record")?;
    match record {
        Record::DataEnd(data_end) => Ok(data_end),
        other => bail!(
            "expected data end record at offset {offset}, found opcode {:02x}",
            other.opcode()
        ),
    }
}

fn collect_existing_summary(input: &[u8]) -> Result<ExistingSummaryData> {
    if let Some(summary) = mcap::Summary::read(input).context("failed to read summary")? {
        let mut data = ExistingSummaryData {
            statistics: summary.stats,
            channels: BTreeMap::new(),
            schemas: BTreeMap::new(),
            chunk_indexes: summary.chunk_indexes,
            attachment_indexes: summary.attachment_indexes,
            metadata_indexes: summary.metadata_indexes,
        };

        for schema in summary.schemas.values() {
            let schema = schema.as_ref();
            data.schemas.insert(
                schema.id,
                ParsedSchema {
                    header: records::SchemaHeader {
                        id: schema.id,
                        name: schema.name.clone(),
                        encoding: schema.encoding.clone(),
                    },
                    data: schema.data.clone().into_owned(),
                },
            );
        }
        for channel in summary.channels.values() {
            let channel = channel.as_ref();
            data.channels.insert(
                channel.id,
                records::Channel {
                    id: channel.id,
                    schema_id: channel.schema.as_ref().map(|schema| schema.id).unwrap_or(0),
                    topic: channel.topic.clone(),
                    message_encoding: channel.message_encoding.clone(),
                    metadata: channel.metadata.clone(),
                },
            );
        }
        return Ok(data);
    }

    let mut data = ExistingSummaryData::default();
    for record in mcap::read::LinearReader::new(input).context("failed to scan MCAP records")? {
        let record = record.context("failed to parse MCAP record")?;
        if let Record::Chunk {
            header: chunk_header,
            data: chunk_data,
        } = record
        {
            for nested in mcap::read::ChunkReader::new(chunk_header, chunk_data.as_ref())
                .context("failed to parse chunk records")?
            {
                collect_record(
                    &mut data,
                    nested.context("failed to parse nested chunk record")?,
                )?;
            }
        } else {
            collect_record(&mut data, record)?;
        }
    }
    Ok(data)
}

fn collect_record(data: &mut ExistingSummaryData, record: Record<'_>) -> Result<()> {
    match record {
        Record::Statistics(statistics) => {
            data.statistics = Some(statistics);
        }
        Record::Channel(channel) => {
            if let Some(existing) = data.channels.get(&channel.id) {
                if existing != &channel {
                    bail!("conflicting channel definition for id {}", channel.id);
                }
            } else {
                data.channels.insert(channel.id, channel);
            }
        }
        Record::Schema {
            header,
            data: schema_data,
        } => {
            let schema = ParsedSchema {
                header,
                data: schema_data.into_owned(),
            };
            if let Some(existing) = data.schemas.get(&schema.header.id) {
                if existing != &schema {
                    bail!("conflicting schema definition for id {}", schema.header.id);
                }
            } else {
                data.schemas.insert(schema.header.id, schema);
            }
        }
        Record::ChunkIndex(index) => data.chunk_indexes.push(index),
        Record::AttachmentIndex(index) => data.attachment_indexes.push(index),
        Record::MetadataIndex(index) => data.metadata_indexes.push(index),
        _ => {}
    }
    Ok(())
}

fn build_summary_bytes(
    existing: &ExistingSummaryData,
    new_attachment_indexes: &[records::AttachmentIndex],
    new_metadata_indexes: &[records::MetadataIndex],
    summary_start: u64,
    emit_summary_offsets: bool,
) -> Result<SummaryBuild> {
    let mut bytes = Vec::new();
    let mut summary_offsets = Vec::<records::SummaryOffset>::new();
    let mut wrote_summary_group = false;

    let schemas_group_start = summary_start + bytes.len() as u64;
    let schemas_group_before = bytes.len();
    for schema in existing.schemas.values() {
        append_schema_record(&mut bytes, &schema.header, &schema.data)?;
    }
    if bytes.len() > schemas_group_before {
        wrote_summary_group = true;
        summary_offsets.push(records::SummaryOffset {
            group_opcode: op::SCHEMA,
            group_start: schemas_group_start,
            group_length: (bytes.len() - schemas_group_before) as u64,
        });
    }

    let channels_group_start = summary_start + bytes.len() as u64;
    let channels_group_before = bytes.len();
    for channel in existing.channels.values() {
        append_channel_record(&mut bytes, channel)?;
    }
    if bytes.len() > channels_group_before {
        wrote_summary_group = true;
        summary_offsets.push(records::SummaryOffset {
            group_opcode: op::CHANNEL,
            group_start: channels_group_start,
            group_length: (bytes.len() - channels_group_before) as u64,
        });
    }

    if let Some(statistics) = &existing.statistics {
        let stats_group_start = summary_start + bytes.len() as u64;
        let stats_group_before = bytes.len();
        append_statistics_record(&mut bytes, statistics)?;
        wrote_summary_group = true;
        summary_offsets.push(records::SummaryOffset {
            group_opcode: op::STATISTICS,
            group_start: stats_group_start,
            group_length: (bytes.len() - stats_group_before) as u64,
        });
    }

    let chunk_group_start = summary_start + bytes.len() as u64;
    let chunk_group_before = bytes.len();
    for chunk_index in &existing.chunk_indexes {
        append_chunk_index_record(&mut bytes, chunk_index)?;
    }
    if bytes.len() > chunk_group_before {
        wrote_summary_group = true;
        summary_offsets.push(records::SummaryOffset {
            group_opcode: op::CHUNK_INDEX,
            group_start: chunk_group_start,
            group_length: (bytes.len() - chunk_group_before) as u64,
        });
    }

    let mut attachment_indexes = existing.attachment_indexes.clone();
    attachment_indexes.extend_from_slice(new_attachment_indexes);
    attachment_indexes.sort_by_key(|index| index.offset);
    let attachment_group_start = summary_start + bytes.len() as u64;
    let attachment_group_before = bytes.len();
    for attachment_index in &attachment_indexes {
        append_attachment_index_record(&mut bytes, attachment_index)?;
    }
    if bytes.len() > attachment_group_before {
        wrote_summary_group = true;
        summary_offsets.push(records::SummaryOffset {
            group_opcode: op::ATTACHMENT_INDEX,
            group_start: attachment_group_start,
            group_length: (bytes.len() - attachment_group_before) as u64,
        });
    }

    let mut metadata_indexes = existing.metadata_indexes.clone();
    metadata_indexes.extend_from_slice(new_metadata_indexes);
    metadata_indexes.sort_by_key(|index| index.offset);
    let metadata_group_start = summary_start + bytes.len() as u64;
    let metadata_group_before = bytes.len();
    for metadata_index in &metadata_indexes {
        append_metadata_index_record(&mut bytes, metadata_index)?;
    }
    if bytes.len() > metadata_group_before {
        wrote_summary_group = true;
        summary_offsets.push(records::SummaryOffset {
            group_opcode: op::METADATA_INDEX,
            group_start: metadata_group_start,
            group_length: (bytes.len() - metadata_group_before) as u64,
        });
    }

    let mut summary_offset_start = 0;
    if emit_summary_offsets && !summary_offsets.is_empty() {
        summary_offset_start = summary_start + bytes.len() as u64;
        for summary_offset in &summary_offsets {
            append_summary_offset_record(&mut bytes, summary_offset)?;
        }
    }

    Ok(SummaryBuild {
        bytes,
        summary_start: if wrote_summary_group {
            summary_start
        } else {
            0
        },
        summary_offset_start,
    })
}

fn compute_summary_crc(summary_bytes: &[u8], summary_start: u64, summary_offset_start: u64) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(summary_bytes);

    let mut footer_prefix = Vec::with_capacity(1 + 8 + 8 + 8);
    footer_prefix.push(op::FOOTER);
    footer_prefix.extend_from_slice(&20u64.to_le_bytes());
    footer_prefix.extend_from_slice(&summary_start.to_le_bytes());
    footer_prefix.extend_from_slice(&summary_offset_start.to_le_bytes());
    hasher.update(&footer_prefix);
    hasher.finalize()
}

fn build_attachment_record(attachment: &AttachmentToAdd) -> Result<Vec<u8>> {
    let header = records::AttachmentHeader {
        log_time: attachment.log_time,
        create_time: attachment.create_time,
        name: attachment.name.clone(),
        media_type: attachment.media_type.clone(),
    };
    let header_bytes = serialize_binrw(&header)?;
    let mut body = Vec::with_capacity(header_bytes.len() + 8 + attachment.data.len() + 4);
    body.extend_from_slice(&header_bytes);
    body.extend_from_slice(&(attachment.data.len() as u64).to_le_bytes());
    body.extend_from_slice(&attachment.data);
    let crc = crc32fast::hash(&body);
    body.extend_from_slice(&crc.to_le_bytes());

    let mut record = Vec::with_capacity(1 + 8 + body.len());
    append_record(&mut record, op::ATTACHMENT, &body);
    Ok(record)
}

fn build_metadata_record(metadata: &records::Metadata) -> Result<Vec<u8>> {
    let body = serialize_binrw(metadata)?;
    let mut record = Vec::with_capacity(1 + 8 + body.len());
    append_record(&mut record, op::METADATA, &body);
    Ok(record)
}

fn append_schema_record(
    output: &mut Vec<u8>,
    header: &records::SchemaHeader,
    schema_data: &[u8],
) -> Result<()> {
    let mut body = serialize_binrw(header)?;
    body.extend_from_slice(&(schema_data.len() as u32).to_le_bytes());
    body.extend_from_slice(schema_data);
    append_record(output, op::SCHEMA, &body);
    Ok(())
}

fn append_channel_record(output: &mut Vec<u8>, channel: &records::Channel) -> Result<()> {
    let body = serialize_binrw(channel)?;
    append_record(output, op::CHANNEL, &body);
    Ok(())
}

fn append_chunk_index_record(output: &mut Vec<u8>, index: &records::ChunkIndex) -> Result<()> {
    let body = serialize_binrw(index)?;
    append_record(output, op::CHUNK_INDEX, &body);
    Ok(())
}

fn append_attachment_index_record(
    output: &mut Vec<u8>,
    index: &records::AttachmentIndex,
) -> Result<()> {
    let body = serialize_binrw(index)?;
    append_record(output, op::ATTACHMENT_INDEX, &body);
    Ok(())
}

fn append_metadata_index_record(
    output: &mut Vec<u8>,
    index: &records::MetadataIndex,
) -> Result<()> {
    let body = serialize_binrw(index)?;
    append_record(output, op::METADATA_INDEX, &body);
    Ok(())
}

fn append_statistics_record(output: &mut Vec<u8>, statistics: &records::Statistics) -> Result<()> {
    let body = serialize_binrw(statistics)?;
    append_record(output, op::STATISTICS, &body);
    Ok(())
}

fn append_summary_offset_record(
    output: &mut Vec<u8>,
    summary_offset: &records::SummaryOffset,
) -> Result<()> {
    let body = serialize_binrw(summary_offset)?;
    append_record(output, op::SUMMARY_OFFSET, &body);
    Ok(())
}

fn append_data_end_record(output: &mut Vec<u8>, crc: u32) -> Result<()> {
    let body = serialize_binrw(&records::DataEnd {
        data_section_crc: crc,
    })?;
    append_record(output, op::DATA_END, &body);
    Ok(())
}

fn append_footer_record(
    output: &mut Vec<u8>,
    summary_start: u64,
    summary_offset_start: u64,
    summary_crc: u32,
) -> Result<()> {
    let body = serialize_binrw(&records::Footer {
        summary_start,
        summary_offset_start,
        summary_crc,
    })?;
    append_record(output, op::FOOTER, &body);
    Ok(())
}

fn serialize_binrw<T>(value: &T) -> Result<Vec<u8>>
where
    T: for<'a> BinWrite<Args<'a> = ()>,
{
    let mut bytes = Vec::new();
    std::io::Cursor::new(&mut bytes)
        .write_le(value)
        .context("failed to serialize record")?;
    Ok(bytes)
}

fn append_record(output: &mut Vec<u8>, opcode: u8, body: &[u8]) {
    output.push(opcode);
    output.extend_from_slice(&(body.len() as u64).to_le_bytes());
    output.extend_from_slice(body);
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::Cursor;

    use super::{
        amend_mcap_bytes, parse_data_end, AttachmentToAdd, DATA_END_RECORD_LEN, FOOTER_RECORD_LEN,
    };
    use anyhow::Result;
    use mcap::records::{self, MessageHeader};

    fn make_input_mcap(
        data_crc: bool,
        summary_crc: bool,
        emit_summary_offsets: bool,
        emit_summary_records: bool,
    ) -> Result<Vec<u8>> {
        let cursor = Cursor::new(Vec::new());
        let mut writer = mcap::WriteOptions::new()
            .calculate_data_section_crc(data_crc)
            .calculate_summary_section_crc(summary_crc)
            .emit_summary_offsets(emit_summary_offsets)
            .emit_summary_records(emit_summary_records)
            .create(cursor)?;
        let schema_id = writer.add_schema("demo_schema", "jsonschema", br#"{"type":"object"}"#)?;
        let channel_id = writer.add_channel(schema_id, "/demo", "json", &BTreeMap::new())?;
        writer.write_to_known_channel(
            &MessageHeader {
                channel_id,
                sequence: 1,
                log_time: 10,
                publish_time: 11,
            },
            br#"{"k":"v"}"#,
        )?;
        writer.finish()?;
        Ok(writer.into_inner().into_inner())
    }

    #[test]
    fn amend_adds_attachment_and_metadata() -> Result<()> {
        let input = make_input_mcap(true, true, true, true)?;
        let output = amend_mcap_bytes(
            &input,
            &[AttachmentToAdd {
                log_time: 100,
                create_time: 200,
                name: "attachment.txt".to_string(),
                media_type: "text/plain".to_string(),
                data: b"hello".to_vec(),
            }],
            &[records::Metadata {
                name: "demo_meta".to_string(),
                metadata: BTreeMap::from([("k".to_string(), "v".to_string())]),
            }],
        )?;

        let summary = mcap::Summary::read(&output)?.expect("summary should exist after amendment");
        assert_eq!(summary.attachment_indexes.len(), 1);
        assert_eq!(summary.metadata_indexes.len(), 1);
        let stats = summary.stats.expect("statistics should exist");
        assert_eq!(stats.attachment_count, 1);
        assert_eq!(stats.metadata_count, 1);

        let attachment = mcap::read::attachment(&output, &summary.attachment_indexes[0])?;
        assert_eq!(attachment.name, "attachment.txt");
        assert_eq!(attachment.media_type, "text/plain");
        assert_eq!(attachment.data.as_ref(), b"hello");

        let metadata = mcap::read::metadata(&output, &summary.metadata_indexes[0])?;
        assert_eq!(metadata.name, "demo_meta");
        assert_eq!(metadata.metadata.get("k"), Some(&"v".to_string()));

        let message_count = mcap::MessageStream::new(&output)?.count();
        assert_eq!(message_count, 1);
        Ok(())
    }

    #[test]
    fn amend_preserves_crc_disabled_mode() -> Result<()> {
        let input = make_input_mcap(false, false, true, true)?;
        let output = amend_mcap_bytes(
            &input,
            &[],
            &[records::Metadata {
                name: "demo".to_string(),
                metadata: BTreeMap::new(),
            }],
        )?;
        let footer = mcap::read::footer(&output)?;
        assert_eq!(footer.summary_crc, 0);
        let data_end_offset = if footer.summary_start > 0 {
            footer.summary_start.saturating_sub(DATA_END_RECORD_LEN)
        } else {
            (output.len() as u64)
                .saturating_sub(mcap::MAGIC.len() as u64 + FOOTER_RECORD_LEN + DATA_END_RECORD_LEN)
        };
        let data_end = parse_data_end(&output, data_end_offset)?;
        assert_eq!(data_end.data_section_crc, 0);
        Ok(())
    }

    #[test]
    fn amend_preserves_summary_offsets_disabled_mode() -> Result<()> {
        let input = make_input_mcap(true, true, false, true)?;
        let output = amend_mcap_bytes(
            &input,
            &[],
            &[records::Metadata {
                name: "demo".to_string(),
                metadata: BTreeMap::new(),
            }],
        )?;
        let footer = mcap::read::footer(&output)?;
        assert_eq!(footer.summary_offset_start, 0);
        Ok(())
    }

    #[test]
    fn amend_works_for_summaryless_input() -> Result<()> {
        let input = make_input_mcap(true, true, false, false)?;
        assert!(mcap::Summary::read(&input)?.is_none());

        let output = amend_mcap_bytes(
            &input,
            &[],
            &[records::Metadata {
                name: "demo".to_string(),
                metadata: BTreeMap::from([("k".to_string(), "v".to_string())]),
            }],
        )?;

        let summary = mcap::Summary::read(&output)?.expect("summary should be rebuilt");
        assert_eq!(summary.metadata_indexes.len(), 1);
        let metadata = mcap::read::metadata(&output, &summary.metadata_indexes[0])?;
        assert_eq!(metadata.name, "demo");
        assert_eq!(metadata.metadata.get("k"), Some(&"v".to_string()));
        Ok(())
    }
}
