use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
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
    pub summary_available: bool,
    pub statistics: Option<records::Statistics>,
    pub message_count: Option<u64>,
    pub message_start_time: Option<u64>,
    pub message_end_time: Option<u64>,
    pub channel_message_counts: BTreeMap<u16, u64>,
    pub channel_message_time_ranges: BTreeMap<u16, (u64, u64)>,
    pub attachment_count: Option<u32>,
    pub metadata_count: Option<u32>,
    pub channels: std::collections::BTreeMap<u16, records::Channel>,
    pub schemas: std::collections::BTreeMap<u16, ParsedSchema>,
    pub chunk_indexes: Vec<records::ChunkIndex>,
    pub attachment_indexes: Vec<records::AttachmentIndex>,
    pub metadata_indexes: Vec<records::MetadataIndex>,
}

pub fn parse_mcap(mcap: &[u8]) -> Result<ParsedMcap> {
    parse_mcap_with_scan_fallback(mcap, false)
}

pub fn parse_mcap_with_scan_fallback(mcap: &[u8], force_scan_data: bool) -> Result<ParsedMcap> {
    let header = read_header(mcap)?;
    if let Some(parsed_from_summary) = parse_mcap_from_summary(mcap, header.clone())? {
        if force_scan_data {
            if parsed_from_summary.statistics.is_none() {
                eprintln!(
                    "Warning: Statistics record not available; full scan may be slow. Run `mcap doctor` for details."
                );
            }
            return parse_mcap_linear(mcap, header);
        }
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
        summary_available: true,
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
        message_count: Some(0),
        attachment_count: Some(0),
        metadata_count: Some(0),
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
    if !parsed.summary_available {
        return false;
    }
    match &parsed.statistics {
        Some(statistics) => statistics.attachment_count as usize > parsed.attachment_indexes.len(),
        None => parsed.attachment_indexes.is_empty(),
    }
}

pub(crate) fn metadata_indexes_need_scan(parsed: &ParsedMcap) -> bool {
    if !parsed.summary_available {
        return false;
    }
    match &parsed.statistics {
        Some(statistics) => statistics.metadata_count as usize > parsed.metadata_indexes.len(),
        None => parsed.metadata_indexes.is_empty(),
    }
}

pub(crate) fn warn_index_scan(record_kind: &str) {
    eprintln!("Warning: {record_kind} indexes incomplete or unavailable; full scan may be slow.");
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
        Record::Message { header, .. } => {
            increment_optional_u64(&mut out.message_count, "message")?;
            increment_map_count(&mut out.channel_message_counts, header.channel_id)?;
            let range = out
                .channel_message_time_ranges
                .entry(header.channel_id)
                .or_insert((header.log_time, header.log_time));
            range.0 = range.0.min(header.log_time);
            range.1 = range.1.max(header.log_time);
            out.message_start_time = Some(
                out.message_start_time
                    .map_or(header.log_time, |start| start.min(header.log_time)),
            );
            out.message_end_time = Some(
                out.message_end_time
                    .map_or(header.log_time, |end| end.max(header.log_time)),
            );
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
            increment_optional_u32(&mut out.attachment_count, "attachment")?;
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
            increment_optional_u32(&mut out.metadata_count, "metadata")?;
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

/// Accumulates a channel's first and last `log_time` from one message index.
pub(crate) fn collect_message_index_time_range(
    ranges: &mut BTreeMap<u16, (u64, u64)>,
    index: &records::MessageIndex,
) -> Result<()> {
    let Some((first, last)) = index.records.iter().map(|entry| entry.log_time).fold(
        None::<(u64, u64)>,
        |range, log_time| match range {
            Some((first, last)) => Some((first.min(log_time), last.max(log_time))),
            None => Some((log_time, log_time)),
        },
    ) else {
        return Ok(());
    };
    let range = ranges.entry(index.channel_id).or_insert((first, last));
    range.0 = range.0.min(first);
    range.1 = range.1.max(last);
    Ok(())
}

/// Parses one complete MessageIndex record, including its opcode and length prefix.
pub(crate) fn parse_message_index_record(bytes: &[u8]) -> Result<records::MessageIndex> {
    let header = bytes
        .get(..9)
        .context("message index record header is truncated")?;
    if header[0] != records::op::MESSAGE_INDEX {
        bail!("expected MessageIndex record");
    }
    let body_len = usize::try_from(u64::from_le_bytes(
        header[1..9].try_into().expect("message index length slice"),
    ))
    .context("message index record length is too large for this platform")?;
    let body = bytes
        .get(9..9 + body_len)
        .context("message index record body is truncated")?;
    match mcap::parse_record(records::op::MESSAGE_INDEX, body)? {
        Record::MessageIndex(index) => Ok(index),
        _ => unreachable!("opcode is checked above"),
    }
}

/// Parses one contiguous MessageIndex record sequence and accumulates channel time ranges.
pub(crate) fn collect_message_indexes_from_bytes(
    bytes: &[u8],
    ranges: &mut BTreeMap<u16, (u64, u64)>,
) -> Result<()> {
    let mut offset = 0usize;
    while offset < bytes.len() {
        let record = bytes
            .get(offset..)
            .context("message index record is truncated")?;
        let index = parse_message_index_record(record)?;
        let record_len = message_index_record_length(record)?;
        offset = offset
            .checked_add(record_len)
            .context("message index record range overflows")?;
        collect_message_index_time_range(ranges, &index)?;
    }
    Ok(())
}

pub(crate) fn message_index_record_length(bytes: &[u8]) -> Result<usize> {
    let length = bytes
        .get(1..9)
        .context("message index record header is truncated")?;
    let body_len = usize::try_from(u64::from_le_bytes(
        length.try_into().expect("message index length slice"),
    ))
    .context("message index record is too large for this platform")?;
    9usize
        .checked_add(body_len)
        .context("message index record length overflows")
}

/// Fills `channel_message_time_ranges` from the message indexes referenced by the summary.
///
/// This reads only the index records after each chunk. It does not decompress or scan chunk data.
pub(crate) fn collect_channel_time_ranges_from_indexes(
    mcap: &[u8],
    parsed: &mut ParsedMcap,
) -> Result<()> {
    let mut exact_ranges = BTreeMap::new();
    for chunk in &parsed.chunk_indexes {
        let Some(first_offset) = chunk.message_index_offsets.values().min().copied() else {
            continue;
        };
        if chunk.message_index_length == 0 {
            continue;
        }
        let start = usize::try_from(first_offset).context("message index offset is too large")?;
        let length = usize::try_from(chunk.message_index_length)
            .context("message index length is too large")?;
        let end = start
            .checked_add(length)
            .context("message index range overflows")?;
        let bytes = mcap
            .get(start..end)
            .context("message index range is outside the input")?;
        match collect_message_indexes_from_bytes(bytes, &mut exact_ranges) {
            Ok(()) => {}
            Err(_) => {
                break;
            }
        }
    }
    for (channel_id, range) in exact_ranges {
        parsed.channel_message_time_ranges.insert(channel_id, range);
    }
    Ok(())
}

/// Adds a conservative channel time range inferred from chunk indexes that claim the channel.
///
/// Message indexes are in the data section rather than the summary, so this keeps `mcap info`
/// summary-only while avoiding the whole-file denominator when chunk metadata can narrow it.
pub(crate) fn add_chunk_index_time_ranges(parsed: &mut ParsedMcap) {
    let exact_ranges = parsed.channel_message_time_ranges.clone();
    for chunk in &parsed.chunk_indexes {
        for channel_id in chunk.message_index_offsets.keys() {
            let (start, end) = (chunk.message_start_time, chunk.message_end_time);
            parsed
                .channel_message_time_ranges
                .entry(*channel_id)
                .or_insert((start, end));
        }
    }
    parsed.channel_message_time_ranges.extend(exact_ranges);
}

fn increment_optional_u64(count: &mut Option<u64>, label: &str) -> Result<()> {
    *count = Some(
        count
            .unwrap_or_default()
            .checked_add(1)
            .with_context(|| format!("{label} count exceeds u64::MAX"))?,
    );
    Ok(())
}

fn increment_optional_u32(count: &mut Option<u32>, label: &str) -> Result<()> {
    *count = Some(
        count
            .unwrap_or_default()
            .checked_add(1)
            .with_context(|| format!("{label} count exceeds u32::MAX"))?,
    );
    Ok(())
}

fn increment_map_count(counts: &mut BTreeMap<u16, u64>, channel_id: u16) -> Result<()> {
    let count = counts.entry(channel_id).or_default();
    *count = count
        .checked_add(1)
        .with_context(|| format!("message count for channel {channel_id} exceeds u64::MAX"))?;
    Ok(())
}

/// A summary parsed from its raw section, plus the topics it names but cannot represent.
pub(crate) struct ParsedSummary {
    pub(crate) summary: mcap::Summary,
    /// Channels the summary repeated without their schema, by id. They are left out of
    /// `summary.channels` so the in-chunk definitions define them, but they still name real
    /// topics (CSV export reports a topic with no messages in range differently from one that
    /// does not exist), and their ids let a reader tell them from chunk-local channels.
    pub(crate) omitted_channels: BTreeMap<u16, String>,
}

// TODO: keep this in sync with mcap::sans_io::SummaryReader and mcap::read::ChannelAccumulator.
// A future mcap crate range-summary API should replace this CLI-local parser.
pub(crate) fn parse_summary_section(summary: &[u8]) -> Result<ParsedSummary> {
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
    // A summary may repeat a channel without its schema (defined only inside a chunk). Omit such a
    // channel rather than inventing a schemaless one: a channel missing from the summary makes
    // readers collect the in-chunk definitions, which link it. Keep its topic, which is still real.
    let mut omitted_channels = BTreeMap::new();
    for channel in channel_defs.into_values() {
        let schema = if channel.schema_id == 0 {
            None
        } else {
            match schemas.get(&channel.schema_id) {
                Some(schema) => Some(schema.clone()),
                None => {
                    omitted_channels.insert(channel.id, channel.topic);
                    continue;
                }
            }
        };
        out.channels.insert(
            channel.id,
            Arc::new(mcap::Channel {
                id: channel.id,
                topic: channel.topic,
                schema,
                message_encoding: channel.message_encoding,
                metadata: channel.metadata,
            }),
        );
    }
    out.schemas = schemas;
    Ok(ParsedSummary {
        summary: out,
        omitted_channels,
    })
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

/// Context for errors from linking schema and channel records. The crate's errors name the record
/// but not what to do about it.
pub(crate) const INVALID_DEFINITION_HINT: &str = "invalid schema or channel record; `mcap doctor` \
    lists them and `mcap recover` writes a copy without them";

/// Turns a message's channel lookup into the channel, reporting a missing one with the crate's
/// `UnknownChannel` error so cat's and filter's read paths describe the condition the same way.
pub(crate) fn message_channel<'a>(
    channel: Option<Arc<mcap::Channel<'a>>>,
    channel_id: u16,
    sequence: u32,
) -> Result<Arc<mcap::Channel<'a>>> {
    channel.ok_or_else(|| mcap::McapError::UnknownChannel(sequence, channel_id).into())
}

/// Collects the schema and channel definitions stored inside chunks, for a summary that does not
/// repeat them. Schemas are recorded as each chunk is read, but channels are linked only by
/// [`Self::finish`], after every chunk: chunk indexes need not be listed in data order, so a
/// channel's schema may sit in a chunk listed later.
pub(crate) struct ChunkDefinitions<'c> {
    channels: &'c mut mcap::read::ChannelAccumulator<'static>,
    /// Keyed by id so a writer that repeats its channels in every chunk holds each once.
    pending_channels: BTreeMap<u16, records::Channel>,
}

impl<'c> ChunkDefinitions<'c> {
    pub(crate) fn new(channels: &'c mut mcap::read::ChannelAccumulator<'static>) -> Self {
        Self {
            channels,
            pending_channels: BTreeMap::new(),
        }
    }

    /// Collects from the chunk that `index` locates within an in-memory file.
    pub(crate) fn collect_from_mcap(
        &mut self,
        mcap: &[u8],
        index: &records::ChunkIndex,
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
        self.collect_from_record_bytes(chunk)
    }

    /// Collects from one complete chunk record (opcode, length, and body).
    pub(crate) fn collect_from_record_bytes(&mut self, chunk: &[u8]) -> Result<()> {
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
            match record? {
                // ChunkReader hands out owned records, so this moves the bytes rather than
                // copying them.
                Record::Schema { header, data } => {
                    self.channels
                        .add_schema(header, Cow::Owned(data.into_owned()))
                        .context(INVALID_DEFINITION_HINT)?;
                }
                // A chunk's repeat must match the first copy, as the accumulator requires.
                Record::Channel(channel) => match self.pending_channels.entry(channel.id) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(channel);
                    }
                    std::collections::btree_map::Entry::Occupied(entry) => {
                        if entry.get() != &channel {
                            return Err(mcap::McapError::ConflictingChannels(channel.topic))
                                .context(INVALID_DEFINITION_HINT);
                        }
                    }
                },
                _ => {}
            }
        }
        Ok(())
    }

    /// Topics of the channels collected so far, available before [`Self::finish`] links them.
    pub(crate) fn topics(&self) -> impl Iterator<Item = &str> {
        self.pending_channels
            .values()
            .map(|channel| channel.topic.as_str())
    }

    /// Links every collected channel against the schemas seen in all chunks.
    pub(crate) fn finish(self) -> Result<()> {
        for channel in self.pending_channels.into_values() {
            self.channels
                .add_channel(channel)
                .context(INVALID_DEFINITION_HINT)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::collections::BTreeMap;

    use super::{
        add_chunk_index_time_ranges, collect_attachment_indexes_linear,
        collect_channel_time_ranges_from_indexes, collect_message_index_time_range,
        collect_metadata_indexes_linear, parse_mcap, parse_mcap_from_summary,
        parse_mcap_with_scan_fallback, parse_summary_section, ChunkDefinitions, ParsedMcap,
    };
    use mcap::records;

    fn record(opcode: u8, body: &[u8]) -> Vec<u8> {
        let mut out = vec![opcode];
        out.extend_from_slice(&(body.len() as u64).to_le_bytes());
        out.extend_from_slice(body);
        out
    }

    fn push_string(out: &mut Vec<u8>, value: &str) {
        out.extend_from_slice(&(value.len() as u32).to_le_bytes());
        out.extend_from_slice(value.as_bytes());
    }

    fn schema_record(id: u16, name: &str, data: &[u8]) -> Vec<u8> {
        let mut body = id.to_le_bytes().to_vec();
        push_string(&mut body, name);
        push_string(&mut body, "jsonschema");
        body.extend_from_slice(&(data.len() as u32).to_le_bytes());
        body.extend_from_slice(data);
        record(records::op::SCHEMA, &body)
    }

    fn channel_record(id: u16, schema_id: u16, topic: &str) -> Vec<u8> {
        let mut body = id.to_le_bytes().to_vec();
        body.extend_from_slice(&schema_id.to_le_bytes());
        push_string(&mut body, topic);
        push_string(&mut body, "json");
        body.extend_from_slice(&0u32.to_le_bytes()); // empty metadata map
        record(records::op::CHANNEL, &body)
    }

    /// An uncompressed chunk record holding `records`. A zero CRC skips validation.
    fn chunk_record(records: &[u8]) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&0u64.to_le_bytes()); // message_start_time
        body.extend_from_slice(&0u64.to_le_bytes()); // message_end_time
        body.extend_from_slice(&(records.len() as u64).to_le_bytes()); // uncompressed_size
        body.extend_from_slice(&0u32.to_le_bytes()); // uncompressed_crc
        push_string(&mut body, ""); // compression
        body.extend_from_slice(&(records.len() as u64).to_le_bytes()); // compressed_size
        body.extend_from_slice(records);
        record(records::op::CHUNK, &body)
    }

    #[test]
    fn chunk_definitions_link_channels_after_every_chunk() {
        // The chunk listed first holds only the channel; its schema is in the chunk listed second.
        let channel_chunk = chunk_record(&channel_record(1, 7, "/demo"));
        let schema_chunk = chunk_record(&schema_record(7, "Example", b"{}"));

        let mut channels = mcap::read::ChannelAccumulator::default();
        let mut definitions = ChunkDefinitions::new(&mut channels);
        definitions
            .collect_from_record_bytes(&channel_chunk)
            .expect("a channel before its schema is deferred, not rejected");
        definitions
            .collect_from_record_bytes(&schema_chunk)
            .expect("schema-only chunk");
        definitions
            .finish()
            .expect("the channel links to the schema seen later");

        let channel = channels.get(1).expect("channel 1 is defined");
        assert_eq!(channel.topic, "/demo");
        assert_eq!(
            channel.schema.as_ref().map(|schema| schema.name.as_str()),
            Some("Example")
        );
    }

    #[test]
    fn chunk_definitions_hold_a_repeated_channel_once_and_reject_a_differing_repeat() {
        let mut definitions_chunk = schema_record(7, "Example", b"{}");
        definitions_chunk.extend(channel_record(1, 7, "/demo"));
        let first_chunk = chunk_record(&definitions_chunk);
        let repeat_chunk = chunk_record(&channel_record(1, 7, "/demo"));
        let renamed_chunk = chunk_record(&channel_record(1, 7, "/renamed"));

        let mut channels = mcap::read::ChannelAccumulator::default();
        let mut definitions = ChunkDefinitions::new(&mut channels);
        definitions
            .collect_from_record_bytes(&first_chunk)
            .expect("first chunk");
        definitions
            .collect_from_record_bytes(&repeat_chunk)
            .expect("an identical repeat is accepted");
        assert_eq!(definitions.topics().collect::<Vec<_>>(), ["/demo"]);
        let err = definitions
            .collect_from_record_bytes(&renamed_chunk)
            .expect_err("a differing repeat is rejected");
        assert!(matches!(
            err.downcast_ref::<mcap::McapError>(),
            Some(mcap::McapError::ConflictingChannels(topic)) if topic == "/renamed"
        ));
    }

    #[test]
    fn chunk_definitions_reject_channel_whose_schema_is_in_no_chunk() {
        let channel_chunk = chunk_record(&channel_record(1, 7, "/demo"));

        let mut channels = mcap::read::ChannelAccumulator::default();
        let mut definitions = ChunkDefinitions::new(&mut channels);
        definitions
            .collect_from_record_bytes(&channel_chunk)
            .expect("collection defers linking");
        let err = definitions.finish().expect_err("schema 7 never appears");
        assert!(matches!(
            err.downcast_ref::<mcap::McapError>(),
            Some(mcap::McapError::UnknownSchema(topic, 7)) if topic == "/demo"
        ));
    }

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
    fn parse_mcap_scan_fallback_counts_records_when_summary_lacks_statistics() {
        let mut buffer = Vec::new();
        let channel_id = {
            let mut writer = mcap::WriteOptions::new()
                .emit_statistics(false)
                .create(std::io::Cursor::new(&mut buffer))
                .expect("writer");
            let schema_id = writer
                .add_schema("demo_schema", "jsonschema", br#"{"type":"object"}"#)
                .expect("schema");
            let channel_id = writer
                .add_channel(schema_id, "/demo", "json", &BTreeMap::new())
                .expect("channel");
            for (sequence, log_time) in [(1, 10), (2, 40)] {
                writer
                    .write_to_known_channel(
                        &records::MessageHeader {
                            channel_id,
                            sequence,
                            log_time,
                            publish_time: log_time,
                        },
                        br#"{"k":"v"}"#,
                    )
                    .expect("write message");
            }
            writer.finish().expect("finish writer");
            channel_id
        };

        let summary_only = parse_mcap(&buffer).expect("parse mcap from summary");
        assert!(summary_only.statistics.is_none());
        assert_eq!(summary_only.message_count, None);

        let parsed =
            parse_mcap_with_scan_fallback(&buffer, true).expect("parse mcap with scan fallback");
        assert!(parsed.statistics.is_none());
        assert_eq!(parsed.message_count, Some(2));
        assert_eq!(parsed.message_start_time, Some(10));
        assert_eq!(parsed.message_end_time, Some(40));
        assert_eq!(
            parsed.channel_message_counts.get(&channel_id).copied(),
            Some(2)
        );
        assert_eq!(
            parsed.channel_message_time_ranges.get(&channel_id),
            Some(&(10, 40))
        );
    }

    #[test]
    fn parse_mcap_scan_fallback_reports_zero_counts() {
        let mut buffer = Vec::new();
        {
            let mut writer = mcap::WriteOptions::new()
                .emit_statistics(false)
                .create(std::io::Cursor::new(&mut buffer))
                .expect("writer");
            writer.finish().expect("finish writer");
        }

        let parsed =
            parse_mcap_with_scan_fallback(&buffer, true).expect("parse mcap with scan fallback");
        assert!(parsed.statistics.is_none());
        assert_eq!(parsed.message_count, Some(0));
        assert_eq!(parsed.attachment_count, Some(0));
        assert_eq!(parsed.metadata_count, Some(0));
    }

    #[test]
    fn parse_message_index_updates_channel_log_time_range() {
        let index = records::MessageIndex {
            channel_id: 7,
            records: vec![
                records::MessageIndexEntry {
                    log_time: 40,
                    offset: 0,
                },
                records::MessageIndexEntry {
                    log_time: 10,
                    offset: 20,
                },
                records::MessageIndexEntry {
                    log_time: 30,
                    offset: 40,
                },
            ],
        };

        let mut ranges = BTreeMap::new();
        collect_message_index_time_range(&mut ranges, &index).expect("collect index range");
        assert_eq!(ranges.get(&7), Some(&(10, 40)));
    }

    #[test]
    fn chunk_index_range_does_not_replace_exact_message_index_range() {
        let mut parsed = ParsedMcap {
            chunk_indexes: vec![records::ChunkIndex {
                message_start_time: 1,
                message_end_time: 1_000,
                chunk_start_offset: 0,
                chunk_length: 0,
                message_index_offsets: BTreeMap::from([(7, 0)]),
                message_index_length: 0,
                compression: String::new(),
                compressed_size: 0,
                uncompressed_size: 0,
            }],
            channel_message_time_ranges: BTreeMap::from([(7, (10, 20))]),
            ..ParsedMcap::default()
        };

        add_chunk_index_time_ranges(&mut parsed);
        assert_eq!(parsed.channel_message_time_ranges.get(&7), Some(&(10, 20)));
    }

    #[test]
    fn message_indexes_extract_distinct_channel_ranges_from_one_chunk() {
        let mut buffer = Vec::new();
        let (early_channel, late_channel) = {
            let mut writer = mcap::WriteOptions::new()
                .compression(None)
                .create(std::io::Cursor::new(&mut buffer))
                .expect("writer");
            let schema_id = writer
                .add_schema("demo_schema", "jsonschema", br#"{"type":"object"}"#)
                .expect("schema");
            let early_channel = writer
                .add_channel(schema_id, "/early", "json", &BTreeMap::new())
                .expect("early channel");
            let late_channel = writer
                .add_channel(schema_id, "/late", "json", &BTreeMap::new())
                .expect("late channel");
            for (channel_id, log_time) in [(early_channel, 10), (late_channel, 1_000)] {
                writer
                    .write_to_known_channel(
                        &records::MessageHeader {
                            channel_id,
                            sequence: 0,
                            log_time,
                            publish_time: log_time,
                        },
                        br#"{"k":"v"}"#,
                    )
                    .expect("write message");
            }
            writer.finish().expect("finish writer");
            (early_channel, late_channel)
        };

        let mut parsed = parse_mcap(&buffer).expect("parse summary");
        assert_eq!(parsed.chunk_indexes.len(), 1);
        let chunk = &parsed.chunk_indexes[0];
        assert_eq!(chunk.message_start_time, 10);
        assert_eq!(chunk.message_end_time, 1_000);
        assert!(parsed.channel_message_time_ranges.is_empty());

        collect_channel_time_ranges_from_indexes(&buffer, &mut parsed).expect("index ranges");
        assert_eq!(
            parsed.channel_message_time_ranges.get(&early_channel),
            Some(&(10, 10))
        );
        assert_eq!(
            parsed.channel_message_time_ranges.get(&late_channel),
            Some(&(1_000, 1_000))
        );
    }

    #[test]
    fn chunk_index_range_fills_channels_without_message_index_data() {
        let mut parsed = ParsedMcap {
            chunk_indexes: vec![records::ChunkIndex {
                message_start_time: 10,
                message_end_time: 1_000,
                chunk_start_offset: 0,
                chunk_length: 0,
                message_index_offsets: BTreeMap::new(),
                message_index_length: 0,
                compression: String::new(),
                compressed_size: 0,
                uncompressed_size: 0,
            }],
            ..ParsedMcap::default()
        };

        add_chunk_index_time_ranges(&mut parsed);
        assert!(parsed.channel_message_time_ranges.is_empty());
    }

    #[test]
    fn parse_mcap_force_scan_replaces_summary_counts_with_scanned_time_ranges() {
        let mut buffer = Vec::new();
        let (channel_id, other_channel_id) = {
            let mut writer = mcap::WriteOptions::new()
                .create(std::io::Cursor::new(&mut buffer))
                .expect("writer");
            let schema_id = writer
                .add_schema("demo_schema", "jsonschema", br#"{"type":"object"}"#)
                .expect("schema");
            let channel_id = writer
                .add_channel(schema_id, "/early", "json", &BTreeMap::new())
                .expect("channel");
            let other_channel_id = writer
                .add_channel(schema_id, "/late", "json", &BTreeMap::new())
                .expect("channel");
            for (channel_id, sequence, log_time) in [
                (channel_id, 1, 10),
                (channel_id, 2, 30),
                (other_channel_id, 1, 1_000),
                (other_channel_id, 2, 1_100),
            ] {
                writer
                    .write_to_known_channel(
                        &records::MessageHeader {
                            channel_id,
                            sequence,
                            log_time,
                            publish_time: log_time,
                        },
                        br#"{"k":"v"}"#,
                    )
                    .expect("write message");
            }
            writer.finish().expect("finish writer");
            (channel_id, other_channel_id)
        };

        let summary_only = parse_mcap(&buffer).expect("parse mcap from summary");
        let statistics = summary_only
            .statistics
            .as_ref()
            .expect("summary should include statistics");
        assert_eq!(statistics.message_start_time, 10);
        assert_eq!(statistics.message_end_time, 1_100);
        assert!(summary_only.channel_message_time_ranges.is_empty());

        let parsed =
            parse_mcap_with_scan_fallback(&buffer, true).expect("parse mcap with forced scan");
        assert!(parsed.statistics.is_some());
        assert_eq!(
            parsed.channel_message_time_ranges.get(&channel_id),
            Some(&(10, 30))
        );
        assert_eq!(
            parsed.channel_message_time_ranges.get(&other_channel_id),
            Some(&(1_000, 1_100))
        );
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
    fn parse_summary_section_omits_channel_with_missing_schema() {
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
        let parsed = parse_summary_section(&buffer[footer.summary_start as usize..footer_start])
            .expect("summary should parse without repeated schema");
        // The channel's schema lives only inside the chunk, so the channel is left for the
        // in-chunk definition pass instead of being reported with no schema; its topic survives.
        let summary = &parsed.summary;
        assert!(!summary.channels.contains_key(&channel_id));
        assert!(!summary.schemas.contains_key(&schema_id));
        assert!(summary
            .chunk_indexes
            .iter()
            .any(|chunk| chunk.message_index_offsets.contains_key(&channel_id)));
        assert_eq!(
            parsed.omitted_channels,
            BTreeMap::from([(channel_id, "/demo".to_string())])
        );
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
