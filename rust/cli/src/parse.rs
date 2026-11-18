use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use anyhow::{bail, Context as _, Result};
use mcap::records::{self, Record};
use mcap::sans_io::{LinearReadEvent, LinearReader as SansIoReader, LinearReaderOptions};

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
    pub summary_available: bool,
    pub statistics: Option<records::Statistics>,
    pub message_count: Option<u64>,
    pub message_start_time: Option<u64>,
    pub message_end_time: Option<u64>,
    pub channel_message_counts: BTreeMap<u16, u64>,
    pub attachment_count: Option<u32>,
    pub metadata_count: Option<u32>,
    pub channels: std::collections::BTreeMap<u16, records::Channel>,
    pub schemas: std::collections::BTreeMap<u16, ParsedSchema>,
    pub chunk_indexes: Vec<records::ChunkIndex>,
    pub attachment_indexes: Vec<records::AttachmentIndex>,
    pub metadata_indexes: Vec<records::MetadataIndex>,
}

pub fn parse_mcap_with_scan_fallback(
    mcap: &[u8],
    scan_without_statistics: bool,
) -> Result<ParsedMcap> {
    let header = read_header(mcap)?;
    if let Some(parsed_from_summary) = parse_mcap_from_summary(mcap, header.clone())? {
        if scan_without_statistics && parsed_from_summary.statistics.is_none() {
            eprintln!(
                "Warning: Statistics record not available; full scan may be slow. Run `mcap doctor` for details."
            );
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

/// What a summary section records about attachments and metadata.
pub(crate) struct SummaryIndexes {
    statistics: Option<records::Statistics>,
    chunk_indexes: Vec<records::ChunkIndex>,
    attachment_indexes: Vec<records::AttachmentIndex>,
    metadata_indexes: Vec<records::MetadataIndex>,
}

/// Reads the statistics and the attachment and metadata indexes from the summary section, or
/// `None` when the file has no summary. Prefers [`SummaryReader`](mcap::sans_io::SummaryReader);
/// a summary it rejects with [`mcap::McapError::UnknownSchema`] is parsed from its raw records
/// instead, since none of these records depends on a schema.
fn read_summary_indexes(
    source: &mut dyn crate::byte_source::ByteSource,
) -> Result<Option<SummaryIndexes>> {
    match crate::byte_source::read_summary(source) {
        Ok(Some(summary)) => Ok(Some(SummaryIndexes {
            statistics: summary.stats,
            chunk_indexes: summary.chunk_indexes,
            attachment_indexes: summary.attachment_indexes,
            metadata_indexes: summary.metadata_indexes,
        })),
        Ok(None) => Ok(None),
        Err(err) if is_unknown_schema_error(&err) => {
            let Some(bytes) = crate::byte_source::read_summary_section(source)? else {
                return Ok(None);
            };
            let parsed = parsed_mcap_from_summary_section(None, &bytes)?;
            Ok(Some(SummaryIndexes {
                statistics: parsed.statistics,
                chunk_indexes: parsed.chunk_indexes,
                attachment_indexes: parsed.attachment_indexes,
                metadata_indexes: parsed.metadata_indexes,
            }))
        }
        Err(err) => Err(err),
    }
}

/// The index record of a top-level record kind: an attachment or a metadata record.
pub(crate) trait TopLevelIndex: Sized {
    /// The record kind, as warnings name it.
    const KIND: &'static str;

    /// The name of the indexed record.
    fn name(&self) -> &str;

    /// Takes the indexes of this kind out of `summary`.
    fn take_from(summary: &mut SummaryIndexes) -> Vec<Self>;

    /// How many records of this kind `statistics` says the file holds.
    fn count_in(statistics: &records::Statistics) -> u64;

    /// Builds the indexes by scanning `source`, stepping over the chunks in `chunk_indexes`.
    fn scan(
        source: &mut dyn crate::byte_source::ByteSource,
        chunk_indexes: &[records::ChunkIndex],
    ) -> Result<Vec<Self>>;
}

impl TopLevelIndex for records::AttachmentIndex {
    const KIND: &'static str = "attachment";

    fn name(&self) -> &str {
        &self.name
    }

    fn take_from(summary: &mut SummaryIndexes) -> Vec<Self> {
        std::mem::take(&mut summary.attachment_indexes)
    }

    fn count_in(statistics: &records::Statistics) -> u64 {
        u64::from(statistics.attachment_count)
    }

    fn scan(
        source: &mut dyn crate::byte_source::ByteSource,
        chunk_indexes: &[records::ChunkIndex],
    ) -> Result<Vec<Self>> {
        collect_attachment_indexes_from_byte_source(source, chunk_indexes)
    }
}

impl TopLevelIndex for records::MetadataIndex {
    const KIND: &'static str = "metadata";

    fn name(&self) -> &str {
        &self.name
    }

    fn take_from(summary: &mut SummaryIndexes) -> Vec<Self> {
        std::mem::take(&mut summary.metadata_indexes)
    }

    fn count_in(statistics: &records::Statistics) -> u64 {
        u64::from(statistics.metadata_count)
    }

    fn scan(
        source: &mut dyn crate::byte_source::ByteSource,
        chunk_indexes: &[records::ChunkIndex],
    ) -> Result<Vec<Self>> {
        collect_metadata_indexes_from_byte_source(source, chunk_indexes)
    }
}

/// Returns the attachment or metadata indexes of `source`: the summary's when it lists every
/// record, and otherwise those found by scanning the top-level records. A scan is announced on
/// stderr with the reason for it. A source with an
/// [indexed-read budget](crate::byte_source::ByteSource::indexed_read_budget) may not be scanned,
/// so needing a scan of one is an error that gives the reason.
///
/// The statistics record says whether the summary lists every record. Without it, a list that
/// is empty or lacks `wanted_name` is scanned for, and a source that may not be scanned keeps
/// the list it has and warns that it may be incomplete.
pub(crate) fn top_level_indexes<T: TopLevelIndex>(
    source: &mut dyn crate::byte_source::ByteSource,
    wanted_name: Option<&str>,
) -> Result<Vec<T>> {
    let Some(mut summary) = read_summary_indexes(source)? else {
        return scan_for_indexes(
            source,
            &[],
            "summary section not available",
            " Run `mcap doctor` for details.",
        );
    };
    let indexes = T::take_from(&mut summary);
    let needs_scan = match &summary.statistics {
        Some(statistics) => T::count_in(statistics) > indexes.len() as u64,
        None => {
            let lacks_wanted = match wanted_name {
                Some(name) => !indexes.iter().any(|index| index.name() == name),
                None => indexes.is_empty(),
            };
            let may_scan = source.indexed_read_budget().is_none();
            if lacks_wanted && !may_scan {
                eprintln!(
                    "Warning: {kind} indexes may be incomplete; pass --allow-remote-scan to scan for {kind} records that have no index.",
                    kind = T::KIND
                );
            }
            lacks_wanted && may_scan
        }
    };
    if !needs_scan {
        return Ok(indexes);
    }
    let reason = format!("{} indexes incomplete or unavailable", T::KIND);
    scan_for_indexes(source, &summary.chunk_indexes, &reason, "")
}

/// Builds the indexes by scanning `source`. `reason` says why the scan is needed: a source
/// that refuses the scan reports it in the error, and otherwise it is a warning on stderr,
/// followed by `advice`.
fn scan_for_indexes<T: TopLevelIndex>(
    source: &mut dyn crate::byte_source::ByteSource,
    chunk_indexes: &[records::ChunkIndex],
    reason: &str,
    advice: &str,
) -> Result<Vec<T>> {
    crate::source::require_remote_scan_for_linear(source, reason)?;
    eprintln!("Warning: {reason}; full scan may be slow.{advice}");
    T::scan(source, chunk_indexes)
}

pub(crate) fn is_unknown_schema_error(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<mcap::McapError>()
            .is_some_and(|e| matches!(e, mcap::McapError::UnknownSchema(_, _)))
    })
}

fn collect_attachment_indexes_from_byte_source(
    source: &mut dyn crate::byte_source::ByteSource,
    chunk_indexes: &[records::ChunkIndex],
) -> Result<Vec<records::AttachmentIndex>> {
    let mut indexes = Vec::new();
    crate::byte_source::for_each_top_level_record_of(
        source,
        records::op::ATTACHMENT,
        chunk_indexes,
        |offset, body| {
            if let Record::Attachment { header, data, .. } =
                mcap::parse_record(records::op::ATTACHMENT, body)?
            {
                indexes.push(records::AttachmentIndex {
                    offset,
                    length: (crate::byte_source::RECORD_PREFIX_LEN + body.len()) as u64,
                    log_time: header.log_time,
                    create_time: header.create_time,
                    data_size: data.len() as u64,
                    name: header.name,
                    media_type: header.media_type,
                });
            }
            Ok(())
        },
    )?;
    Ok(indexes)
}

fn collect_metadata_indexes_from_byte_source(
    source: &mut dyn crate::byte_source::ByteSource,
    chunk_indexes: &[records::ChunkIndex],
) -> Result<Vec<records::MetadataIndex>> {
    let mut indexes = Vec::new();
    crate::byte_source::for_each_top_level_record_of(
        source,
        records::op::METADATA,
        chunk_indexes,
        |offset, body| {
            if let Record::Metadata(metadata) = mcap::parse_record(records::op::METADATA, body)? {
                indexes.push(records::MetadataIndex {
                    offset,
                    length: (crate::byte_source::RECORD_PREFIX_LEN + body.len()) as u64,
                    name: metadata.name,
                });
            }
            Ok(())
        },
    )?;
    Ok(indexes)
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
                let record_length =
                    crate::byte_source::RECORD_PREFIX_LEN as u64 + data.len() as u64;
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

/// The payload borrows from `bytes`; nothing is copied.
pub(crate) fn parse_attachment_record(bytes: &[u8]) -> Result<mcap::Attachment<'_>> {
    let mut reader = mcap::read::LinearReader::sans_magic(bytes);
    let attachment = match reader.next().ok_or(mcap::McapError::BadIndex)?? {
        mcap::records::Record::Attachment { header, data, .. } => mcap::Attachment {
            log_time: header.log_time,
            create_time: header.create_time,
            name: header.name,
            media_type: header.media_type,
            data,
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
        collect_attachment_indexes_from_byte_source, collect_metadata_indexes_from_byte_source,
        parse_mcap_from_summary, parse_mcap_with_scan_fallback, parse_summary_section,
        top_level_indexes, ChunkDefinitions, TopLevelIndex,
    };
    use crate::byte_source::{InstrumentedSource, MemorySource};
    use mcap::records;
    use mcap::records::{AttachmentIndex, MetadataIndex};

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

    /// One message, one attachment named `demo.bin` and one metadata record named `demo`.
    fn attachment_and_metadata_mcap(options: mcap::WriteOptions) -> Vec<u8> {
        let mut buffer = Vec::new();
        {
            let mut writer = options
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

    /// A summary with no statistics record and no attachment or metadata indexes.
    fn summary_without_statistics_or_indexes() -> mcap::WriteOptions {
        mcap::WriteOptions::new()
            .emit_statistics(false)
            .emit_attachment_indexes(false)
            .emit_metadata_indexes(false)
    }

    fn summaryless() -> mcap::WriteOptions {
        mcap::WriteOptions::new()
            .emit_summary_records(false)
            .emit_summary_offsets(false)
    }

    /// Resolves the indexes of `mcap` and returns them with the byte range of every read made.
    fn indexes_and_reads<T: TopLevelIndex>(
        mcap: Vec<u8>,
        wanted_name: Option<&str>,
    ) -> (Vec<T>, Vec<(u64, u64)>) {
        let mut reads = Vec::new();
        let mut source = InstrumentedSource::new(mcap, |offset, data: &[u8]| {
            reads.push((offset, offset + data.len() as u64));
            Ok(())
        });
        let indexes = top_level_indexes(&mut source, wanted_name).expect("indexes");
        drop(source);
        (indexes, reads)
    }

    /// Number of reads whose range contains `byte_offset`.
    fn reads_covering(reads: &[(u64, u64)], byte_offset: u64) -> usize {
        reads
            .iter()
            .filter(|(start, end)| *start <= byte_offset && byte_offset < *end)
            .count()
    }

    #[test]
    fn top_level_indexes_come_from_a_complete_summary_without_other_reads() {
        // The statistics confirm the summary lists every record, so a missing name is simply
        // absent. Neither the head of the file nor the indexed records may be read.
        let mcap = attachment_and_metadata_mcap(mcap::WriteOptions::new());

        let (attachments, reads) =
            indexes_and_reads::<AttachmentIndex>(mcap.clone(), Some("missing"));
        assert_eq!(attachments.len(), 1);
        assert_eq!(attachments[0].name, "demo.bin");
        assert_eq!(reads_covering(&reads, 0), 0, "{reads:?}");
        assert_eq!(
            reads_covering(&reads, attachments[0].offset),
            0,
            "{reads:?}"
        );

        let (metadata, reads) = indexes_and_reads::<MetadataIndex>(mcap, Some("missing"));
        assert_eq!(metadata.len(), 1);
        assert_eq!(metadata[0].name, "demo");
        assert_eq!(reads_covering(&reads, 0), 0, "{reads:?}");
        assert_eq!(reads_covering(&reads, metadata[0].offset), 0, "{reads:?}");
    }

    #[test]
    fn top_level_indexes_scan_a_summaryless_file_once() {
        let mcap = attachment_and_metadata_mcap(summaryless());

        let (attachments, reads) =
            indexes_and_reads::<AttachmentIndex>(mcap.clone(), Some("missing"));
        assert_eq!(attachments.len(), 1);
        assert_eq!(
            reads_covering(&reads, attachments[0].offset),
            1,
            "{reads:?}"
        );

        let (metadata, reads) = indexes_and_reads::<MetadataIndex>(mcap, Some("missing"));
        assert_eq!(metadata.len(), 1);
        assert_eq!(reads_covering(&reads, metadata[0].offset), 1, "{reads:?}");
    }

    #[test]
    fn top_level_indexes_scan_when_a_summary_without_statistics_lacks_what_is_wanted() {
        // No statistics and an empty list: the records may exist without being indexed.
        let unindexed = attachment_and_metadata_mcap(summary_without_statistics_or_indexes());
        let (attachments, _) = indexes_and_reads::<AttachmentIndex>(unindexed.clone(), None);
        assert_eq!(attachments.len(), 1);
        let (metadata, _) = indexes_and_reads::<MetadataIndex>(unindexed, None);
        assert_eq!(metadata.len(), 1);

        // No statistics and a list that has the wanted name: the list is used as it is. A name
        // it lacks may belong to a record that was not indexed.
        let indexed =
            attachment_and_metadata_mcap(mcap::WriteOptions::new().emit_statistics(false));
        let (attachments, reads) =
            indexes_and_reads::<AttachmentIndex>(indexed.clone(), Some("demo.bin"));
        assert_eq!(
            reads_covering(&reads, attachments[0].offset),
            0,
            "{reads:?}"
        );
        let (attachments, reads) = indexes_and_reads::<AttachmentIndex>(indexed, Some("missing"));
        assert_eq!(
            reads_covering(&reads, attachments[0].offset),
            1,
            "{reads:?}"
        );
    }

    #[test]
    fn top_level_indexes_keep_a_list_of_unknown_completeness_when_scans_are_refused() {
        let mcap = attachment_and_metadata_mcap(summary_without_statistics_or_indexes());
        let mut source = MemorySource::new(mcap).with_indexed_read_budget(1024 * 1024);

        let attachments: Vec<AttachmentIndex> =
            top_level_indexes(&mut source, None).expect("list attachments");
        assert!(attachments.is_empty());
        let metadata: Vec<MetadataIndex> =
            top_level_indexes(&mut source, Some("missing")).expect("get metadata");
        assert!(metadata.is_empty());
    }

    #[test]
    fn top_level_index_scan_skips_chunk_bodies() {
        // One large uncompressed chunk, then an attachment.
        let payload = vec![0x5a; 512 * 1024];
        let mut buffer = Vec::new();
        {
            let mut writer = summaryless()
                .compression(None)
                .create(std::io::Cursor::new(&mut buffer))
                .expect("writer");
            let channel_id = writer
                .add_channel(0, "/demo", "raw", &BTreeMap::new())
                .expect("channel");
            writer
                .write_to_known_channel(
                    &records::MessageHeader {
                        channel_id,
                        sequence: 1,
                        log_time: 1,
                        publish_time: 1,
                    },
                    &payload,
                )
                .expect("message");
            writer
                .attach(&mcap::Attachment {
                    log_time: 10,
                    create_time: 11,
                    name: "after-chunk.bin".to_string(),
                    media_type: "application/octet-stream".to_string(),
                    data: Cow::Borrowed(b"hello"),
                })
                .expect("attachment");
            writer.finish().expect("finish writer");
        }

        let (attachments, reads) = indexes_and_reads::<AttachmentIndex>(buffer.clone(), None);
        assert_eq!(attachments.len(), 1);
        let attachment = mcap::read::attachment(&buffer, &attachments[0]).expect("attachment");
        assert_eq!(attachment.data.as_ref(), b"hello");
        let bytes_read: u64 = reads.iter().map(|(start, end)| end - start).sum();
        assert!(
            bytes_read < payload.len() as u64 / 2,
            "read {bytes_read} bytes: {reads:?}"
        );
    }

    #[test]
    fn top_level_index_scan_steps_over_chunks_the_summary_lists() {
        // Many chunks, then an attachment the summary counts but does not index. The scan reads
        // the header record and the records after the last chunk, and nothing in between.
        let payload = vec![0x5a; 2048];
        let mut buffer = Vec::new();
        {
            let mut writer = mcap::WriteOptions::new()
                .compression(None)
                .chunk_size(Some(4096))
                .emit_attachment_indexes(false)
                .create(std::io::Cursor::new(&mut buffer))
                .expect("writer");
            let channel_id = writer
                .add_channel(0, "/demo", "raw", &BTreeMap::new())
                .expect("channel");
            for sequence in 0..200 {
                writer
                    .write_to_known_channel(
                        &records::MessageHeader {
                            channel_id,
                            sequence,
                            log_time: u64::from(sequence),
                            publish_time: u64::from(sequence),
                        },
                        &payload,
                    )
                    .expect("message");
            }
            writer
                .attach(&mcap::Attachment {
                    log_time: 10,
                    create_time: 11,
                    name: "after-chunks.bin".to_string(),
                    media_type: "application/octet-stream".to_string(),
                    data: Cow::Borrowed(b"hello"),
                })
                .expect("attachment");
            writer.finish().expect("finish writer");
        }

        let (attachments, reads) = indexes_and_reads::<AttachmentIndex>(buffer.clone(), None);
        assert_eq!(attachments.len(), 1);
        let attachment = mcap::read::attachment(&buffer, &attachments[0]).expect("attachment");
        assert_eq!(attachment.data.as_ref(), b"hello");

        // The header record ends well before this offset, and the chunks run up to the
        // attachment.
        let chunks_start = 1024;
        let after_chunks = attachments[0].offset;
        assert!(after_chunks > 100 * 1024, "fixture too small");
        let between: Vec<_> = reads
            .iter()
            .filter(|(start, _)| (chunks_start..after_chunks).contains(start))
            .collect();
        assert!(between.is_empty(), "read inside the chunks: {between:?}");
    }

    #[test]
    fn top_level_indexes_refuse_a_scan_of_a_source_with_a_budget() {
        // The statistics count records the summary does not list, so the list is known to be
        // incomplete.
        let incomplete = attachment_and_metadata_mcap(
            mcap::WriteOptions::new()
                .emit_attachment_indexes(false)
                .emit_metadata_indexes(false),
        );
        let cases = [
            (incomplete, "attachment indexes incomplete or unavailable"),
            (
                attachment_and_metadata_mcap(summaryless()),
                "summary section not available",
            ),
        ];
        for (mcap, reason) in cases {
            let mut source = MemorySource::new(mcap).with_indexed_read_budget(1024 * 1024);
            let err = top_level_indexes::<AttachmentIndex>(&mut source, None)
                .expect_err("scan should be refused");
            let message = err.to_string();
            assert!(message.contains(reason), "{message}");
            assert!(message.contains("requires a full scan"), "{message}");
        }
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

        let parsed = parse_mcap_with_scan_fallback(&buffer, false).expect("parse mcap");
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

        let summary_only =
            parse_mcap_with_scan_fallback(&buffer, false).expect("parse mcap from summary");
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

        let parsed = parse_mcap_with_scan_fallback(&buffer, false).expect("parse mcap");
        assert!(parsed.header.is_some());
        assert!(parsed.channels.contains_key(&channel_id));
        assert!(parsed.schemas.contains_key(&schema_id));
    }

    #[test]
    fn parse_mcap_linear_collects_unindexed_attachment_and_metadata_offsets() {
        let buffer = write_unindexed_attachment_and_metadata(false);
        let parsed = parse_mcap_with_scan_fallback(&buffer, false).expect("parse mcap");

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
        let parsed = parse_mcap_with_scan_fallback(&buffer, false).expect("parse mcap");
        assert!(parsed.attachment_indexes.is_empty());
        assert!(parsed.metadata_indexes.is_empty());

        let attachment_indexes = collect_attachment_indexes_from_byte_source(
            &mut MemorySource::new(buffer.clone()),
            &[],
        )
        .expect("attachment indexes");
        let metadata_indexes =
            collect_metadata_indexes_from_byte_source(&mut MemorySource::new(buffer.clone()), &[])
                .expect("metadata indexes");
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

        let indexes = collect_attachment_indexes_from_byte_source(
            &mut MemorySource::new(buffer.clone()),
            &[],
        )
        .expect("attachment indexes");
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

        let parsed = parse_mcap_with_scan_fallback(&buffer, false).expect("parse mcap");
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
