//! Low-level helpers shared by the single-input rewrite pipeline ([`super::engine`]) and the
//! multi-input merge pipeline ([`super::merge`]): input slicing, summary/index inspection, the
//! writer builder, and the index-or-scan traversals for metadata and attachments.
use std::io::{Seek, Write};
use std::sync::Arc;

use anyhow::{bail, Context, Result};

/// Number of bytes per message-index entry (a `(log_time, offset)` pair of `uint64`s).
const MESSAGE_INDEX_ENTRY_SIZE: usize = 16;

/// Output encoding for a rewritten MCAP. Both the single-input and merge pipelines build one of
/// these and hand it to [`create_writer`] so the writer is configured identically.
pub(crate) struct WriterConfig {
    pub(crate) profile: String,
    pub(crate) use_chunks: bool,
    pub(crate) chunk_size: u64,
    pub(crate) compression: Option<mcap::Compression>,
    pub(crate) include_crc: bool,
}

/// Creates the output writer with the CLI's library identity and the requested encoding. Message
/// indexes only accompany chunks, so they are disabled for unchunked output.
pub(crate) fn create_writer<W: Write + Seek>(
    sink: W,
    config: &WriterConfig,
    disable_seeking: bool,
) -> Result<mcap::Writer<W>> {
    let mut write_options = mcap::WriteOptions::new()
        .profile(config.profile.clone())
        .library(crate::cli::LIBRARY_IDENTIFIER.clone())
        .use_chunks(config.use_chunks)
        .chunk_size(Some(config.chunk_size))
        .compression(config.compression)
        .calculate_chunk_crcs(config.include_crc)
        .calculate_data_section_crc(config.include_crc)
        .calculate_summary_section_crc(config.include_crc)
        .calculate_attachment_crcs(config.include_crc)
        .disable_seeking(disable_seeking);

    if !config.use_chunks {
        write_options = write_options.emit_message_indexes(false);
    }

    write_options
        .create(sink)
        .context("failed to create mcap writer")
}

/// A named MCAP input: a display name for diagnostics plus its bytes.
#[derive(Debug, Clone, Copy)]
pub(crate) struct InputRef<'a> {
    pub(crate) name: &'a str,
    pub(crate) data: &'a [u8],
}

/// Reads the leading [`Header`](mcap::records::Header) record, if present. Used to carry the input
/// profile onto the output.
pub(crate) fn read_header(input: &[u8]) -> Result<Option<mcap::records::Header>> {
    let mut reader = mcap::read::LinearReader::new(input)?;
    match reader.next() {
        Some(Ok(mcap::records::Record::Header(header))) => Ok(Some(header)),
        Some(Ok(_)) | None => Ok(None),
        Some(Err(err)) => Err(err.into()),
    }
}

/// The profile to stamp on the output: the shared profile when every input agrees, otherwise empty.
/// For a single input this is just that input's profile.
pub(crate) fn common_profile(inputs: &[InputRef<'_>]) -> Result<String> {
    let mut common: Option<String> = None;
    for input in inputs {
        let profile = read_header(input.data)
            .with_context(|| format!("failed to read header from '{}'", input.name))?
            .map(|header| header.profile)
            .unwrap_or_default();
        match &common {
            None => common = Some(profile),
            Some(existing) if *existing != profile => return Ok(String::new()),
            Some(_) => {}
        }
    }
    Ok(common.unwrap_or_default())
}

fn incomplete_indexed_summary_error() -> anyhow::Error {
    anyhow::anyhow!(
        "chunk-indexed MCAP summary is missing channel or schema records; run `mcap recover` to rewrite the file"
    )
}

/// Reads a summary only when it is usable for indexed reads. Returns `None` when there is no
/// summary, no chunk indexes, or (via [`mcap::McapError::UnknownSchema`]) a summary that does not
/// claim to be chunk-indexed. Returns an error for a summary that claims chunk indexes but is
/// missing the channel/schema records an indexed read needs.
pub(crate) fn read_indexed_summary(input: &[u8]) -> Result<Option<mcap::Summary>> {
    match mcap::Summary::read(input) {
        Ok(Some(summary)) if summary.chunk_indexes.is_empty() => Ok(None),
        Ok(Some(summary)) if summary_supports_indexed_read(&summary) => Ok(Some(summary)),
        Ok(Some(_)) => Err(incomplete_indexed_summary_error()),
        Ok(None) => Ok(None),
        Err(mcap::McapError::UnknownSchema(_, _))
            if !crate::parse::summary_section_has_chunk_indexes(input)? =>
        {
            Ok(None)
        }
        Err(mcap::McapError::UnknownSchema(_, _)) => Err(incomplete_indexed_summary_error()),
        Err(err) => Err(err.into()),
    }
}

/// Whether a summary carries the channel/schema records required to resolve every message an
/// indexed read would surface.
pub(crate) fn summary_supports_indexed_read(summary: &mcap::Summary) -> bool {
    if !summary.chunk_indexes.is_empty() && summary.channels.is_empty() {
        return false;
    }

    if let Some(stats) = &summary.stats {
        if stats.channel_count as usize > summary.channels.len()
            || stats
                .channel_message_counts
                .keys()
                .any(|channel_id| !summary.channels.contains_key(channel_id))
        {
            return false;
        }
    }

    summary
        .chunk_indexes
        .iter()
        .flat_map(|index| index.message_index_offsets.keys())
        .all(|channel_id| summary.channels.contains_key(channel_id))
}

/// Whether every message counted by the statistics record is reachable through the chunk message
/// indexes. Returns `false` when statistics are absent, since completeness can't be proven. A
/// `false` result means an index-only read could miss records, so the caller must use a linear
/// scan. See [`summary_has_unindexed_messages`] for the variant that keeps the fast path for
/// stats-less inputs.
pub(crate) fn summary_indexes_all_messages(input: &[u8], summary: &mcap::Summary) -> bool {
    let Some(stats) = summary.stats.as_ref() else {
        return false;
    };
    indexed_message_count(input, summary) == Some(stats.message_count)
}

/// Whether the summary *proves* at least one message lives outside the chunk message indexes: the
/// statistics count differs from what the indexes cover, or a message index can't be parsed.
/// Returns `false` when statistics are absent, so a well-formed stats-less indexed file keeps the
/// indexed fast path (an index-only read of it is correct and cheaper, and `--last-per-channel`
/// requires it).
pub(crate) fn summary_has_unindexed_messages(input: &[u8], summary: &mcap::Summary) -> bool {
    let Some(stats) = summary.stats.as_ref() else {
        return false;
    };
    match indexed_message_count(input, summary) {
        Some(indexed_messages) => indexed_messages != stats.message_count,
        // An unparsable message index can't be trusted; fall back to a lossless scan.
        None => true,
    }
}

/// Sums the messages reachable through the chunk message indexes, or `None` if a message-index
/// record can't be parsed. Offsets are de-duplicated so a shared message-index record is counted
/// once.
fn indexed_message_count(input: &[u8], summary: &mcap::Summary) -> Option<u64> {
    let mut offsets = std::collections::HashSet::new();
    let mut indexed_messages = 0u64;
    for offset in summary
        .chunk_indexes
        .iter()
        .flat_map(|chunk| chunk.message_index_offsets.values())
    {
        if offsets.insert(*offset) {
            let count = message_index_count(input, *offset).ok()?;
            indexed_messages = indexed_messages.saturating_add(count as u64);
        }
    }
    Some(indexed_messages)
}

fn message_index_count(input: &[u8], offset: u64) -> Result<usize> {
    let start = usize::try_from(offset).with_context(|| {
        format!("message index offset out of range for this platform: {offset}")
    })?;
    let header_end = start
        .checked_add(9)
        .ok_or_else(|| anyhow::anyhow!("message index header overflows at offset {offset}"))?;
    let header = input
        .get(start..header_end)
        .ok_or_else(|| anyhow::anyhow!("message index header out of bounds at offset {offset}"))?;
    let opcode = header[0];
    if opcode != mcap::records::op::MESSAGE_INDEX {
        bail!("expected MessageIndex record at offset {offset}");
    }
    let length = u64::from_le_bytes(header[1..9].try_into().expect("slice has length 8"));
    let length = usize::try_from(length)
        .with_context(|| format!("message index length out of range at offset {offset}"))?;
    let body_start = header_end;
    let body_end = body_start
        .checked_add(length)
        .ok_or_else(|| anyhow::anyhow!("message index length overflows at offset {offset}"))?;
    let body = input
        .get(body_start..body_end)
        .ok_or_else(|| anyhow::anyhow!("message index body out of bounds at offset {offset}"))?;

    if body.len() < 6 {
        bail!("message index body too short at offset {offset}");
    }
    let byte_len = u32::from_le_bytes(body[2..6].try_into().expect("slice has length 4")) as usize;
    if !byte_len.is_multiple_of(MESSAGE_INDEX_ENTRY_SIZE) {
        bail!("message index entries are misaligned at offset {offset}");
    }
    let expected_len = 6usize
        .checked_add(byte_len)
        .ok_or_else(|| anyhow::anyhow!("message index byte length overflows at offset {offset}"))?;
    if body.len() != expected_len {
        bail!("message index length mismatch at offset {offset}");
    }
    Ok(byte_len / MESSAGE_INDEX_ENTRY_SIZE)
}

/// Bounds-checks a `[offset, offset + length)` slice of the input, used to hand chunk bytes to an
/// [`mcap::sans_io::IndexedReader`] on demand.
fn checked_slice(input: &[u8], offset: u64, length: usize) -> Result<&[u8]> {
    let start = usize::try_from(offset)
        .with_context(|| format!("chunk offset out of range for this platform: {offset}"))?;
    let end = start
        .checked_add(length)
        .ok_or_else(|| anyhow::anyhow!("chunk read overflow at offset {offset}"))?;
    input.get(start..end).ok_or_else(|| {
        anyhow::anyhow!("chunk read out of bounds at offset {offset} length {length}")
    })
}

/// Fulfills an [`mcap::sans_io::IndexedReader`] `ReadChunkRequest` by handing the reader the
/// requested slice of the input. Shared by every indexed read loop (single-input and merge).
pub(crate) fn service_chunk_request(
    reader: &mut mcap::sans_io::IndexedReader,
    input: &[u8],
    offset: u64,
    length: usize,
) -> Result<()> {
    let chunk_data = checked_slice(input, offset, length)?;
    reader.insert_chunk_record_data(offset, chunk_data)?;
    Ok(())
}

/// Resolves a channel record against the known schemas into an owned [`mcap::Channel`].
pub(crate) fn build_channel(
    channel: &mcap::records::Channel,
    schemas: &std::collections::HashMap<u16, Arc<mcap::Schema<'static>>>,
) -> Result<Arc<mcap::Channel<'static>>> {
    let schema = if channel.schema_id == 0 {
        None
    } else {
        Some(schemas.get(&channel.schema_id).cloned().ok_or_else(|| {
            anyhow::anyhow!(
                "encountered channel with topic {} with unknown schema ID {}",
                channel.topic,
                channel.schema_id
            )
        })?)
    };

    Ok(Arc::new(mcap::Channel {
        id: channel.id,
        topic: channel.topic.clone(),
        schema,
        message_encoding: channel.message_encoding.clone(),
        metadata: channel.metadata.clone(),
    }))
}

/// Visits every metadata record in the input, preferring the summary index and falling back to a
/// top-level scan when the summary does not index every record (index records are optional, and
/// statistics may be absent), so no records are dropped. Metadata is never inside a chunk, so the
/// scan does not decompress.
pub(crate) fn for_each_metadata<F>(
    input: &[u8],
    summary: Option<&mcap::Summary>,
    mut visit: F,
) -> Result<()>
where
    F: FnMut(mcap::records::Metadata) -> Result<()>,
{
    if let Some(summary) = summary {
        let indexed_count = summary.stats.as_ref().map(|stats| stats.metadata_count);
        if indexed_count == Some(summary.metadata_indexes.len() as u32) {
            let mut indexes = summary.metadata_indexes.clone();
            indexes.sort_by_key(|index| index.offset);
            for index in &indexes {
                let metadata = mcap::read::metadata(input, index).with_context(|| {
                    format!(
                        "failed to read metadata '{}' at offset {}",
                        index.name, index.offset
                    )
                })?;
                visit(metadata)?;
            }
            return Ok(());
        }
    }

    for record in mcap::read::LinearReader::new(input)? {
        if let mcap::records::Record::Metadata(metadata) = record? {
            visit(metadata)?;
        }
    }
    Ok(())
}

/// Visits every attachment in the input, using the same index-or-scan strategy as
/// [`for_each_metadata`]. Attachments are never inside a chunk, so the scan does not decompress.
pub(crate) fn for_each_attachment<F>(
    input: &[u8],
    summary: Option<&mcap::Summary>,
    mut visit: F,
) -> Result<()>
where
    F: FnMut(mcap::Attachment) -> Result<()>,
{
    if let Some(summary) = summary {
        let indexed_count = summary.stats.as_ref().map(|stats| stats.attachment_count);
        if indexed_count == Some(summary.attachment_indexes.len() as u32) {
            let mut indexes = summary.attachment_indexes.clone();
            indexes.sort_by_key(|index| index.offset);
            for index in &indexes {
                let attachment = mcap::read::attachment(input, index).with_context(|| {
                    format!(
                        "failed to read attachment '{}' at offset {}",
                        index.name, index.offset
                    )
                })?;
                visit(attachment)?;
            }
            return Ok(());
        }
    }

    for record in mcap::read::LinearReader::new(input)? {
        if let mcap::records::Record::Attachment { header, data, .. } = record? {
            visit(mcap::Attachment {
                log_time: header.log_time,
                create_time: header.create_time,
                name: header.name,
                media_type: header.media_type,
                data: std::borrow::Cow::Borrowed(data.as_ref()),
            })?;
        }
    }
    Ok(())
}

/// Tracks metadata records already written so a merge can drop exact duplicates and enforce the
/// name-conflict policy across inputs.
#[derive(Default)]
pub(crate) struct MetadataState {
    seen: std::collections::HashSet<MetadataKey>,
    names: std::collections::HashSet<String>,
}

#[derive(PartialEq, Eq, Hash)]
struct MetadataKey {
    name: String,
    entries: Vec<(String, String)>,
}

/// Writes a metadata record, optionally deduplicating. With `dedup` off (the single-input rewrite
/// commands) every record is written verbatim. With `dedup` on (`merge`), records identical in name
/// and content are written once; a repeated name with *different* content is an error unless
/// `allow_duplicate_names` is set.
pub(crate) fn write_metadata_record<W: Write + Seek>(
    writer: &mut mcap::Writer<W>,
    state: &mut MetadataState,
    metadata: mcap::records::Metadata,
    dedup: bool,
    allow_duplicate_names: bool,
) -> Result<()> {
    if !dedup {
        writer.write_metadata(&metadata)?;
        return Ok(());
    }

    if state.names.contains(&metadata.name) && !allow_duplicate_names {
        bail!(
            "metadata name '{}' was previously encountered. Supply --allow-duplicate-metadata to override.",
            metadata.name
        );
    }

    let key = MetadataKey {
        name: metadata.name.clone(),
        entries: metadata
            .metadata
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
    };
    if state.seen.insert(key) {
        writer.write_metadata(&metadata)?;
        state.names.insert(metadata.name.clone());
    }
    Ok(())
}
