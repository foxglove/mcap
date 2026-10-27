//! Low-level helpers shared by the single-input rewrite pipeline ([`super::single`]) and the
//! multi-input merge pipeline ([`super::merge`]): the writer builder and output-sink selection,
//! summary/index inspection against a [`ByteSource`], and the index-or-scan traversals for
//! metadata and attachments.
use std::io::{IsTerminal as _, Seek, Write};
use std::ops::ControlFlow;
use std::path::Path;

use anyhow::{bail, Context, Result};

use crate::byte_source::{self, ByteSource};

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

/// The output a rewrite writes to: a file, or stdout wrapped so the writer treats it as
/// non-seekable. A single type lets both entrypoints hand any output to the same generic writer.
pub(crate) enum OutputSink {
    File(std::fs::File),
    Stdout(mcap::write::NoSeek<std::io::StdoutLock<'static>>),
}

impl Write for OutputSink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            OutputSink::File(file) => file.write(buf),
            OutputSink::Stdout(stdout) => stdout.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            OutputSink::File(file) => file.flush(),
            OutputSink::Stdout(stdout) => stdout.flush(),
        }
    }
}

impl Seek for OutputSink {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        match self {
            OutputSink::File(file) => file.seek(pos),
            OutputSink::Stdout(stdout) => stdout.seek(pos),
        }
    }
}

/// Opens the destination for a rewrite: the given path, or stdout when `None`. Returns the sink and
/// whether the writer must disable seeking (stdout can't seek). Errors if stdout is a terminal, to
/// avoid dumping binary there.
pub(crate) fn open_output(output: Option<&Path>) -> Result<(OutputSink, bool)> {
    match output {
        Some(path) => {
            let file = std::fs::File::create(path)
                .with_context(|| format!("failed to open '{}' for writing", path.display()))?;
            Ok((OutputSink::File(file), false))
        }
        None => {
            if std::io::stdout().is_terminal() {
                bail!("{}", crate::source::PLEASE_REDIRECT);
            }
            let stdout = mcap::write::NoSeek::new(std::io::stdout().lock());
            Ok((OutputSink::Stdout(stdout), true))
        }
    }
}

/// Reads the leading [`Header`](mcap::records::Header) record, if present. Used to carry the input
/// profile onto the output. Stops after the first record, and takes a leading chunk whole rather
/// than decompressing it.
pub(crate) fn read_header(source: &mut dyn ByteSource) -> Result<Option<mcap::records::Header>> {
    let mut header = None;
    byte_source::try_for_each_linear_record(
        source,
        mcap::sans_io::LinearReaderOptions::default().with_emit_chunks(true),
        |opcode, data| {
            if let mcap::records::Record::Header(found) = mcap::parse_record(opcode, data)? {
                header = Some(found);
            }
            Ok(ControlFlow::Break(()))
        },
    )?;
    Ok(header)
}

fn incomplete_indexed_summary_error() -> anyhow::Error {
    anyhow::anyhow!(
        "chunk-indexed MCAP summary is missing channel or schema records; run `mcap recover` to rewrite the file"
    )
}

fn is_unknown_schema_error(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<mcap::McapError>()
            .is_some_and(|e| matches!(e, mcap::McapError::UnknownSchema(_, _)))
    })
}

/// Whether the summary section contains any ChunkIndex records. Used when summary parsing fails
/// with [`mcap::McapError::UnknownSchema`] to distinguish "not chunk-indexed → linear" from
/// "chunk-indexed but incomplete → error".
fn summary_section_has_chunk_indexes(source: &mut dyn ByteSource) -> Result<bool> {
    let tail_len = crate::parse::FOOTER_RECORD_AND_END_MAGIC_LEN;
    let Some(footer_start) = source
        .size()
        .filter(|size| *size >= (mcap::MAGIC.len() + tail_len) as u64)
        .map(|size| size - tail_len as u64)
    else {
        return Err(mcap::McapError::UnexpectedEof.into());
    };
    let tail = source.read_at(footer_start, tail_len)?;
    if tail.len() != tail_len || !tail.ends_with(mcap::MAGIC) {
        return Err(mcap::McapError::BadMagic.into());
    }
    let body = &tail[9..tail_len - mcap::MAGIC.len()];
    let declared_len = u64::from_le_bytes(tail[1..9].try_into().expect("8-byte length"));
    if tail[0] != mcap::records::op::FOOTER || declared_len != body.len() as u64 {
        return Err(mcap::McapError::BadFooter.into());
    }
    let footer = match mcap::parse_record(tail[0], body)? {
        mcap::records::Record::Footer(footer) => footer,
        _ => return Err(mcap::McapError::BadFooter.into()),
    };
    if footer.summary_start == 0 {
        return Ok(false);
    }
    if footer.summary_start > footer_start {
        return Err(mcap::McapError::UnexpectedEof.into());
    }
    let summary_len = usize::try_from(footer_start - footer.summary_start)
        .context("summary section is too large")?;
    let summary = source.read_at(footer.summary_start, summary_len)?;
    if summary.len() != summary_len {
        return Err(mcap::McapError::UnexpectedEof.into());
    }
    let parsed = crate::parse::parsed_mcap_from_summary_section(None, &summary)?;
    Ok(!parsed.chunk_indexes.is_empty())
}

/// Reads a summary only when it is usable for indexed reads. Returns `None` when there is no
/// summary, no chunk indexes, or (via [`mcap::McapError::UnknownSchema`]) a summary that does not
/// claim to be chunk-indexed. Returns an error for a summary that claims chunk indexes but is
/// missing the channel/schema records an indexed read needs.
pub(crate) fn read_indexed_summary(source: &mut dyn ByteSource) -> Result<Option<mcap::Summary>> {
    match byte_source::read_summary(source) {
        Ok(Some(summary)) if summary.chunk_indexes.is_empty() => Ok(None),
        Ok(Some(summary)) if summary_supports_indexed_read(&summary) => Ok(Some(summary)),
        Ok(Some(_)) => Err(incomplete_indexed_summary_error()),
        Ok(None) => Ok(None),
        Err(err) if is_unknown_schema_error(&err) => {
            if !summary_section_has_chunk_indexes(source)? {
                Ok(None)
            } else {
                Err(incomplete_indexed_summary_error())
            }
        }
        Err(err) => Err(err),
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
pub(crate) fn summary_indexes_all_messages(
    source: &mut dyn ByteSource,
    summary: &mcap::Summary,
) -> Result<bool> {
    let Some(stats) = summary.stats.as_ref() else {
        return Ok(false);
    };
    Ok(indexed_message_count(source, summary)? == Some(stats.message_count))
}

/// Whether the summary *proves* at least one message lives outside the chunk message indexes: the
/// statistics count differs from what the indexes cover, or a message index can't be parsed.
/// Returns `false` when statistics are absent, so a well-formed stats-less indexed file keeps the
/// indexed fast path (an index-only read of it is correct and cheaper, and `--last-per-channel`
/// requires it).
pub(crate) fn summary_has_unindexed_messages(
    source: &mut dyn ByteSource,
    summary: &mcap::Summary,
) -> Result<bool> {
    let Some(stats) = summary.stats.as_ref() else {
        return Ok(false);
    };
    match indexed_message_count(source, summary)? {
        Some(indexed_messages) => Ok(indexed_messages != stats.message_count),
        // An unparsable message index can't be trusted; fall back to a lossless scan.
        None => Ok(true),
    }
}

/// Sums the messages reachable through the chunk message indexes, or `None` if a message-index
/// record can't be parsed. Offsets are de-duplicated so a shared message-index record is counted
/// once.
fn indexed_message_count(
    source: &mut dyn ByteSource,
    summary: &mcap::Summary,
) -> Result<Option<u64>> {
    let mut offsets = std::collections::HashSet::new();
    let mut indexed_messages = 0u64;
    for offset in summary
        .chunk_indexes
        .iter()
        .flat_map(|chunk| chunk.message_index_offsets.values())
    {
        if offsets.insert(*offset) {
            let Some(count) = message_index_count(source, *offset).ok() else {
                return Ok(None);
            };
            indexed_messages = indexed_messages.saturating_add(count as u64);
        }
    }
    Ok(Some(indexed_messages))
}

/// Number of bytes per message-index entry (a `(log_time, offset)` pair of `uint64`s).
const MESSAGE_INDEX_ENTRY_SIZE: usize = 16;

fn message_index_count(source: &mut dyn ByteSource, offset: u64) -> Result<usize> {
    // Opcode (1), record length (8), channel ID (2), entry-array byte length (4). The entries
    // themselves are not read: the count follows from the byte length, as it did when the
    // memory mapping made slicing the body free.
    const PREFIX_LEN: usize = 15;
    let prefix = source.read_at(offset, PREFIX_LEN)?;
    if prefix.len() < PREFIX_LEN {
        bail!("message index header out of bounds at offset {offset}");
    }
    if prefix[0] != mcap::records::op::MESSAGE_INDEX {
        bail!("expected MessageIndex record at offset {offset}");
    }
    let length = u64::from_le_bytes(prefix[1..9].try_into().expect("slice has length 8"));
    let byte_len = u64::from(u32::from_le_bytes(
        prefix[11..15].try_into().expect("slice has length 4"),
    ));
    if !byte_len.is_multiple_of(MESSAGE_INDEX_ENTRY_SIZE as u64) {
        bail!("message index entries are misaligned at offset {offset}");
    }
    if length != 6 + byte_len {
        bail!("message index length mismatch at offset {offset}");
    }
    if source
        .size()
        .is_some_and(|size| offset.saturating_add(9 + length) > size)
    {
        bail!("message index body out of bounds at offset {offset}");
    }
    Ok((byte_len / MESSAGE_INDEX_ENTRY_SIZE as u64) as usize)
}

/// Visits every metadata record in the input, preferring the summary index and falling back to a
/// top-level scan when the summary does not index every record (index records are optional, and
/// statistics may be absent), so no records are dropped. Metadata is never inside a chunk, so the
/// scan does not decompress.
pub(crate) fn for_each_metadata<F>(
    source: &mut dyn ByteSource,
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
                let length = usize::try_from(index.length).with_context(|| {
                    format!(
                        "metadata '{}' length out of range at offset {}",
                        index.name, index.offset
                    )
                })?;
                let bytes = source.read_at(index.offset, length)?;
                let metadata = crate::parse::parse_metadata_record(&bytes).with_context(|| {
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

    byte_source::for_each_linear_record(
        source,
        mcap::sans_io::LinearReaderOptions::default().with_emit_chunks(true),
        |opcode, data| {
            if opcode == mcap::records::op::METADATA {
                if let mcap::records::Record::Metadata(metadata) = mcap::parse_record(opcode, data)?
                {
                    visit(metadata)?;
                }
            }
            Ok(())
        },
    )
}

/// Visits every attachment in the input, using the same index-or-scan strategy as
/// [`for_each_metadata`]. Attachments are never inside a chunk, so the scan does not decompress.
pub(crate) fn for_each_attachment<F>(
    source: &mut dyn ByteSource,
    summary: Option<&mcap::Summary>,
    mut visit: F,
) -> Result<()>
where
    F: FnMut(mcap::Attachment<'static>) -> Result<()>,
{
    if let Some(summary) = summary {
        let indexed_count = summary.stats.as_ref().map(|stats| stats.attachment_count);
        if indexed_count == Some(summary.attachment_indexes.len() as u32) {
            let mut indexes = summary.attachment_indexes.clone();
            indexes.sort_by_key(|index| index.offset);
            for index in &indexes {
                let length = usize::try_from(index.length).with_context(|| {
                    format!(
                        "attachment '{}' length out of range at offset {}",
                        index.name, index.offset
                    )
                })?;
                let bytes = source.read_at(index.offset, length)?;
                let attachment =
                    crate::parse::parse_attachment_record(&bytes).with_context(|| {
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

    byte_source::for_each_linear_record(
        source,
        mcap::sans_io::LinearReaderOptions::default().with_emit_chunks(true),
        |opcode, data| {
            if opcode == mcap::records::op::ATTACHMENT {
                if let mcap::records::Record::Attachment { header, data, .. } =
                    mcap::parse_record(opcode, data)?
                {
                    visit(mcap::Attachment {
                        log_time: header.log_time,
                        create_time: header.create_time,
                        name: header.name,
                        media_type: header.media_type,
                        data: std::borrow::Cow::Owned(data.into_owned()),
                    })?;
                }
            }
            Ok(())
        },
    )
}

/// Flips the last byte of the first message inside the file's first chunk, which must be
/// uncompressed, so every record still parses and only the chunk's CRC disagrees.
#[cfg(test)]
pub(crate) fn corrupt_first_chunk_message(bytes: &mut [u8]) {
    const PREFIX: usize = 1 + 8;
    let len_at =
        |at: usize| u64::from_le_bytes(bytes[at + 1..at + PREFIX].try_into().unwrap()) as usize;
    let mut at = mcap::MAGIC.len();
    while at + PREFIX <= bytes.len() {
        let len = len_at(at);
        if bytes[at] != mcap::records::op::CHUNK {
            at += PREFIX + len;
            continue;
        }
        // Chunk body: start/end time (16), uncompressed size (8), crc (4), compression
        // (4 + name), records length (8), records.
        let body = at + PREFIX;
        let compression_len =
            u32::from_le_bytes(bytes[body + 28..body + 32].try_into().unwrap()) as usize;
        assert_eq!(compression_len, 0, "the chunk must be uncompressed");
        let records_len_at = body + 32;
        let records_len = u64::from_le_bytes(
            bytes[records_len_at..records_len_at + 8]
                .try_into()
                .unwrap(),
        ) as usize;
        let mut record = records_len_at + 8;
        let end = record + records_len;
        while record + PREFIX <= end {
            let record_len = len_at(record);
            if bytes[record] == mcap::records::op::MESSAGE {
                bytes[record + PREFIX + record_len - 1] ^= 0xFF;
                return;
            }
            record += PREFIX + record_len;
        }
        panic!("no message in the first chunk");
    }
    panic!("no chunk in the file");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::byte_source::MemorySource;

    /// Builds a well-formed MessageIndex record whose entry array holds `entry_count` entries.
    fn message_index_record(entry_count: usize) -> Vec<u8> {
        let byte_len = (entry_count * MESSAGE_INDEX_ENTRY_SIZE) as u32;
        let mut body = Vec::new();
        body.extend_from_slice(&0u16.to_le_bytes()); // channel_id (not used by the count)
        body.extend_from_slice(&byte_len.to_le_bytes()); // entry array byte length
        body.extend_from_slice(&vec![0u8; byte_len as usize]); // entries
        let mut record = vec![mcap::records::op::MESSAGE_INDEX];
        record.extend_from_slice(&(body.len() as u64).to_le_bytes());
        record.extend_from_slice(&body);
        record
    }

    #[test]
    fn message_index_count_counts_entries() {
        let mut source = MemorySource::new(message_index_record(0));
        assert_eq!(message_index_count(&mut source, 0).unwrap(), 0);
        let mut source = MemorySource::new(message_index_record(3));
        assert_eq!(message_index_count(&mut source, 0).unwrap(), 3);
    }

    #[test]
    fn message_index_count_honors_a_nonzero_offset() {
        let mut buf = vec![0xAAu8; 5]; // arbitrary leading bytes
        buf.extend_from_slice(&message_index_record(2));
        let mut source = MemorySource::new(buf);
        assert_eq!(message_index_count(&mut source, 5).unwrap(), 2);
    }

    #[test]
    fn message_index_count_rejects_wrong_opcode() {
        let mut record = message_index_record(1);
        record[0] = mcap::records::op::MESSAGE;
        let mut source = MemorySource::new(record);
        let err = message_index_count(&mut source, 0).unwrap_err();
        assert!(err.to_string().contains("expected MessageIndex record"));
    }

    #[test]
    fn message_index_count_rejects_misaligned_entries() {
        // A 15-byte entry array is not a multiple of the 16-byte entry size.
        let mut body = Vec::new();
        body.extend_from_slice(&0u16.to_le_bytes());
        body.extend_from_slice(&15u32.to_le_bytes());
        body.extend_from_slice(&[0u8; 15]);
        let mut record = vec![mcap::records::op::MESSAGE_INDEX];
        record.extend_from_slice(&(body.len() as u64).to_le_bytes());
        record.extend_from_slice(&body);
        let mut source = MemorySource::new(record);
        let err = message_index_count(&mut source, 0).unwrap_err();
        assert!(err.to_string().contains("misaligned"));
    }

    #[test]
    fn message_index_count_rejects_length_mismatch() {
        let mut record = message_index_record(1); // byte_len 16, body 22, length 22
                                                  // Shrink the declared record length so the body no longer equals 6 + byte_len.
        record[1..9].copy_from_slice(&14u64.to_le_bytes());
        let mut source = MemorySource::new(record);
        let err = message_index_count(&mut source, 0).unwrap_err();
        assert!(err.to_string().contains("length mismatch"));
    }

    #[test]
    fn message_index_count_rejects_out_of_bounds_header() {
        let mut source = MemorySource::new(Vec::new());
        let err = message_index_count(&mut source, 0).unwrap_err();
        assert!(err.to_string().contains("out of bounds"));
    }
}
