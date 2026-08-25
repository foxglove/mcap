//! Low-level helpers shared by the single-input rewrite pipeline ([`super::single`]) and the
//! multi-input merge pipeline ([`super::merge`]): the writer builder and output-sink selection,
//! summary/index inspection against a [`ByteSource`], and the index-or-scan traversals for
//! metadata and attachments.
use std::io::{IsTerminal as _, Seek, Write};
use std::path::Path;

use anyhow::{bail, Context, Result};

use crate::byte_source::{self, ByteSource};
use crate::source::{self, SourceOptions};

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
/// profile onto the output.
pub(crate) fn read_header(source: &mut dyn ByteSource) -> Result<Option<mcap::records::Header>> {
    use mcap::sans_io::{LinearReadEvent, LinearReader, LinearReaderOptions};

    if !source.is_seekable() {
        bail!("reading the MCAP header requires a seekable byte source");
    }

    let mut reader = LinearReader::new_with_options(LinearReaderOptions::default());
    let mut pos = 0u64;
    while let Some(event) = reader.next_event() {
        match event.context("linear reader error")? {
            LinearReadEvent::ReadRequest(need) => {
                let data = source.read_at(pos, need)?;
                let buf = reader.insert(need);
                let n = data.len().min(buf.len());
                buf[..n].copy_from_slice(&data[..n]);
                reader.notify_read(n);
                pos = pos.saturating_add(n as u64);
            }
            LinearReadEvent::Record { opcode, data } => {
                return match mcap::parse_record(opcode, data)? {
                    mcap::records::Record::Header(header) => Ok(Some(header)),
                    _ => Ok(None),
                };
            }
        }
    }
    Ok(None)
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
    let Some(size) = source.size()? else {
        return Ok(false);
    };
    if size < (mcap::MAGIC.len() + 9 + 20 + mcap::MAGIC.len()) as u64 {
        return Ok(false);
    }
    // Footer body: summary_start (u64) + summary_offset_start (u64) + summary_crc (u32) = 20 bytes.
    // Record = opcode (1) + length (8) + body (20). Trailing magic follows.
    let footer_record_len = 9 + 20;
    let footer_offset = size
        .checked_sub(mcap::MAGIC.len() as u64 + footer_record_len as u64)
        .context("file too short for footer")?;
    let footer_bytes = source.read_at(footer_offset, footer_record_len)?;
    if footer_bytes.len() != footer_record_len || footer_bytes[0] != mcap::records::op::FOOTER {
        return Ok(false);
    }
    let summary_start = u64::from_le_bytes(footer_bytes[9..17].try_into().expect("8 bytes"));
    if summary_start == 0 || summary_start >= footer_offset {
        return Ok(false);
    }
    let summary_len = (footer_offset - summary_start) as usize;
    let summary_bytes = source.read_at(summary_start, summary_len)?;
    let mut offset = 0usize;
    while offset + 9 <= summary_bytes.len() {
        let opcode = summary_bytes[offset];
        let length =
            u64::from_le_bytes(summary_bytes[offset + 1..offset + 9].try_into().expect("8"))
                as usize;
        if opcode == mcap::records::op::CHUNK_INDEX {
            return Ok(true);
        }
        offset = offset
            .checked_add(9 + length)
            .ok_or_else(|| anyhow::anyhow!("summary record length overflows"))?;
    }
    Ok(false)
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
    // Opcode (1) + length (8) is enough to know the body size; then fetch the body.
    let header = source.read_at(offset, 9)?;
    if header.len() < 9 {
        bail!("message index header out of bounds at offset {offset}");
    }
    let opcode = header[0];
    if opcode != mcap::records::op::MESSAGE_INDEX {
        bail!("expected MessageIndex record at offset {offset}");
    }
    let length = u64::from_le_bytes(header[1..9].try_into().expect("slice has length 8"));
    let length = usize::try_from(length)
        .with_context(|| format!("message index length out of range at offset {offset}"))?;
    let body = source.read_at(offset + 9, length)?;
    if body.len() != length {
        bail!("message index body out of bounds at offset {offset}");
    }

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

/// Fulfills an [`mcap::sans_io::IndexedReader`] `ReadChunkRequest` via a ranged read.
pub(crate) fn service_chunk_request(
    reader: &mut mcap::sans_io::IndexedReader,
    source: &mut dyn ByteSource,
    offset: u64,
    length: usize,
) -> Result<()> {
    byte_source::service_indexed_chunk(reader, source, offset, length)
}

/// Errors when a remote source would need a full linear scan without `--allow-remote-scan`.
pub(crate) fn require_remote_scan_for_linear(
    source: &dyn ByteSource,
    options: SourceOptions,
) -> Result<()> {
    if source.is_remote() && !options.allow_remote_scan {
        bail!(
            "{}: remote rewrite would scan the entire file; {}",
            source.display_name(),
            source::remote_scan_opt_in_suffix()
        );
    }
    Ok(())
}

/// Visits every metadata record in the input, preferring the summary index and falling back to a
/// top-level scan when the summary does not index every record (index records are optional, and
/// statistics may be absent), so no records are dropped. Metadata is never inside a chunk, so the
/// scan does not decompress.
pub(crate) fn for_each_metadata<F>(
    source: &mut dyn ByteSource,
    summary: Option<&mcap::Summary>,
    source_options: SourceOptions,
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

    require_remote_scan_for_linear(source, source_options)?;
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
    source_options: SourceOptions,
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

    require_remote_scan_for_linear(source, source_options)?;
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
