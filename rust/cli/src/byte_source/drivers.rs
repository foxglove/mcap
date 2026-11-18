//! Drive mcap sans-io readers against a [`ByteSource`].

use std::collections::HashMap;
use std::io::SeekFrom;
use std::ops::ControlFlow;

use anyhow::{bail, Context, Result};
use mcap::records::{op, Footer, Record};
use mcap::sans_io::{
    IndexedReader, LinearReadEvent, LinearReader, LinearReaderOptions, SummaryReadEvent,
    SummaryReader, SummaryReaderOptions,
};
use mcap::McapError;

use super::ByteSource;

/// Opcode (1) and little-endian body length (8) that start every record.
pub const RECORD_PREFIX_LEN: usize = 1 + 8;

/// Splits a record prefix off the front of `bytes`: the opcode, the declared body length, and
/// the bytes after the prefix. `None` when `bytes` is shorter than a prefix.
fn split_record_prefix(bytes: &[u8]) -> Option<(u8, u64, &[u8])> {
    let (prefix, rest) = bytes.split_at_checked(RECORD_PREFIX_LEN)?;
    let len = u64::from_le_bytes(prefix[1..].try_into().expect("8-byte length"));
    Some((prefix[0], len, rest))
}

/// Parses one serialized record (prefix followed by its body) from the start of `bytes`.
pub fn parse_record_bytes(bytes: &[u8]) -> Result<Record<'_>> {
    let (opcode, body_len, rest) = split_record_prefix(bytes)
        .context("record is shorter than its opcode and length prefix")?;
    let body = usize::try_from(body_len)
        .ok()
        .and_then(|len| rest.get(..len))
        .with_context(|| format!("record body of {body_len} bytes is truncated"))?;
    Ok(mcap::parse_record(opcode, body)?)
}

/// Reads the footer record at the end of `source` and checks the trailing magic. Returns the
/// footer's offset and the footer. Errors are [`McapError`]s: `UnexpectedEof` when the size is
/// unknown or too small to hold a footer, `BadMagic` when the trailing magic is wrong, and
/// `BadFooter` when the record before the magic is not a well-formed footer.
pub fn read_footer(source: &mut dyn ByteSource) -> Result<(u64, Footer)> {
    let tail_len = crate::parse::FOOTER_RECORD_AND_END_MAGIC_LEN;
    let footer_start = source
        .size()
        .filter(|size| *size >= (mcap::MAGIC.len() + tail_len) as u64)
        .map(|size| size - tail_len as u64)
        .ok_or(McapError::UnexpectedEof)?;
    let tail = source.read_at(footer_start, tail_len)?;
    if tail.len() != tail_len {
        return Err(McapError::UnexpectedEof.into());
    }
    let (record, magic) = tail.split_at(tail_len - mcap::MAGIC.len());
    if magic != mcap::MAGIC {
        return Err(McapError::BadMagic.into());
    }
    let (opcode, len, body) =
        split_record_prefix(record).expect("tail is longer than a record prefix");
    if opcode != op::FOOTER || len != body.len() as u64 {
        return Err(McapError::BadFooter.into());
    }
    match mcap::parse_record(opcode, body)? {
        Record::Footer(footer) => Ok((footer_start, footer)),
        _ => Err(McapError::BadFooter.into()),
    }
}

/// The largest record a source can hold: its size, when known. A reader given this limit rejects
/// a damaged length field instead of trying to allocate for it, which the slice-backed readers
/// did by construction.
pub fn record_length_limit(source: &dyn ByteSource) -> Option<usize> {
    source.size().and_then(|size| usize::try_from(size).ok())
}

/// Reads the raw bytes of the summary section, from the footer's `summary_start` to the footer.
/// Returns `Ok(None)` when the file has no summary section.
pub fn read_summary_section(source: &mut dyn ByteSource) -> Result<Option<Vec<u8>>> {
    let (footer_start, footer) = read_footer(source)?;
    if footer.summary_start == 0 {
        return Ok(None);
    }
    if footer.summary_start > footer_start {
        return Err(McapError::UnexpectedEof.into());
    }
    let summary_len = usize::try_from(footer_start - footer.summary_start)
        .context("summary section is too large")?;
    let summary = source.read_at(footer.summary_start, summary_len)?;
    if summary.len() != summary_len {
        return Err(McapError::UnexpectedEof.into());
    }
    Ok(Some(summary))
}

/// Load the MCAP summary section via [`SummaryReader`].
///
/// Returns `Ok(None)` when the file has no summary section. On a source with an
/// [indexed-read budget](ByteSource::indexed_read_budget) the section is capped like any other
/// indexed read: over the budget it is refused before any of it is fetched.
/// A failure is reported as the source describes it: see [`ByteSource::summary_read_error`].
pub fn read_summary(source: &mut dyn ByteSource) -> Result<Option<mcap::Summary>> {
    // A refusal over the budget is not a failed read, and already names the source.
    if source.indexed_read_budget().is_some() {
        require_summary_within_budget(source)?;
    }
    read_summary_as_read(source).map_err(|err| source.summary_read_error(err))
}

fn read_summary_as_read(source: &mut dyn ByteSource) -> Result<Option<mcap::Summary>> {
    let size = source.size();
    let mut options = SummaryReaderOptions::default();
    if let Some(size) = size {
        options = options.with_file_size(size);
    }
    if let Some(limit) = record_length_limit(source) {
        options = options.with_record_length_limit(limit);
    }
    let mut reader = SummaryReader::new_with_options(options);
    let mut pos = 0u64;

    while let Some(event) = reader.next_event() {
        match event.context("summary reader error")? {
            SummaryReadEvent::ReadRequest(need) => {
                let buf = reader.insert(need);
                let n = source.read_into(pos, buf)?;
                reader.notify_read(n);
                pos = pos.saturating_add(n as u64);
            }
            SummaryReadEvent::SeekRequest(to) => {
                pos = resolve_seek(to, pos, size)?;
                reader.notify_seeked(pos);
            }
        }
    }

    Ok(reader.finish())
}

/// Refuses a summary section over the source's indexed-read budget before any of it is fetched.
/// The footer gives its length, and a remote source usually still holds the footer in the tail
/// it prefetched at open. A footer that cannot be read is left for the reader to report.
fn require_summary_within_budget(source: &mut dyn ByteSource) -> Result<()> {
    let Ok((footer_start, footer)) = read_footer(source) else {
        return Ok(());
    };
    if footer.summary_start == 0 || footer.summary_start > footer_start {
        return Ok(());
    }
    crate::source::require_remote_indexed_read_budget(
        source,
        footer_start - footer.summary_start,
        "remote summary section",
    )
    .with_context(|| format!("failed to read {}", source.display_name()))
}

/// Fetch one indexed chunk and insert it into [`IndexedReader`].
pub fn service_indexed_chunk(
    reader: &mut IndexedReader,
    source: &mut dyn ByteSource,
    offset: u64,
    length: usize,
) -> Result<()> {
    let data = source.read_exact_at(offset, length)?;
    reader
        .insert_chunk_record_data(offset, &data)
        .context("failed to insert chunk data into indexed reader")?;
    Ok(())
}

/// Walk every top-level record in file order via [`LinearReader`].
///
/// Starts at offset 0 and advances with each read. To stop early (for example on a broken output
/// pipe) use [`try_for_each_linear_record`].
pub fn for_each_linear_record(
    source: &mut dyn ByteSource,
    options: LinearReaderOptions,
    mut visit: impl FnMut(u8, &[u8]) -> Result<()>,
) -> Result<()> {
    try_for_each_linear_record(source, options, |opcode, data| {
        visit(opcode, data).map(|()| ControlFlow::Continue(()))
    })
}

/// Like [`for_each_linear_record`], but `visit` can return [`ControlFlow::Break`] to stop the
/// scan without reading or decompressing the rest of the file. Unless `options` already carries
/// a record length limit, the source's size is used as one.
pub fn try_for_each_linear_record(
    source: &mut dyn ByteSource,
    options: LinearReaderOptions,
    mut visit: impl FnMut(u8, &[u8]) -> Result<ControlFlow<()>>,
) -> Result<()> {
    let options = match (options.record_length_limit, record_length_limit(source)) {
        (None, Some(limit)) => options.with_record_length_limit(limit),
        _ => options,
    };
    let mut reader = LinearReader::new_with_options(options);
    let mut pos = 0u64;

    while let Some(event) = reader.next_event() {
        match event.context("linear reader error")? {
            LinearReadEvent::ReadRequest(need) => {
                let buf = reader.insert(need);
                let n = source.read_into(pos, buf)?;
                reader.notify_read(n);
                pos = pos.saturating_add(n as u64);
            }
            LinearReadEvent::Record { opcode, data } => {
                if visit(opcode, data)?.is_break() {
                    return Ok(());
                }
            }
        }
    }

    Ok(())
}

/// Fills `dest` from `offset` with [`ByteSource::read_into`]. Returns how many bytes were read,
/// short only at the end of the source.
fn read_into_full(source: &mut dyn ByteSource, offset: u64, dest: &mut [u8]) -> Result<usize> {
    let mut filled = 0;
    while filled < dest.len() {
        let n = source.read_into(offset + filled as u64, &mut dest[filled..])?;
        if n == 0 {
            break;
        }
        filled += n;
    }
    Ok(filled)
}

/// Visit every top-level record of the data section with the given opcode, in file order,
/// passing its offset in the file and its body. The walk ends at the DataEnd record.
///
/// Unlike [`for_each_linear_record`], records of other opcodes are skipped by their declared
/// length rather than read, so scanning for a record kind that is never inside a chunk (metadata,
/// attachments) does not read the chunks. A chunk listed in `chunk_indexes` (the summary's, or
/// empty) is stepped over together with its message-index records, without a read.
///
/// Record prefixes are read with [`ByteSource::read_into`], so a remote source serves them from
/// its read-ahead window rather than one request each.
pub fn for_each_top_level_record_of(
    source: &mut dyn ByteSource,
    wanted: u8,
    chunk_indexes: &[mcap::records::ChunkIndex],
    mut visit: impl FnMut(u64, &[u8]) -> Result<()>,
) -> Result<()> {
    let mut magic = [0u8; mcap::MAGIC.len()];
    let n = read_into_full(source, 0, &mut magic)?;
    if &magic[..n] != mcap::MAGIC {
        return Err(McapError::BadMagic.into());
    }
    let size = source.size();
    let limit = record_length_limit(source);
    // Maps the start of each listed chunk to the end of its message indexes. An entry that
    // does not move forward or runs past the end of the file is left out, and the walk finds
    // that chunk by its prefix.
    let listed_chunks: HashMap<u64, u64> = chunk_indexes
        .iter()
        .filter_map(|index| {
            let end = index
                .chunk_start_offset
                .checked_add(index.chunk_length)?
                .checked_add(index.message_index_length)?;
            let in_file = size.is_none_or(|size| end <= size);
            (end > index.chunk_start_offset && in_file).then_some((index.chunk_start_offset, end))
        })
        .collect();
    let mut pos = mcap::MAGIC.len() as u64;
    loop {
        while let Some(&end) = listed_chunks.get(&pos) {
            pos = end;
        }
        let mut prefix = [0u8; RECORD_PREFIX_LEN];
        let n = read_into_full(source, pos, &mut prefix)?;
        let Some((opcode, len, _)) = split_record_prefix(&prefix[..n]) else {
            return Err(McapError::UnexpectedEof.into());
        };
        let body_start = pos.saturating_add(RECORD_PREFIX_LEN as u64);
        if opcode == wanted {
            let body_len = usize::try_from(len)
                .ok()
                .filter(|body_len| limit.is_none_or(|limit| *body_len <= limit));
            let Some(body_len) = body_len else {
                return Err(McapError::RecordTooLarge { opcode, len }.into());
            };
            let body = source.read_at(body_start, body_len)?;
            if body.len() < body_len {
                return Err(McapError::UnexpectedEof.into());
            }
            visit(pos, &body)?;
        }
        if opcode == op::DATA_END || opcode == op::FOOTER {
            return Ok(());
        }
        pos = body_start.saturating_add(len);
    }
}

fn resolve_seek(from: SeekFrom, pos: u64, size: Option<u64>) -> Result<u64> {
    let target = match from {
        SeekFrom::Start(offset) => offset as i128,
        SeekFrom::End(offset) => {
            let size = size.context("seek from end requires a known file size")?;
            size as i128 + offset as i128
        }
        SeekFrom::Current(offset) => pos as i128 + offset as i128,
    };
    if target < 0 {
        bail!("seek target is before the start of the file");
    }
    Ok(target as u64)
}
