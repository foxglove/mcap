//! Drive mcap sans-io readers against a [`ByteSource`].

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
use crate::source::SourceOptions;

/// Serialized footer record length: opcode, u64 body length, and the 20-byte body.
const FOOTER_RECORD_LEN: u64 = 1 + 8 + 20;

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

/// Reads the leading [`Header`](mcap::records::Header) record, if present. Stops after the first
/// record, and takes a leading chunk whole rather than decompressing it.
pub fn read_header(source: &mut dyn ByteSource) -> Result<Option<mcap::records::Header>> {
    let mut header = None;
    try_for_each_linear_record(
        source,
        LinearReaderOptions::default().with_emit_chunks(true),
        |opcode, data| {
            if let mcap::records::Record::Header(found) = mcap::parse_record(opcode, data)? {
                header = Some(found);
            }
            Ok(ControlFlow::Break(()))
        },
    )?;
    Ok(header)
}

/// Load the MCAP summary section via [`SummaryReader`].
///
/// Returns `Ok(None)` when the file has no summary section. On a remote source the summary
/// section is capped like any other indexed read: if it exceeds the no-opt-in budget the read is
/// refused before any of it is fetched, unless `--allow-remote-scan` was given.
pub fn read_summary(
    source: &mut dyn ByteSource,
    source_options: SourceOptions,
) -> Result<Option<mcap::Summary>> {
    let size = source.size();
    if let Some(size) = size.filter(|_| source.is_remote()) {
        require_remote_summary_budget(source, size, source_options)?;
    }
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

/// Reads the footer (already in the remote read-ahead window after open) to learn the summary
/// section length, and applies the remote indexed-read budget to it before [`SummaryReader`]
/// starts fetching the section. Malformed footers are left for the reader to report.
fn require_remote_summary_budget(
    source: &mut dyn ByteSource,
    size: u64,
    source_options: SourceOptions,
) -> Result<()> {
    let magic_len = mcap::MAGIC.len() as u64;
    let Some(footer_start) = size.checked_sub(FOOTER_RECORD_LEN + magic_len) else {
        return Ok(());
    };
    let footer = source.read_at(footer_start, FOOTER_RECORD_LEN as usize)?;
    if footer.len() != FOOTER_RECORD_LEN as usize || footer[0] != mcap::records::op::FOOTER {
        return Ok(());
    }
    let summary_start = u64::from_le_bytes(footer[9..17].try_into().expect("8-byte offset"));
    if summary_start == 0 || summary_start > footer_start {
        return Ok(());
    }
    crate::source::require_remote_indexed_read_budget(
        source,
        footer_start - summary_start,
        source_options,
        "remote summary section",
    )
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

/// Visit the body of every top-level record with the given opcode, in file order.
///
/// Unlike [`for_each_linear_record`], records of other opcodes are seeked over by their declared
/// length rather than read, so scanning for a record kind that is never inside a chunk (metadata,
/// attachments) costs one small read per top-level record instead of a read of the whole file.
pub fn for_each_top_level_record_of(
    source: &mut dyn ByteSource,
    wanted: u8,
    mut visit: impl FnMut(&[u8]) -> Result<()>,
) -> Result<()> {
    let magic = source.read_at(0, mcap::MAGIC.len())?;
    if magic.as_slice() != mcap::MAGIC {
        return Err(McapError::BadMagic.into());
    }
    let limit = record_length_limit(source);
    let mut pos = mcap::MAGIC.len() as u64;
    loop {
        let prefix = source.read_at(pos, RECORD_PREFIX_LEN)?;
        let Some((opcode, len, _)) = split_record_prefix(&prefix) else {
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
            visit(&body)?;
        }
        if opcode == op::FOOTER {
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
