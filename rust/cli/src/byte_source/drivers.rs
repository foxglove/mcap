//! Drive mcap sans-io readers against a [`ByteSource`].

use std::io::SeekFrom;
use std::ops::ControlFlow;

use anyhow::{bail, Context, Result};
use mcap::sans_io::{
    IndexedReader, LinearReadEvent, LinearReader, LinearReaderOptions, SummaryReadEvent,
    SummaryReader, SummaryReaderOptions,
};

use super::ByteSource;

/// The largest record a source can hold: its size, when known. A reader given this limit rejects
/// a damaged length field instead of trying to allocate for it, which the slice-backed readers
/// did by construction.
pub fn record_length_limit(source: &dyn ByteSource) -> Option<usize> {
    source.size().and_then(|size| usize::try_from(size).ok())
}

/// Load the MCAP summary section via [`SummaryReader`].
///
/// Returns `Ok(None)` when the file has no summary section.
pub fn read_summary(source: &mut dyn ByteSource) -> Result<Option<mcap::Summary>> {
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

/// Fetch one indexed chunk and insert it into [`IndexedReader`].
pub fn service_indexed_chunk(
    reader: &mut IndexedReader,
    source: &mut dyn ByteSource,
    offset: u64,
    length: usize,
) -> Result<()> {
    let data = source.read_at(offset, length)?;
    if data.len() != length {
        bail!(
            "short read for chunk at offset {offset}: expected {length} bytes, got {}",
            data.len()
        );
    }
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
    /// Opcode (1) and record length (8).
    const PREFIX_LEN: usize = 9;

    let magic = source.read_at(0, mcap::MAGIC.len())?;
    if magic.as_slice() != mcap::MAGIC {
        return Err(mcap::McapError::BadMagic.into());
    }
    let limit = record_length_limit(source);
    let mut pos = mcap::MAGIC.len() as u64;
    loop {
        let prefix = source.read_at(pos, PREFIX_LEN)?;
        if prefix.len() < PREFIX_LEN {
            return Err(mcap::McapError::UnexpectedEof.into());
        }
        let opcode = prefix[0];
        let len = u64::from_le_bytes(
            prefix[1..PREFIX_LEN]
                .try_into()
                .expect("slice has length 8"),
        );
        let body_start = pos.saturating_add(PREFIX_LEN as u64);
        if opcode == wanted {
            let body_len = usize::try_from(len)
                .ok()
                .filter(|body_len| limit.is_none_or(|limit| *body_len <= limit));
            let Some(body_len) = body_len else {
                return Err(mcap::McapError::RecordTooLarge { opcode, len }.into());
            };
            let body = source.read_at(body_start, body_len)?;
            if body.len() < body_len {
                return Err(mcap::McapError::UnexpectedEof.into());
            }
            visit(&body)?;
        }
        if opcode == mcap::records::op::FOOTER {
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
