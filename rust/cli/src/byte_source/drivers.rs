//! Drive mcap sans-io readers against a [`ByteSource`].

use std::io::SeekFrom;
use std::ops::ControlFlow;

use anyhow::{bail, Context, Result};
use mcap::sans_io::{
    IndexedReader, LinearReadEvent, LinearReader, LinearReaderOptions, SummaryReadEvent,
    SummaryReader, SummaryReaderOptions,
};

use super::ByteSource;
use crate::source::SourceOptions;

/// Serialized footer record length: opcode, u64 body length, and the 20-byte body.
const FOOTER_RECORD_LEN: u64 = 1 + 8 + 20;

/// Read the leading [`Header`](mcap::records::Header) record, if present.
pub fn read_header(source: &mut dyn ByteSource) -> Result<Option<mcap::records::Header>> {
    if !source.is_seekable() {
        bail!("reading the MCAP header requires a seekable byte source");
    }

    let mut reader = LinearReader::new_with_options(LinearReaderOptions::default());
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
                return match mcap::parse_record(opcode, data)? {
                    mcap::records::Record::Header(header) => Ok(Some(header)),
                    _ => Ok(None),
                };
            }
        }
    }
    Ok(None)
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
    let size = source.size()?;
    if let Some(size) = size.filter(|_| source.is_remote()) {
        require_remote_summary_budget(source, size, source_options)?;
    }
    let options = match size {
        Some(size) => SummaryReaderOptions::default().with_file_size(size),
        None => SummaryReaderOptions::default(),
    };
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
/// Requires a seekable source. Starts at offset 0 and advances with each read. To stop early
/// (for example on a broken output pipe) use [`try_for_each_linear_record`].
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
/// scan without reading or decompressing the rest of the file.
pub fn try_for_each_linear_record(
    source: &mut dyn ByteSource,
    options: LinearReaderOptions,
    mut visit: impl FnMut(u8, &[u8]) -> Result<ControlFlow<()>>,
) -> Result<()> {
    if !source.is_seekable() {
        bail!("linear record scan requires a seekable byte source");
    }

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
