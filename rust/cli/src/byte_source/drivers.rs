//! Drive mcap sans-io readers against a [`ByteSource`].

use std::io::SeekFrom;

use anyhow::{bail, Context, Result};
use mcap::sans_io::{
    IndexedReader, LinearReadEvent, LinearReader, LinearReaderOptions, SummaryReadEvent,
    SummaryReader, SummaryReaderOptions,
};

use super::ByteSource;

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

/// Load the MCAP summary section via [`SummaryReader`].
///
/// Returns `Ok(None)` when the file has no summary section.
pub fn read_summary(source: &mut dyn ByteSource) -> Result<Option<mcap::Summary>> {
    let size = source.size()?;
    let options = match size {
        Some(size) => SummaryReaderOptions::default().with_file_size(size),
        None => SummaryReaderOptions::default(),
    };
    let mut reader = SummaryReader::new_with_options(options);
    let mut pos = 0u64;

    while let Some(event) = reader.next_event() {
        match event.context("summary reader error")? {
            SummaryReadEvent::ReadRequest(need) => {
                let data = source.read_at(pos, need)?;
                let buf = reader.insert(need);
                let n = data.len().min(buf.len());
                buf[..n].copy_from_slice(&data[..n]);
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
/// Requires a seekable source. Starts at offset 0 and advances with each read.
pub fn for_each_linear_record(
    source: &mut dyn ByteSource,
    options: LinearReaderOptions,
    mut visit: impl FnMut(u8, &[u8]) -> Result<()>,
) -> Result<()> {
    if !source.is_seekable() {
        bail!("linear record scan requires a seekable byte source");
    }

    let mut reader = LinearReader::new_with_options(options);
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
                visit(opcode, data)?;
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
