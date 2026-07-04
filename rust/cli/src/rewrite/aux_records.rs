//! Copying of auxiliary (non-message) records — metadata and attachments — from the input to the
//! output. These are placed around the message stream (metadata first, attachments last) by the
//! pipeline in the parent module.
//!
//! Both use the same strategy: if a summary is available and its statistics confirm it indexes
//! every record of that kind, read them by offset; otherwise fall back to a top-level linear scan
//! so nothing is silently dropped (metadata/attachments are never stored inside chunks, so the scan
//! does not decompress). Passing `summary: None` (a summaryless input) always scans.

use std::borrow::Cow;
use std::io::{Seek, Write};

use anyhow::{Context, Result};

/// Copies every metadata record from `input` to `writer` at the current position.
pub(crate) fn copy_metadata<W: Write + Seek>(
    input: &[u8],
    summary: Option<&mcap::Summary>,
    writer: &mut mcap::Writer<W>,
) -> Result<()> {
    if let Some(summary) = summary {
        let indexed_count = summary.stats.as_ref().map(|stats| stats.metadata_count);
        if indexed_count == Some(summary.metadata_indexes.len() as u32) {
            let mut indexes = summary.metadata_indexes.clone();
            indexes.sort_by_key(|index| index.offset);
            for index in &indexes {
                let metadata = mcap::read::metadata(input, index).with_context(|| {
                    format!("failed to read metadata at offset {}", index.offset)
                })?;
                writer.write_metadata(&metadata)?;
            }
            return Ok(());
        }
    }

    for record in mcap::read::LinearReader::new(input)? {
        if let mcap::records::Record::Metadata(metadata) = record? {
            writer.write_metadata(&metadata)?;
        }
    }
    Ok(())
}

/// Copies every attachment from `input` to `writer` at the current position.
pub(crate) fn copy_attachments<W: Write + Seek>(
    input: &[u8],
    summary: Option<&mcap::Summary>,
    writer: &mut mcap::Writer<W>,
) -> Result<()> {
    if let Some(summary) = summary {
        let indexed_count = summary.stats.as_ref().map(|stats| stats.attachment_count);
        if indexed_count == Some(summary.attachment_indexes.len() as u32) {
            let mut indexes = summary.attachment_indexes.clone();
            indexes.sort_by_key(|index| index.offset);
            for index in &indexes {
                let attachment = mcap::read::attachment(input, index).with_context(|| {
                    format!(
                        "failed to read attachment {} at offset {}",
                        index.name, index.offset
                    )
                })?;
                writer.attach(&attachment)?;
            }
            return Ok(());
        }
    }

    for record in mcap::read::LinearReader::new(input)? {
        if let mcap::records::Record::Attachment { header, data, .. } = record? {
            writer.attach(&mcap::Attachment {
                log_time: header.log_time,
                create_time: header.create_time,
                name: header.name,
                media_type: header.media_type,
                data: Cow::Borrowed(data.as_ref()),
            })?;
        }
    }
    Ok(())
}
