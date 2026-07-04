//! Where messages come from, hidden behind [`MessageSource`].
//!
//! An input is read one of two ways:
//! - [`MessageSource::Indexed`] — through the summary index, in log-time order. The summary is the
//!   input's own (when it has a usable one) or one synthesized up front for a summaryless chunked
//!   file (see [`synthesize_chunk_summary`]), which keeps memory bounded to the working set.
//! - [`MessageSource::Linear`] — a single scan, used only for summaryless *unchunked* input:
//!   streamed in file order, or buffered and sorted when log-time ordering is requested.
//!
//! The engine takes a single input today, so a `MessageSource` writes its messages straight to the
//! output writer. A future multi-input merge would instead pull ordered messages from several
//! sources and interleave them.

use std::borrow::Cow;
use std::collections::HashMap;
use std::io::{Seek, Write};
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};

use super::{include_topic, FilterOptions};

/// A pre-start message buffered for the `--last-per-channel-topic-regex` feature.
#[derive(Debug, Clone)]
struct PreStartMessage {
    channel_id: u16,
    sequence: u32,
    log_time: u64,
    publish_time: u64,
    data: Vec<u8>,
}

/// How the messages of a single input are read.
pub(crate) enum MessageSource {
    /// Read via a summary index (the input's own or a synthesized one) in log-time order. The
    /// summary is boxed to keep this enum small.
    Indexed(Box<mcap::Summary>),
    /// Summaryless unchunked input: stream in file order, or buffer + sort when ordering.
    Linear,
}

impl MessageSource {
    /// Decides how to read `input`: use its summary index if usable; otherwise, when ordering is
    /// requested, synthesize an index for a chunked file; otherwise read linearly.
    pub(crate) fn plan(input: &[u8], opts: &FilterOptions) -> Result<Self> {
        if let Some(summary) = read_summary_for_indexed_transcode(input)? {
            if !summary.chunk_indexes.is_empty() {
                return Ok(MessageSource::Indexed(Box::new(summary)));
            }
        }
        if opts.order_by_log_time {
            if let Ok(Some(summary)) = synthesize_chunk_summary(input) {
                return Ok(MessageSource::Indexed(Box::new(summary)));
            }
        }
        Ok(MessageSource::Linear)
    }

    /// The summary backing this source, if any. Used to read metadata/attachments by offset.
    pub(crate) fn summary(&self) -> Option<&mcap::Summary> {
        match self {
            MessageSource::Indexed(summary) => Some(summary.as_ref()),
            MessageSource::Linear => None,
        }
    }

    /// Writes the selected messages, in the order this source produces them.
    pub(crate) fn write_messages<W: Write + Seek>(
        &self,
        input: &[u8],
        writer: &mut mcap::Writer<W>,
        opts: &FilterOptions,
    ) -> Result<()> {
        match self {
            MessageSource::Indexed(summary) => write_indexed(input, summary, writer, opts),
            MessageSource::Linear => write_linear(input, writer, opts),
        }
    }
}

fn write_indexed<W: Write + Seek>(
    input: &[u8],
    summary: &mcap::Summary,
    writer: &mut mcap::Writer<W>,
    opts: &FilterOptions,
) -> Result<()> {
    let has_topic_filters = !opts.include_topics.is_empty() || !opts.exclude_topics.is_empty();
    let included_topics: std::collections::BTreeSet<String> = summary
        .channels
        .values()
        .filter(|channel| include_topic(&channel.topic, opts))
        .map(|channel| channel.topic.clone())
        .collect();

    write_last_per_channel_preroll(input, summary, writer, opts)?;

    if !(has_topic_filters && included_topics.is_empty()) {
        let mut indexed_opts = mcap::sans_io::IndexedReaderOptions::new()
            .with_order(mcap::sans_io::indexed_reader::ReadOrder::LogTime)
            .log_time_on_or_after(opts.start);
        if opts.end != u64::MAX {
            indexed_opts = indexed_opts.log_time_before(opts.end);
        }
        if has_topic_filters {
            indexed_opts = indexed_opts.include_topics(included_topics.iter().cloned());
        }

        let mut reader = mcap::sans_io::IndexedReader::new_with_options(summary, indexed_opts)?;
        while let Some(event) = reader.next_event() {
            match event? {
                mcap::sans_io::IndexedReadEvent::ReadChunkRequest { offset, length } => {
                    let chunk_data = checked_slice(input, offset, length)?;
                    reader.insert_chunk_record_data(offset, chunk_data)?;
                }
                mcap::sans_io::IndexedReadEvent::Message { header, data } => {
                    let channel = summary.channels.get(&header.channel_id).ok_or_else(|| {
                        anyhow!("message references unknown channel {}", header.channel_id)
                    })?;
                    writer.write(&mcap::Message {
                        channel: channel.clone(),
                        sequence: header.sequence,
                        log_time: header.log_time,
                        publish_time: header.publish_time,
                        data: Cow::Borrowed(data),
                    })?;
                }
            }
        }
    }

    Ok(())
}

/// Writes the last message before `--start` for each matching `--last-per-channel-topic-regex`
/// topic, in log-time order, ahead of the main message stream.
fn write_last_per_channel_preroll<W: Write + Seek>(
    input: &[u8],
    summary: &mcap::Summary,
    writer: &mut mcap::Writer<W>,
    opts: &FilterOptions,
) -> Result<()> {
    if opts.last_per_channel_topics.is_empty() || opts.start == 0 {
        return Ok(());
    }

    let target_topics: std::collections::BTreeSet<String> = summary
        .channels
        .values()
        .filter(|channel| {
            include_topic(&channel.topic, opts)
                && opts
                    .last_per_channel_topics
                    .iter()
                    .any(|regex| regex.is_match(&channel.topic))
        })
        .map(|channel| channel.topic.clone())
        .collect();

    let mut pending_channels: std::collections::BTreeSet<u16> = summary
        .channels
        .iter()
        .filter(|(_, channel)| target_topics.contains(&channel.topic))
        .map(|(id, _)| *id)
        .collect();

    if pending_channels.is_empty() {
        return Ok(());
    }

    let mut reader = mcap::sans_io::IndexedReader::new_with_options(
        summary,
        mcap::sans_io::IndexedReaderOptions::new()
            .with_order(mcap::sans_io::indexed_reader::ReadOrder::ReverseLogTime)
            .log_time_before(opts.start)
            .include_topics(target_topics.iter().cloned()),
    )?;

    let mut pre_start_messages = Vec::<PreStartMessage>::new();
    while let Some(event) = reader.next_event() {
        match event? {
            mcap::sans_io::IndexedReadEvent::ReadChunkRequest { offset, length } => {
                let chunk_data = checked_slice(input, offset, length)?;
                reader.insert_chunk_record_data(offset, chunk_data)?;
            }
            mcap::sans_io::IndexedReadEvent::Message { header, data } => {
                if pending_channels.remove(&header.channel_id) {
                    pre_start_messages.push(PreStartMessage {
                        channel_id: header.channel_id,
                        sequence: header.sequence,
                        log_time: header.log_time,
                        publish_time: header.publish_time,
                        data: data.to_vec(),
                    });
                    if pending_channels.is_empty() {
                        break;
                    }
                }
            }
        }
    }

    pre_start_messages.sort_by_key(|message| {
        (
            message.log_time,
            message.channel_id,
            message.sequence,
            message.publish_time,
        )
    });
    for message in pre_start_messages {
        let channel = summary
            .channels
            .get(&message.channel_id)
            .ok_or_else(|| anyhow!("message references unknown channel {}", message.channel_id))?;
        writer.write(&mcap::Message {
            channel: channel.clone(),
            sequence: message.sequence,
            log_time: message.log_time,
            publish_time: message.publish_time,
            data: Cow::Borrowed(message.data.as_slice()),
        })?;
    }

    Ok(())
}

fn write_linear<W: Write + Seek>(
    input: &[u8],
    writer: &mut mcap::Writer<W>,
    opts: &FilterOptions,
) -> Result<()> {
    if !opts.last_per_channel_topics.is_empty() {
        bail!("including last-per-channel topics is not supported for non-indexed input");
    }

    let mut schemas = HashMap::<u16, Arc<mcap::Schema<'static>>>::new();
    let mut channel_defs = HashMap::<u16, mcap::records::Channel>::new();
    let mut channels = HashMap::<u16, Arc<mcap::Channel<'static>>>::new();
    // When ordering by log time, messages are buffered and sorted before writing (indexed inputs
    // are already read in log-time order, so only this path needs to sort). Payloads are borrowed
    // from `input` — unchunked messages are not decompressed — so this holds references, not copies.
    let mut pending_messages = Vec::<(usize, mcap::Message)>::new();

    for record in mcap::read::ChunkFlattener::new(input)? {
        match record? {
            mcap::records::Record::Schema { header, data } => {
                let schema = Arc::new(mcap::Schema {
                    id: header.id,
                    name: header.name,
                    encoding: header.encoding,
                    data: Cow::Owned(data.into_owned()),
                });
                schemas.insert(schema.id, schema);
            }
            mcap::records::Record::Channel(channel) => {
                if channel.schema_id == 0 || schemas.contains_key(&channel.schema_id) {
                    let resolved = build_channel(&channel, &schemas)?;
                    channels.insert(channel.id, resolved);
                }
                channel_defs.insert(channel.id, channel);
            }
            mcap::records::Record::Message { header, data } => {
                if header.log_time < opts.start || header.log_time >= opts.end {
                    continue;
                }

                let channel = if let Some(channel) = channels.get(&header.channel_id) {
                    channel.clone()
                } else {
                    let Some(channel_def) = channel_defs.get(&header.channel_id) else {
                        bail!("message references unknown channel {}", header.channel_id);
                    };
                    let resolved = build_channel(channel_def, &schemas)?;
                    channels.insert(header.channel_id, resolved.clone());
                    resolved
                };

                if !include_topic(&channel.topic, opts) {
                    continue;
                }

                let message = mcap::Message {
                    channel,
                    sequence: header.sequence,
                    log_time: header.log_time,
                    publish_time: header.publish_time,
                    data,
                };
                if opts.order_by_log_time {
                    let input_order = pending_messages.len();
                    pending_messages.push((input_order, message));
                } else {
                    writer.write(&message)?;
                }
            }
            // Metadata and attachments are copied separately by the pipeline; other records
            // (indexes, statistics, etc.) are regenerated by the writer.
            _ => {}
        }
    }

    if opts.order_by_log_time {
        pending_messages.sort_by_key(|(input_order, message)| (message.log_time, *input_order));
        for (_, message) in &pending_messages {
            writer.write(message)?;
        }
    }

    Ok(())
}

fn build_channel(
    channel: &mcap::records::Channel,
    schemas: &HashMap<u16, Arc<mcap::Schema<'static>>>,
) -> Result<Arc<mcap::Channel<'static>>> {
    let schema = if channel.schema_id == 0 {
        None
    } else {
        Some(schemas.get(&channel.schema_id).cloned().ok_or_else(|| {
            anyhow!(
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

fn checked_slice(input: &[u8], offset: u64, length: usize) -> Result<&[u8]> {
    let start = usize::try_from(offset)
        .with_context(|| format!("chunk offset out of range for this platform: {offset}"))?;
    let end = start
        .checked_add(length)
        .ok_or_else(|| anyhow!("chunk read overflow at offset {offset}"))?;
    input
        .get(start..end)
        .ok_or_else(|| anyhow!("chunk read out of bounds at offset {offset} length {length}"))
}

fn read_summary_for_indexed_transcode(input: &[u8]) -> Result<Option<mcap::Summary>> {
    match mcap::Summary::read(input) {
        Ok(Some(summary)) if summary.chunk_indexes.is_empty() => Ok(None),
        Ok(Some(summary)) if summary_supports_indexed_transcode(&summary) => Ok(Some(summary)),
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

fn incomplete_indexed_summary_error() -> anyhow::Error {
    anyhow!(
        "chunk-indexed MCAP summary is missing channel or schema records; run `mcap recover` to rewrite the file"
    )
}

pub(crate) fn summary_supports_indexed_transcode(summary: &mcap::Summary) -> bool {
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

/// Rebuilds a summary — chunk indexes, channel/schema definitions, and metadata/attachment indexes
/// — for a chunked file that lacks a usable summary section, so the memory-bounded indexed path can
/// re-order it instead of buffering every message.
///
/// A single top-level pass collects everything the indexed path needs: chunk offsets/time-bounds
/// come straight from the chunk records (decoded once to recover channel/schema definitions), and
/// metadata/attachment index records are built from the top-level records they point at. Populating
/// those indexes (with matching statistics counts) lets the indexed path read metadata/attachments
/// by offset rather than re-scanning the file, keeping the whole operation to two chunk passes: this
/// one and the indexed read.
///
/// Returns `Ok(None)` when the input has no chunks (the unchunked case, which the linear path
/// handles) or when the top-level framing can't be walked (caller falls back to the linear path).
pub(crate) fn synthesize_chunk_summary(input: &[u8]) -> Result<Option<mcap::Summary>> {
    let magic = mcap::MAGIC.len();
    let Some(limit) = input.len().checked_sub(magic) else {
        return Ok(None);
    };

    let mut chunk_indexes = Vec::new();
    let mut schemas = HashMap::<u16, Arc<mcap::Schema<'static>>>::new();
    let mut channels = HashMap::<u16, Arc<mcap::Channel<'static>>>::new();
    let mut attachment_indexes = Vec::new();
    let mut metadata_indexes = Vec::new();

    let mut offset = magic;
    while offset + 9 <= limit {
        let opcode = input[offset];
        let length = u64::from_le_bytes(
            input[offset + 1..offset + 9]
                .try_into()
                .expect("9-byte length slice"),
        ) as usize;
        let body_start = offset + 9;
        let Some(body_end) = body_start
            .checked_add(length)
            .filter(|end| *end <= input.len())
        else {
            return Ok(None);
        };
        let record_length = (9 + length) as u64;

        match mcap::parse_record(opcode, &input[body_start..body_end]) {
            Ok(mcap::records::Record::Chunk { header, data }) => {
                for record in mcap::read::ChunkReader::new(header.clone(), data.as_ref())? {
                    collect_definition(record?, &mut schemas, &mut channels);
                }
                chunk_indexes.push(mcap::records::ChunkIndex {
                    message_start_time: header.message_start_time,
                    message_end_time: header.message_end_time,
                    chunk_start_offset: offset as u64,
                    chunk_length: record_length,
                    message_index_offsets: Default::default(),
                    message_index_length: 0,
                    compression: header.compression,
                    compressed_size: header.compressed_size,
                    uncompressed_size: header.uncompressed_size,
                });
            }
            Ok(mcap::records::Record::Attachment { header, data, .. }) => {
                attachment_indexes.push(mcap::records::AttachmentIndex {
                    offset: offset as u64,
                    length: record_length,
                    log_time: header.log_time,
                    create_time: header.create_time,
                    data_size: data.len() as u64,
                    name: header.name,
                    media_type: header.media_type,
                });
            }
            Ok(mcap::records::Record::Metadata(metadata)) => {
                metadata_indexes.push(mcap::records::MetadataIndex {
                    offset: offset as u64,
                    length: record_length,
                    name: metadata.name,
                });
            }
            Ok(record) => collect_definition(record, &mut schemas, &mut channels),
            Err(_) => return Ok(None),
        }

        offset = body_end;
    }

    if chunk_indexes.is_empty() {
        return Ok(None);
    }
    // Statistics counts must match the index lengths so the indexed path trusts these indexes and
    // reads metadata/attachments by offset instead of scanning.
    let stats = mcap::records::Statistics {
        attachment_count: attachment_indexes.len() as u32,
        metadata_count: metadata_indexes.len() as u32,
        ..Default::default()
    };
    Ok(Some(mcap::Summary {
        stats: Some(stats),
        channels,
        schemas,
        chunk_indexes,
        attachment_indexes,
        metadata_indexes,
    }))
}

/// Collects a schema or channel definition into the maps used to build a synthesized summary.
fn collect_definition(
    record: mcap::records::Record,
    schemas: &mut HashMap<u16, Arc<mcap::Schema<'static>>>,
    channels: &mut HashMap<u16, Arc<mcap::Channel<'static>>>,
) {
    match record {
        mcap::records::Record::Schema { header, data } => {
            schemas.insert(
                header.id,
                Arc::new(mcap::Schema {
                    id: header.id,
                    name: header.name,
                    encoding: header.encoding,
                    data: Cow::Owned(data.into_owned()),
                }),
            );
        }
        mcap::records::Record::Channel(channel) => {
            if let Ok(resolved) = build_channel(&channel, schemas) {
                channels.insert(channel.id, resolved);
            }
        }
        _ => {}
    }
}
