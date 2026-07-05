//! The read / select / place pipeline: [`run`] reads an input source and writes a new MCAP,
//! choosing an indexed or linear read path and applying the standardized record placement.
use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap};
use std::io::{IsTerminal as _, Seek, Write};
use std::sync::Arc;

use anyhow::{bail, Context, Result};

use super::options::{include_topic, resolve_options, ResolvedOptions, RewriteOptions};
use crate::{parse, source};

pub(crate) fn run(args: RewriteOptions, source_options: source::SourceOptions) -> Result<()> {
    let opts = resolve_options(&args)?;
    if let (Some(input), Some(output)) = (args.file.as_deref(), opts.output.as_deref()) {
        source::ensure_distinct_local_input_output(input, output)?;
    }
    let input = source::load_input(args.file.as_deref(), source_options)?;

    if let Some(output) = &opts.output {
        let writer = std::fs::File::create(output)
            .with_context(|| format!("failed to open '{}' for writing", output.display()))?;
        filter_to_writer(input.as_slice(), writer, &opts, false)
    } else {
        if std::io::stdout().is_terminal() {
            bail!("{}", source::PLEASE_REDIRECT);
        }
        let stdout = std::io::stdout();
        let writer = mcap::write::NoSeek::new(stdout.lock());
        filter_to_writer(input.as_slice(), writer, &opts, true)
    }
}

fn filter_to_writer<W: Write + Seek>(
    input: &[u8],
    sink: W,
    opts: &ResolvedOptions,
    disable_seeking: bool,
) -> Result<()> {
    let mut write_options = mcap::WriteOptions::new()
        .use_chunks(opts.use_chunks)
        .chunk_size(Some(opts.chunk_size))
        .compression(opts.compression)
        .disable_seeking(disable_seeking);

    write_options = write_options.library(crate::cli::LIBRARY_IDENTIFIER.clone());
    if let Some(header) = read_header(input)? {
        write_options = write_options.profile(header.profile);
    }

    let mut writer = write_options
        .create(sink)
        .context("failed to create mcap writer")?;
    filter_with_writer(input, &mut writer, opts)?;
    writer.finish().context("failed to finish mcap writer")?;
    Ok(())
}

fn read_header(input: &[u8]) -> Result<Option<mcap::records::Header>> {
    let mut reader = mcap::read::LinearReader::new(input)?;
    match reader.next() {
        Some(Ok(mcap::records::Record::Header(header))) => Ok(Some(header)),
        Some(Ok(_)) | None => Ok(None),
        Some(Err(err)) => Err(err.into()),
    }
}

fn filter_with_writer<W: Write + Seek>(
    input: &[u8],
    writer: &mut mcap::Writer<W>,
    opts: &ResolvedOptions,
) -> Result<()> {
    if let Some(summary) = read_indexed_summary(input)? {
        if !summary.chunk_indexes.is_empty() {
            return filter_indexed(input, &summary, writer, opts);
        }
    }
    filter_linear(input, writer, opts)
}

fn read_indexed_summary(input: &[u8]) -> Result<Option<mcap::Summary>> {
    match mcap::Summary::read(input) {
        Ok(Some(summary)) if summary.chunk_indexes.is_empty() => Ok(None),
        Ok(Some(summary)) if summary_supports_indexed_read(&summary) => Ok(Some(summary)),
        Ok(Some(_)) => Err(incomplete_indexed_summary_error()),
        Ok(None) => Ok(None),
        Err(mcap::McapError::UnknownSchema(_, _))
            if !parse::summary_section_has_chunk_indexes(input)? =>
        {
            Ok(None)
        }
        Err(mcap::McapError::UnknownSchema(_, _)) => Err(incomplete_indexed_summary_error()),
        Err(err) => Err(err.into()),
    }
}

pub(crate) fn incomplete_indexed_summary_error() -> anyhow::Error {
    anyhow::anyhow!(
        "chunk-indexed MCAP summary is missing channel or schema records; run `mcap recover` to rewrite the file"
    )
}

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

#[derive(Debug, Clone)]
struct PreStartMessage {
    channel_id: u16,
    sequence: u32,
    log_time: u64,
    publish_time: u64,
    data: Vec<u8>,
}

fn filter_indexed<W: Write + Seek>(
    input: &[u8],
    summary: &mcap::Summary,
    writer: &mut mcap::Writer<W>,
    opts: &ResolvedOptions,
) -> Result<()> {
    let has_topic_filters = !opts.include_topics.is_empty() || !opts.exclude_topics.is_empty();
    let included_topics: BTreeSet<String> = summary
        .channels
        .values()
        .filter(|channel| include_topic(&channel.topic, opts))
        .map(|channel| channel.topic.clone())
        .collect();

    // Metadata is written first, before any messages (see module docs).
    if opts.include_metadata {
        copy_metadata_indexed_or_scanned(input, summary, writer)?;
    }

    if !opts.last_per_channel_topics.is_empty() && opts.start > 0 {
        let target_topics: BTreeSet<String> = summary
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

        let mut pending_channels: BTreeSet<u16> = summary
            .channels
            .iter()
            .filter(|(_, channel)| target_topics.contains(&channel.topic))
            .map(|(id, _)| *id)
            .collect();

        if !pending_channels.is_empty() {
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
                let channel = summary.channels.get(&message.channel_id).ok_or_else(|| {
                    anyhow::anyhow!("message references unknown channel {}", message.channel_id)
                })?;
                writer.write(&mcap::Message {
                    channel: channel.clone(),
                    sequence: message.sequence,
                    log_time: message.log_time,
                    publish_time: message.publish_time,
                    data: Cow::Borrowed(message.data.as_slice()),
                })?;
            }
        }
    }

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
                        anyhow::anyhow!("message references unknown channel {}", header.channel_id)
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

    // Attachments are written last, kept regardless of the message time range.
    if opts.include_attachments {
        copy_attachments_indexed_or_scanned(input, summary, writer)?;
    }

    Ok(())
}

/// Copies every metadata record from an indexed input at the current position. The summary index is
/// trusted only when the statistics count matches its length; otherwise (index records are optional
/// even alongside chunk indexes, or statistics are absent) it falls back to a top-level scan so no
/// records are dropped. Metadata is never inside a chunk, so the scan does not decompress.
fn copy_metadata_indexed_or_scanned<W: Write + Seek>(
    input: &[u8],
    summary: &mcap::Summary,
    writer: &mut mcap::Writer<W>,
) -> Result<()> {
    let indexed_count = summary.stats.as_ref().map(|stats| stats.metadata_count);
    if indexed_count == Some(summary.metadata_indexes.len() as u32) {
        let mut indexes = summary.metadata_indexes.clone();
        indexes.sort_by_key(|index| index.offset);
        for index in &indexes {
            let metadata = mcap::read::metadata(input, index)
                .with_context(|| format!("failed to read metadata at offset {}", index.offset))?;
            writer.write_metadata(&metadata)?;
        }
        return Ok(());
    }

    for record in mcap::read::LinearReader::new(input)? {
        if let mcap::records::Record::Metadata(metadata) = record? {
            writer.write_metadata(&metadata)?;
        }
    }
    Ok(())
}

/// Copies every attachment from an indexed input, writing them at the current position. Uses the
/// same statistics cross-check as [`copy_metadata_indexed_or_scanned`], falling back to a top-level
/// linear scan when the summary does not index every attachment so none are silently dropped.
fn copy_attachments_indexed_or_scanned<W: Write + Seek>(
    input: &[u8],
    summary: &mcap::Summary,
    writer: &mut mcap::Writer<W>,
) -> Result<()> {
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

fn filter_linear<W: Write + Seek>(
    input: &[u8],
    writer: &mut mcap::Writer<W>,
    opts: &ResolvedOptions,
) -> Result<()> {
    if !opts.last_per_channel_topics.is_empty() {
        bail!("including last-per-channel topics is not supported for non-indexed input");
    }

    // Metadata is written first, before any messages (see module docs). Metadata records are never
    // stored inside chunks, so a top-level linear scan surfaces them without decompressing chunks.
    if opts.include_metadata {
        for record in mcap::read::LinearReader::new(input)? {
            if let mcap::records::Record::Metadata(metadata) = record? {
                writer.write_metadata(&metadata)?;
            }
        }
    }

    let mut schemas = HashMap::<u16, Arc<mcap::Schema<'static>>>::new();
    let mut channel_defs = HashMap::<u16, mcap::records::Channel>::new();
    let mut channels = HashMap::<u16, Arc<mcap::Channel<'static>>>::new();
    // Collected during the message pass (borrowing from `input`, no copy) and written last.
    let mut pending_attachments = Vec::<mcap::Attachment>::new();

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

                writer.write(&mcap::Message {
                    channel,
                    sequence: header.sequence,
                    log_time: header.log_time,
                    publish_time: header.publish_time,
                    data: Cow::Borrowed(data.as_ref()),
                })?;
            }
            mcap::records::Record::Attachment { header, data, .. } if opts.include_attachments => {
                // Kept regardless of the message time range.
                pending_attachments.push(mcap::Attachment {
                    log_time: header.log_time,
                    create_time: header.create_time,
                    name: header.name,
                    media_type: header.media_type,
                    data,
                });
            }
            // Metadata is handled up front; everything else (indexes, statistics, etc.) is
            // regenerated by the writer.
            _ => {}
        }
    }

    // Attachments are written last, after all messages (see module docs).
    for attachment in &pending_attachments {
        writer.attach(attachment)?;
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

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::io::Cursor;

    use regex::Regex;

    use super::{filter_to_writer, ResolvedOptions};

    fn write_filter_test_input(chunked: bool, summaryless: bool) -> Vec<u8> {
        write_filter_test_input_with_options(chunked, summaryless, true, true, true, true)
    }

    fn write_filter_test_input_with_summary_repeats(
        chunked: bool,
        summaryless: bool,
        repeat_channels: bool,
        repeat_schemas: bool,
    ) -> Vec<u8> {
        write_filter_test_input_with_options(
            chunked,
            summaryless,
            repeat_channels,
            repeat_schemas,
            true,
            true,
        )
    }

    fn write_filter_test_input_with_options(
        chunked: bool,
        summaryless: bool,
        repeat_channels: bool,
        repeat_schemas: bool,
        emit_message_indexes: bool,
        emit_chunk_indexes: bool,
    ) -> Vec<u8> {
        let mut output = Cursor::new(Vec::new());
        {
            let mut options = mcap::WriteOptions::new()
                .use_chunks(chunked)
                .emit_message_indexes(emit_message_indexes)
                .emit_chunk_indexes(emit_chunk_indexes)
                .library("test-recorder/0.0");
            if chunked {
                options = options.chunk_size(Some(10));
            }
            if summaryless {
                options = options
                    .emit_summary_records(false)
                    .emit_summary_offsets(false);
            } else {
                options = options
                    .repeat_channels(repeat_channels)
                    .repeat_schemas(repeat_schemas);
            }
            let mut writer = options.create(&mut output).expect("writer");
            let schema_id = writer
                .add_schema("schema", "jsonschema", br#"{}"#)
                .expect("schema");
            let camera_a = writer
                .add_channel(schema_id, "camera_a", "json", &BTreeMap::new())
                .expect("channel");
            let camera_b = writer
                .add_channel(schema_id, "camera_b", "json", &BTreeMap::new())
                .expect("channel");
            let radar = writer
                .add_channel(0, "radar_a", "json", &BTreeMap::new())
                .expect("channel");
            for i in 0..100 {
                writer
                    .write_to_known_channel(
                        &mcap::records::MessageHeader {
                            channel_id: camera_a,
                            sequence: i,
                            log_time: i as u64,
                            publish_time: i as u64,
                        },
                        b"a",
                    )
                    .expect("write");
                writer
                    .write_to_known_channel(
                        &mcap::records::MessageHeader {
                            channel_id: camera_b,
                            sequence: i,
                            log_time: i as u64,
                            publish_time: i as u64,
                        },
                        b"b",
                    )
                    .expect("write");
                writer
                    .write_to_known_channel(
                        &mcap::records::MessageHeader {
                            channel_id: radar,
                            sequence: i,
                            log_time: i as u64,
                            publish_time: i as u64,
                        },
                        b"c",
                    )
                    .expect("write");
            }
            writer
                .attach(&mcap::Attachment {
                    log_time: 50,
                    create_time: 50,
                    name: "attachment".to_string(),
                    media_type: "application/octet-stream".to_string(),
                    data: std::borrow::Cow::Borrowed(&[]),
                })
                .expect("attachment");
            writer
                .write_metadata(&mcap::records::Metadata {
                    name: "metadata".to_string(),
                    metadata: BTreeMap::new(),
                })
                .expect("metadata");
            writer.finish().expect("finish");
        }
        output.into_inner()
    }

    fn run_filter(input: &[u8], opts: &ResolvedOptions) -> Vec<u8> {
        let mut output = Cursor::new(Vec::new());
        filter_to_writer(input, &mut output, opts, false).expect("filter should succeed");
        output.into_inner()
    }

    #[derive(Default)]
    struct OutputStats {
        topic_counts: BTreeMap<String, usize>,
        metadata_count: usize,
        attachment_count: usize,
        log_times: Vec<u64>,
    }

    fn analyze_output(output: &[u8]) -> OutputStats {
        let mut stats = OutputStats::default();
        let mut channels_by_id = HashMap::<u16, String>::new();

        for record in mcap::read::ChunkFlattener::new(output)
            .expect("reader")
            .map(|record| record.expect("record"))
        {
            match record {
                mcap::records::Record::Channel(channel) => {
                    channels_by_id.insert(channel.id, channel.topic);
                }
                mcap::records::Record::Message { header, .. } => {
                    let topic = channels_by_id
                        .get(&header.channel_id)
                        .cloned()
                        .unwrap_or_else(|| format!("unknown-{}", header.channel_id));
                    *stats.topic_counts.entry(topic).or_default() += 1;
                    stats.log_times.push(header.log_time);
                }
                mcap::records::Record::Metadata(_) => stats.metadata_count += 1,
                mcap::records::Record::Attachment { .. } => stats.attachment_count += 1,
                _ => {}
            }
        }
        stats
    }

    fn include_all_options() -> ResolvedOptions {
        ResolvedOptions {
            output: None,
            include_topics: Vec::new(),
            exclude_topics: Vec::new(),
            last_per_channel_topics: Vec::new(),
            start: 0,
            end: u64::MAX,
            include_metadata: true,
            include_attachments: true,
            compression: Some(mcap::Compression::Zstd),
            chunk_size: mcap::WriteOptions::DEFAULT_CHUNK_SIZE,
            use_chunks: true,
        }
    }

    fn time_windowed_options(start: u64, end: u64) -> ResolvedOptions {
        ResolvedOptions {
            start,
            end,
            ..include_all_options()
        }
    }

    /// Builds a chunk-indexed file whose summary omits metadata/attachment index records — a
    /// spec-legal shape our own writer avoids but other writers may produce.
    fn write_chunk_indexed_without_aux_indexes() -> Vec<u8> {
        let mut output = Cursor::new(Vec::new());
        {
            let mut writer = mcap::WriteOptions::new()
                .emit_metadata_indexes(false)
                .emit_attachment_indexes(false)
                .library("test-recorder/0.0")
                .create(&mut output)
                .expect("writer");
            let schema_id = writer
                .add_schema("schema", "jsonschema", br#"{}"#)
                .expect("schema");
            let channel = writer
                .add_channel(schema_id, "camera_a", "json", &BTreeMap::new())
                .expect("channel");
            writer
                .write_to_known_channel(
                    &mcap::records::MessageHeader {
                        channel_id: channel,
                        sequence: 0,
                        log_time: 0,
                        publish_time: 0,
                    },
                    b"a",
                )
                .expect("write message");
            writer
                .attach(&mcap::Attachment {
                    log_time: 5,
                    create_time: 5,
                    name: "a.bin".to_string(),
                    media_type: "application/octet-stream".to_string(),
                    data: std::borrow::Cow::Borrowed(&[1, 2, 3]),
                })
                .expect("attachment");
            writer
                .write_metadata(&mcap::records::Metadata {
                    name: "m".to_string(),
                    metadata: BTreeMap::new(),
                })
                .expect("metadata");
            writer.finish().expect("finish");
        }
        output.into_inner()
    }

    /// Collects the opcodes of the top-level records in `bytes`, in file order.
    fn top_level_opcodes(bytes: &[u8]) -> Vec<u8> {
        mcap::read::LinearReader::new(bytes)
            .expect("linear reader")
            .map(|record| record.expect("record").opcode())
            .collect()
    }

    /// Asserts the standardized data-section layout: a metadata record precedes the first message
    /// chunk, and an attachment record follows the chunks but stays within the data section.
    fn assert_standard_placement(output: &[u8]) {
        use mcap::records::op;
        let opcodes = top_level_opcodes(output);
        let position = |target: u8| {
            opcodes
                .iter()
                .position(|&opcode| opcode == target)
                .unwrap_or_else(|| panic!("expected a record with opcode {target:#04x}"))
        };
        let metadata = position(op::METADATA);
        let first_chunk = position(op::CHUNK);
        let attachment = position(op::ATTACHMENT);
        let data_end = position(op::DATA_END);
        assert!(
            metadata < first_chunk,
            "metadata should be written before the first message chunk"
        );
        assert!(
            first_chunk < attachment,
            "attachments should be written after the message chunks"
        );
        assert!(
            attachment < data_end,
            "attachments should be written inside the data section"
        );
    }

    #[test]
    fn indexed_passthrough_includes_messages_metadata_and_attachments() {
        let input = write_filter_test_input(true, false);
        let opts = ResolvedOptions {
            output: None,
            include_topics: Vec::new(),
            exclude_topics: Vec::new(),
            last_per_channel_topics: Vec::new(),
            start: 0,
            end: u64::MAX,
            include_metadata: true,
            include_attachments: true,
            compression: Some(mcap::Compression::Lz4),
            chunk_size: mcap::WriteOptions::DEFAULT_CHUNK_SIZE,
            use_chunks: true,
        };
        let output = run_filter(&input, &opts);
        let stats = analyze_output(&output);
        assert_eq!(stats.topic_counts["camera_a"], 100);
        assert_eq!(stats.topic_counts["camera_b"], 100);
        assert_eq!(stats.topic_counts["radar_a"], 100);
        assert_eq!(stats.metadata_count, 1);
        assert_eq!(stats.attachment_count, 1);
    }

    #[test]
    fn indexed_filtering_respects_exclude_topic_and_time() {
        let input = write_filter_test_input(true, false);
        let opts = ResolvedOptions {
            output: None,
            include_topics: Vec::new(),
            exclude_topics: vec![Regex::new("^radar_a$").expect("regex")],
            last_per_channel_topics: Vec::new(),
            start: 10,
            end: 20,
            include_metadata: false,
            include_attachments: false,
            compression: Some(mcap::Compression::Lz4),
            chunk_size: mcap::WriteOptions::DEFAULT_CHUNK_SIZE,
            use_chunks: true,
        };
        let output = run_filter(&input, &opts);
        let stats = analyze_output(&output);
        assert_eq!(stats.topic_counts["camera_a"], 10);
        assert_eq!(stats.topic_counts["camera_b"], 10);
        assert!(!stats.topic_counts.contains_key("radar_a"));
        // --exclude-metadata / --exclude-attachments drop those records from the output.
        assert_eq!(stats.metadata_count, 0);
        assert_eq!(stats.attachment_count, 0);
    }

    #[test]
    fn linear_filtering_respects_topic_and_time() {
        let input = write_filter_test_input(false, false);
        let opts = ResolvedOptions {
            output: None,
            include_topics: vec![Regex::new("^camera_.*$").expect("regex")],
            exclude_topics: Vec::new(),
            last_per_channel_topics: Vec::new(),
            start: 0,
            end: 49,
            include_metadata: false,
            include_attachments: true,
            compression: Some(mcap::Compression::Lz4),
            chunk_size: mcap::WriteOptions::DEFAULT_CHUNK_SIZE,
            use_chunks: true,
        };
        let output = run_filter(&input, &opts);
        let stats = analyze_output(&output);
        assert_eq!(stats.topic_counts["camera_a"], 49);
        assert_eq!(stats.topic_counts["camera_b"], 49);
        assert!(!stats.topic_counts.contains_key("radar_a"));
        // The attachment (log_time 50) is kept even though it is outside the [0, 49) message
        // window: the time range applies to messages, not auxiliary records.
        assert_eq!(stats.attachment_count, 1);
        // Metadata is excluded here (include_metadata = false), so none is written.
        assert_eq!(stats.metadata_count, 0);
    }

    #[test]
    fn indexed_last_per_channel_adds_one_pre_start_message_per_matching_topic() {
        let input = write_filter_test_input(true, false);
        let opts = ResolvedOptions {
            output: None,
            include_topics: Vec::new(),
            exclude_topics: Vec::new(),
            last_per_channel_topics: vec![Regex::new("^camera_.*$").expect("regex")],
            start: 50,
            end: u64::MAX,
            include_metadata: false,
            include_attachments: false,
            compression: Some(mcap::Compression::Zstd),
            chunk_size: mcap::WriteOptions::DEFAULT_CHUNK_SIZE,
            use_chunks: true,
        };
        let output = run_filter(&input, &opts);
        let stats = analyze_output(&output);
        assert_eq!(stats.topic_counts["camera_a"], 51);
        assert_eq!(stats.topic_counts["camera_b"], 51);
        assert_eq!(stats.topic_counts["radar_a"], 50);
        for pair in stats.log_times.windows(2) {
            assert!(pair[0] <= pair[1], "messages must be log-time ordered");
        }
    }

    #[test]
    fn last_per_channel_errors_for_non_indexed_inputs() {
        let input = write_filter_test_input(false, true);
        let opts = ResolvedOptions {
            output: None,
            include_topics: Vec::new(),
            exclude_topics: Vec::new(),
            last_per_channel_topics: vec![Regex::new("^camera_.*$").expect("regex")],
            start: 50,
            end: u64::MAX,
            include_metadata: false,
            include_attachments: false,
            compression: Some(mcap::Compression::Zstd),
            chunk_size: mcap::WriteOptions::DEFAULT_CHUNK_SIZE,
            use_chunks: true,
        };
        let mut output = Cursor::new(Vec::new());
        let err = filter_to_writer(&input, &mut output, &opts, false)
            .expect_err("last-per should be rejected");
        assert!(err
            .to_string()
            .contains("including last-per-channel topics is not supported for non-indexed input"));
    }

    #[test]
    fn chunked_summaryless_input_falls_back_to_linear_filtering() {
        let input = write_filter_test_input(true, true);
        let opts = ResolvedOptions {
            output: None,
            include_topics: vec![Regex::new("^camera_.*$").expect("regex")],
            exclude_topics: Vec::new(),
            last_per_channel_topics: Vec::new(),
            start: 20,
            end: 25,
            include_metadata: false,
            include_attachments: false,
            compression: Some(mcap::Compression::Lz4),
            chunk_size: mcap::WriteOptions::DEFAULT_CHUNK_SIZE,
            use_chunks: true,
        };
        let output = run_filter(&input, &opts);
        let stats = analyze_output(&output);
        assert_eq!(stats.topic_counts["camera_a"], 5);
        assert_eq!(stats.topic_counts["camera_b"], 5);
        assert!(!stats.topic_counts.contains_key("radar_a"));
    }

    #[test]
    fn chunked_input_without_chunk_index_falls_back_to_linear_filtering_on_unknown_schema() {
        let input = write_filter_test_input_with_options(true, false, true, false, true, false);
        let opts = ResolvedOptions {
            output: None,
            include_topics: vec![Regex::new("^camera_.*$").expect("regex")],
            exclude_topics: Vec::new(),
            last_per_channel_topics: Vec::new(),
            start: 20,
            end: 25,
            include_metadata: false,
            include_attachments: false,
            compression: Some(mcap::Compression::Lz4),
            chunk_size: mcap::WriteOptions::DEFAULT_CHUNK_SIZE,
            use_chunks: true,
        };
        let output = run_filter(&input, &opts);
        let stats = analyze_output(&output);
        assert_eq!(stats.topic_counts["camera_a"], 5);
        assert_eq!(stats.topic_counts["camera_b"], 5);
        assert!(!stats.topic_counts.contains_key("radar_a"));
    }

    #[test]
    fn chunk_indexed_input_without_repeated_channels_errors() {
        let input = write_filter_test_input_with_summary_repeats(true, false, false, false);
        let opts = ResolvedOptions {
            output: None,
            include_topics: vec![Regex::new("^camera_.*$").expect("regex")],
            exclude_topics: Vec::new(),
            last_per_channel_topics: Vec::new(),
            start: 20,
            end: 25,
            include_metadata: false,
            include_attachments: false,
            compression: Some(mcap::Compression::Lz4),
            chunk_size: mcap::WriteOptions::DEFAULT_CHUNK_SIZE,
            use_chunks: true,
        };
        let mut output = Cursor::new(Vec::new());
        let err = filter_to_writer(&input, &mut output, &opts, false)
            .expect_err("invalid indexed summary should fail");
        assert!(err.to_string().contains("mcap recover"));
    }

    #[test]
    fn chunk_indexed_input_without_channels_or_message_indexes_errors() {
        let input = write_filter_test_input_with_options(true, false, false, false, false, true);
        let opts = ResolvedOptions {
            output: None,
            include_topics: vec![Regex::new("^camera_.*$").expect("regex")],
            exclude_topics: Vec::new(),
            last_per_channel_topics: Vec::new(),
            start: 20,
            end: 25,
            include_metadata: false,
            include_attachments: false,
            compression: Some(mcap::Compression::Lz4),
            chunk_size: mcap::WriteOptions::DEFAULT_CHUNK_SIZE,
            use_chunks: true,
        };
        let mut output = Cursor::new(Vec::new());
        let err = filter_to_writer(&input, &mut output, &opts, false)
            .expect_err("invalid indexed summary should fail");
        assert!(err.to_string().contains("mcap recover"));
    }

    #[test]
    fn chunk_indexed_input_without_repeated_schemas_errors() {
        let input = write_filter_test_input_with_summary_repeats(true, false, true, false);
        let opts = ResolvedOptions {
            output: None,
            include_topics: Vec::new(),
            exclude_topics: Vec::new(),
            last_per_channel_topics: Vec::new(),
            start: 0,
            end: u64::MAX,
            include_metadata: false,
            include_attachments: false,
            compression: Some(mcap::Compression::Lz4),
            chunk_size: mcap::WriteOptions::DEFAULT_CHUNK_SIZE,
            use_chunks: true,
        };
        let mut output = Cursor::new(Vec::new());
        let err = filter_to_writer(&input, &mut output, &opts, false)
            .expect_err("invalid indexed summary should fail");
        assert!(err.to_string().contains("mcap recover"));
    }

    #[test]
    fn filter_stamps_cli_writer_library() {
        let input = write_filter_test_input(true, false);
        let opts = ResolvedOptions {
            output: None,
            include_topics: Vec::new(),
            exclude_topics: Vec::new(),
            last_per_channel_topics: Vec::new(),
            start: 0,
            end: u64::MAX,
            include_metadata: false,
            include_attachments: false,
            compression: Some(mcap::Compression::Zstd),
            chunk_size: mcap::WriteOptions::DEFAULT_CHUNK_SIZE,
            use_chunks: true,
        };
        // The CLI is the writer of the output, so it stamps its own identity, not the source's.
        let output = run_filter(&input, &opts);
        let library = crate::parse::read_header(&output)
            .expect("read header")
            .expect("header present")
            .library;
        assert_eq!(library, *crate::cli::LIBRARY_IDENTIFIER);
    }

    #[test]
    fn rewrite_options_support_unchunked_output() {
        let input = write_filter_test_input(true, false);
        let mut input_path = std::env::temp_dir();
        input_path.push(format!(
            "mcap-cli-filter-unchunked-input-{pid}-{nonce}.mcap",
            pid = std::process::id(),
            nonce = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time should be after epoch")
                .as_nanos()
        ));
        std::fs::write(&input_path, &input).expect("write input");

        let mut output_path = std::env::temp_dir();
        output_path.push(format!(
            "mcap-cli-filter-unchunked-output-{pid}-{nonce}.mcap",
            pid = std::process::id(),
            nonce = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time should be after epoch")
                .as_nanos()
        ));

        let mut options =
            super::RewriteOptions::new(Some(input_path.clone()), Some(output_path.clone()), 1024);
        options.include_metadata = true;
        options.include_attachments = true;
        options.use_chunks = false;
        options.output_compression = "none".to_string();

        super::run(options, crate::source::SourceOptions::default())
            .expect("rewrite should succeed");
        let output = std::fs::read(&output_path).expect("read output");
        let summary = mcap::Summary::read(&output)
            .expect("summary read should succeed")
            .expect("summary should exist");
        assert!(
            summary.chunk_indexes.is_empty(),
            "unchunked output should not contain chunk indexes"
        );
        let stats = analyze_output(&output);
        assert_eq!(stats.topic_counts["camera_a"], 100);
        assert_eq!(stats.topic_counts["camera_b"], 100);
        assert_eq!(stats.topic_counts["radar_a"], 100);
        assert_eq!(stats.metadata_count, 1);
        assert_eq!(stats.attachment_count, 1);

        let _ = std::fs::remove_file(input_path);
        let _ = std::fs::remove_file(output_path);
    }

    #[test]
    fn run_rejects_same_input_and_output_without_truncating() {
        let input = write_filter_test_input(true, false);
        let dir = tempfile::TempDir::new().expect("temp dir");
        let path = dir.path().join("same-path.mcap");
        std::fs::write(&path, &input).expect("write input");

        let options = super::RewriteOptions::new(
            Some(path.clone()),
            Some(path.clone()),
            mcap::WriteOptions::DEFAULT_CHUNK_SIZE,
        );
        let err = super::run(options, crate::source::SourceOptions::default())
            .expect_err("same input/output should fail");

        assert!(err.to_string().contains("input and output paths"));
        assert_eq!(std::fs::read(&path).expect("read input"), input);
    }

    #[test]
    fn indexed_attachments_are_not_time_filtered() {
        // The fixture's attachment has log_time 50; a [0, 10) window excludes it from the message
        // range but the attachment is still kept.
        let input = write_filter_test_input(true, false);
        let output = run_filter(&input, &time_windowed_options(0, 10));
        let stats = analyze_output(&output);
        assert_eq!(stats.attachment_count, 1);
        assert_eq!(stats.metadata_count, 1);
    }

    #[test]
    fn linear_attachments_are_not_time_filtered() {
        let input = write_filter_test_input(false, true);
        let output = run_filter(&input, &time_windowed_options(0, 10));
        let stats = analyze_output(&output);
        assert_eq!(stats.attachment_count, 1);
        assert_eq!(stats.metadata_count, 1);
    }

    #[test]
    fn indexed_path_keeps_metadata_and_attachments_missing_from_summary() {
        let input = write_chunk_indexed_without_aux_indexes();
        // Sanity: this takes the indexed path (chunk indexes present) but the summary does not
        // index the metadata/attachment records.
        let summary = mcap::Summary::read(&input)
            .expect("summary read")
            .expect("summary present");
        assert!(!summary.chunk_indexes.is_empty());
        assert!(summary.metadata_indexes.is_empty());
        assert!(summary.attachment_indexes.is_empty());

        let output = run_filter(&input, &include_all_options());
        let stats = analyze_output(&output);
        // Lossless: the records are recovered via the scan fallback rather than dropped.
        assert_eq!(stats.metadata_count, 1);
        assert_eq!(stats.attachment_count, 1);
    }

    #[test]
    fn indexed_filter_places_metadata_first_and_attachments_last() {
        let input = write_filter_test_input(true, false);
        let output = run_filter(&input, &include_all_options());
        assert_standard_placement(&output);
    }

    #[test]
    fn linear_filter_places_metadata_first_and_attachments_last() {
        let input = write_filter_test_input(false, true);
        let output = run_filter(&input, &include_all_options());
        assert_standard_placement(&output);
    }
}
