//! The multi-input `merge` pipeline: k-way merge the messages of several MCAP inputs into one
//! output, ordered by log time. Shares the writer setup, summary/index inspection, and metadata /
//! attachment traversals with the single-input [`super::engine`] via [`super::common`]; the parts
//! unique to merging live here (cross-input schema/channel remapping and coalescing, metadata
//! deduplication, and the k-way merge heap).
use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap, HashMap};
use std::io::{Seek, Write};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use mcap::records::MessageHeader;

use super::common;
use super::common::InputRef;
use crate::cli::CoalesceChannels;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SchemaKey {
    name: String,
    encoding: String,
    data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ChannelKey {
    schema_id: u16,
    topic: String,
    message_encoding: String,
    metadata: Vec<(String, String)>,
}

struct IndexedInputMessageReader<'a> {
    input_idx: usize,
    input_order: usize,
    name: &'a str,
    data: &'a [u8],
    summary: Box<mcap::Summary>,
    reader: mcap::sans_io::IndexedReader,
}

struct MaterializedInputMessages {
    messages: Vec<PendingMessage>,
    next: usize,
}

enum MergeMessageStream<'a> {
    Indexed(IndexedInputMessageReader<'a>),
    Materialized(MaterializedInputMessages),
}

#[derive(Debug, Clone)]
struct PendingMessage {
    input_idx: usize,
    input_order: usize,
    input_channel_id: u16,
    channel: Arc<mcap::Channel<'static>>,
    sequence: u32,
    log_time: u64,
    publish_time: u64,
    data: Vec<u8>,
}

impl PendingMessage {
    fn new(
        input_idx: usize,
        input_order: usize,
        channel: Arc<mcap::Channel<'static>>,
        header: MessageHeader,
        data: Vec<u8>,
    ) -> Self {
        Self {
            input_idx,
            input_order,
            input_channel_id: header.channel_id,
            channel,
            sequence: header.sequence,
            log_time: header.log_time,
            publish_time: header.publish_time,
            data,
        }
    }
}

impl PartialEq for PendingMessage {
    fn eq(&self, other: &Self) -> bool {
        self.log_time == other.log_time
            && self.input_idx == other.input_idx
            && self.input_order == other.input_order
    }
}

impl Eq for PendingMessage {}

impl PartialOrd for PendingMessage {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PendingMessage {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reversed so the `BinaryHeap` (a max-heap) yields the smallest log time first. Ties break
        // by input index, then per-input read order, so a stable log-time order is produced with
        // earlier inputs (and earlier records within an input) winning.
        other
            .log_time
            .cmp(&self.log_time)
            .then_with(|| other.input_idx.cmp(&self.input_idx))
            .then_with(|| other.input_order.cmp(&self.input_order))
    }
}

#[derive(Default)]
struct IdMaps {
    schema_ids: HashMap<(usize, u16), u16>,
    schema_ids_by_content: HashMap<SchemaKey, u16>,
    channel_ids: HashMap<(usize, u16), u16>,
    channel_ids_by_content: HashMap<ChannelKey, u16>,
    next_output_channel_id: u16,
}

/// Writes the merged data section into an existing writer: metadata (deduplicated across inputs)
/// first, then the k-way log-time-merged messages, then attachments. The caller owns writer
/// creation, the profile, and `finish` (see [`super::engine::run`]).
pub(super) fn write_merged<W: Write + Seek>(
    writer: &mut mcap::Writer<W>,
    inputs: &[InputRef<'_>],
    coalesce_channels: CoalesceChannels,
    dedup_metadata: bool,
    allow_duplicate_metadata: bool,
) -> Result<()> {
    let summaries = inputs
        .iter()
        // Treat summary lookup as best effort and fall back to linear scans when
        // summary parsing fails.
        .map(|input| mcap::Summary::read(input.data).unwrap_or_default())
        .collect::<Vec<_>>();

    // Metadata is written first, before any messages (see module docs).
    let mut metadata_state = common::MetadataState::default();
    for (idx, input) in inputs.iter().enumerate() {
        common::for_each_metadata(input.data, summaries[idx].as_ref(), |metadata| {
            common::write_metadata_record(
                writer,
                &mut metadata_state,
                metadata,
                dedup_metadata,
                allow_duplicate_metadata,
            )
        })
        .with_context(|| format!("failed to read metadata from '{}'", input.name))?;
    }

    merge_messages(writer, inputs, &summaries, coalesce_channels)?;

    // Attachments are written last, after all messages (see module docs).
    for (idx, input) in inputs.iter().enumerate() {
        common::for_each_attachment(input.data, summaries[idx].as_ref(), |attachment| {
            writer
                .attach(&attachment)
                .with_context(|| format!("failed to write attachment from '{}'", input.name))?;
            Ok(())
        })
        .with_context(|| format!("failed to read attachments from '{}'", input.name))?;
    }

    Ok(())
}

impl<'a> IndexedInputMessageReader<'a> {
    fn new(input_idx: usize, input: &InputRef<'a>, summary: mcap::Summary) -> Result<Self> {
        let reader = mcap::sans_io::IndexedReader::new_with_options(
            &summary,
            mcap::sans_io::IndexedReaderOptions::new()
                .with_order(mcap::sans_io::indexed_reader::ReadOrder::LogTime),
        )
        .with_context(|| format!("failed to create indexed reader for '{}'", input.name))?;

        Ok(Self {
            input_idx,
            input_order: 0,
            name: input.name,
            data: input.data,
            summary: Box::new(summary),
            reader,
        })
    }

    fn next_message(&mut self) -> Result<Option<PendingMessage>> {
        while let Some(event) = self.reader.next_event() {
            match event.with_context(|| format!("failed to read indexed '{}'", self.name))? {
                mcap::sans_io::IndexedReadEvent::ReadChunkRequest { offset, length } => {
                    common::service_chunk_request(&mut self.reader, self.data, offset, length)?;
                }
                mcap::sans_io::IndexedReadEvent::Message { header, data } => {
                    let channel = self
                        .summary
                        .channels
                        .get(&header.channel_id)
                        .cloned()
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "message references unknown channel {} in '{}'",
                                header.channel_id,
                                self.name
                            )
                        })?;
                    let input_order = self.input_order;
                    self.input_order += 1;
                    return Ok(Some(PendingMessage::new(
                        self.input_idx,
                        input_order,
                        channel,
                        header,
                        data.to_vec(),
                    )));
                }
            }
        }

        Ok(None)
    }
}

impl MaterializedInputMessages {
    fn new(input_idx: usize, input: &InputRef<'_>) -> Result<Self> {
        // A summaryless or incompletely-indexed input can't be read in log-time order on the fly,
        // so read every message (in stored order) and sort. `mcap::MessageStream` resolves each
        // message's channel and applies the same schema/channel conflict checks merge needs.
        let stream = mcap::MessageStream::new(input.data)
            .with_context(|| format!("failed to stream records from '{}'", input.name))?;
        let mut messages = Vec::new();
        for (input_order, message) in stream.enumerate() {
            let message = message
                .with_context(|| format!("failed reading messages from '{}'", input.name))?;
            let header = MessageHeader {
                channel_id: message.channel.id,
                sequence: message.sequence,
                log_time: message.log_time,
                publish_time: message.publish_time,
            };
            messages.push(PendingMessage::new(
                input_idx,
                input_order,
                message.channel,
                header,
                message.data.into_owned(),
            ));
        }
        messages.sort_by_key(|message| (message.log_time, message.input_order));

        Ok(Self { messages, next: 0 })
    }

    fn next_message(&mut self) -> Option<PendingMessage> {
        let message = self.messages.get(self.next).cloned()?;
        self.next += 1;
        Some(message)
    }
}

impl MergeMessageStream<'_> {
    fn next_message(&mut self) -> Result<Option<PendingMessage>> {
        match self {
            MergeMessageStream::Indexed(reader) => reader.next_message(),
            MergeMessageStream::Materialized(messages) => Ok(messages.next_message()),
        }
    }
}

fn merge_messages<W: Write + Seek>(
    writer: &mut mcap::Writer<W>,
    inputs: &[InputRef<'_>],
    summaries: &[Option<mcap::Summary>],
    coalesce_channels: CoalesceChannels,
) -> Result<()> {
    let mut id_maps = IdMaps {
        next_output_channel_id: 1,
        ..IdMaps::default()
    };

    let mut streams = Vec::<MergeMessageStream<'_>>::with_capacity(inputs.len());
    for (input_idx, input) in inputs.iter().enumerate() {
        if let Some(summary) = summaries[input_idx].as_ref() {
            if !summary.chunk_indexes.is_empty()
                && common::summary_supports_indexed_read(summary)
                && common::summary_indexes_all_messages(input.data, summary)
            {
                streams.push(MergeMessageStream::Indexed(IndexedInputMessageReader::new(
                    input_idx,
                    input,
                    summary.clone(),
                )?));
                continue;
            }
        }
        // Without usable message indexes, guaranteeing log-time order requires sorting this input.
        streams.push(MergeMessageStream::Materialized(
            MaterializedInputMessages::new(input_idx, input)?,
        ));
    }

    let mut heap = BinaryHeap::<PendingMessage>::new();
    for stream in &mut streams {
        if let Some(message) = stream.next_message()? {
            heap.push(message);
        }
    }

    while let Some(message) = heap.pop() {
        let input_idx = message.input_idx;
        let output_channel_id = ensure_output_channel_id(
            &mut id_maps,
            writer,
            input_idx,
            message.input_channel_id,
            &message.channel,
            coalesce_channels,
        )?;

        writer.write_to_known_channel(
            &MessageHeader {
                channel_id: output_channel_id,
                sequence: message.sequence,
                log_time: message.log_time,
                publish_time: message.publish_time,
            },
            &message.data,
        )?;

        if let Some(next) = streams[input_idx].next_message()? {
            heap.push(next);
        }
    }

    Ok(())
}

fn ensure_output_channel_id<W: Write + Seek>(
    id_maps: &mut IdMaps,
    writer: &mut mcap::Writer<W>,
    input_idx: usize,
    input_channel_id: u16,
    channel: &Arc<mcap::Channel<'_>>,
    coalesce_channels: CoalesceChannels,
) -> Result<u16> {
    if let Some(output_channel_id) = id_maps.channel_ids.get(&(input_idx, input_channel_id)) {
        return Ok(*output_channel_id);
    }

    let output_schema_id = if let Some(schema) = channel.schema.as_ref() {
        ensure_output_schema_id(id_maps, writer, input_idx, schema)?
    } else {
        0
    };

    if coalesce_channels != CoalesceChannels::None {
        let channel_key = make_channel_key(
            output_schema_id,
            &channel.topic,
            &channel.message_encoding,
            &channel.metadata,
            coalesce_channels,
        );
        if let Some(output_channel_id) = id_maps.channel_ids_by_content.get(&channel_key).copied() {
            id_maps
                .channel_ids
                .insert((input_idx, input_channel_id), output_channel_id);
            return Ok(output_channel_id);
        }

        let output_channel_id = reserve_next_channel_id(id_maps)?;
        writer.add_channel_with_id(
            output_channel_id,
            output_schema_id,
            &channel.topic,
            &channel.message_encoding,
            &channel.metadata,
        )?;
        id_maps
            .channel_ids
            .insert((input_idx, input_channel_id), output_channel_id);
        id_maps
            .channel_ids_by_content
            .insert(channel_key, output_channel_id);
        return Ok(output_channel_id);
    }

    let output_channel_id = reserve_next_channel_id(id_maps)?;
    writer.add_channel_with_id(
        output_channel_id,
        output_schema_id,
        &channel.topic,
        &channel.message_encoding,
        &channel.metadata,
    )?;
    id_maps
        .channel_ids
        .insert((input_idx, input_channel_id), output_channel_id);
    Ok(output_channel_id)
}

fn ensure_output_schema_id<W: Write + Seek>(
    id_maps: &mut IdMaps,
    writer: &mut mcap::Writer<W>,
    input_idx: usize,
    schema: &Arc<mcap::Schema<'_>>,
) -> Result<u16> {
    if let Some(output_schema_id) = id_maps.schema_ids.get(&(input_idx, schema.id)) {
        return Ok(*output_schema_id);
    }

    let key = SchemaKey {
        name: schema.name.clone(),
        encoding: schema.encoding.clone(),
        data: schema.data.clone().into_owned(),
    };

    let output_schema_id = if let Some(existing_schema_id) = id_maps.schema_ids_by_content.get(&key)
    {
        *existing_schema_id
    } else {
        let id = writer.add_schema(&schema.name, &schema.encoding, schema.data.as_ref())?;
        id_maps.schema_ids_by_content.insert(key, id);
        id
    };

    id_maps
        .schema_ids
        .insert((input_idx, schema.id), output_schema_id);
    Ok(output_schema_id)
}

fn reserve_next_channel_id(id_maps: &mut IdMaps) -> Result<u16> {
    if id_maps.next_output_channel_id == 0 {
        bail!("cannot merge more than 65535 channels");
    }
    let id = id_maps.next_output_channel_id;
    id_maps.next_output_channel_id = id_maps.next_output_channel_id.wrapping_add(1);
    Ok(id)
}

fn make_channel_key(
    schema_id: u16,
    topic: &str,
    message_encoding: &str,
    metadata: &BTreeMap<String, String>,
    coalesce_channels: CoalesceChannels,
) -> ChannelKey {
    let metadata_entries = if coalesce_channels == CoalesceChannels::Force {
        Vec::new()
    } else {
        metadata
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    };

    ChannelKey {
        schema_id,
        topic: topic.to_string(),
        message_encoding: message_encoding.to_string(),
        metadata: metadata_entries,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::Cursor;

    use super::*;

    #[derive(Debug, Clone)]
    struct TestMessage {
        channel_id: u16,
        topic: String,
        metadata: BTreeMap<String, String>,
        log_time: u64,
        payload: Vec<u8>,
    }

    fn build_mcap(
        profile: &str,
        messages: &[TestMessage],
        metadata_records: &[mcap::records::Metadata],
        attachments: &[mcap::Attachment<'_>],
        emit_attachment_indexes: bool,
        emit_metadata_indexes: bool,
    ) -> Vec<u8> {
        build_mcap_with_options(
            profile,
            messages,
            metadata_records,
            attachments,
            emit_attachment_indexes,
            emit_metadata_indexes,
            true,
        )
    }

    fn build_mcap_with_options(
        profile: &str,
        messages: &[TestMessage],
        metadata_records: &[mcap::records::Metadata],
        attachments: &[mcap::Attachment<'_>],
        emit_attachment_indexes: bool,
        emit_metadata_indexes: bool,
        emit_statistics: bool,
    ) -> Vec<u8> {
        let mut output = Cursor::new(Vec::<u8>::new());
        {
            let mut writer = mcap::WriteOptions::new()
                .profile(profile)
                .library("test-recorder/0.0")
                .emit_attachment_indexes(emit_attachment_indexes)
                .emit_metadata_indexes(emit_metadata_indexes)
                .emit_statistics(emit_statistics)
                .create(&mut output)
                .expect("writer");

            let mut channels = BTreeMap::<u16, std::sync::Arc<mcap::Channel<'static>>>::new();
            let schema = std::sync::Arc::new(mcap::Schema {
                id: 1,
                name: "example".to_string(),
                encoding: "jsonschema".to_string(),
                data: std::borrow::Cow::Borrowed(br#"{"type":"object"}"#),
            });

            for message in messages {
                let channel = channels
                    .entry(message.channel_id)
                    .or_insert_with(|| {
                        std::sync::Arc::new(mcap::Channel {
                            id: message.channel_id,
                            topic: message.topic.clone(),
                            schema: Some(schema.clone()),
                            message_encoding: "json".to_string(),
                            metadata: message.metadata.clone(),
                        })
                    })
                    .clone();
                writer
                    .write(&mcap::Message {
                        channel,
                        sequence: message.log_time as u32,
                        log_time: message.log_time,
                        publish_time: message.log_time,
                        data: std::borrow::Cow::Borrowed(message.payload.as_slice()),
                    })
                    .expect("write message");
            }

            for metadata in metadata_records {
                writer.write_metadata(metadata).expect("write metadata");
            }
            for attachment in attachments {
                writer.attach(attachment).expect("write attachment");
            }

            writer.finish().expect("finish");
        }
        output.into_inner()
    }

    fn build_non_indexed_mcap(profile: &str, messages: &[TestMessage]) -> Vec<u8> {
        let mut output = Cursor::new(Vec::<u8>::new());
        {
            let mut writer = mcap::WriteOptions::new()
                .profile(profile)
                .library("test-recorder/0.0")
                .use_chunks(false)
                .emit_summary_records(false)
                .emit_summary_offsets(false)
                .create(&mut output)
                .expect("writer");
            let schema_id = writer
                .add_schema("example", "jsonschema", br#"{"type":"object"}"#)
                .expect("schema");
            let mut channels = BTreeMap::new();

            for message in messages {
                let channel_id = *channels.entry(message.channel_id).or_insert_with(|| {
                    writer
                        .add_channel(schema_id, &message.topic, "json", &message.metadata)
                        .expect("channel")
                });
                writer
                    .write_to_known_channel(
                        &MessageHeader {
                            channel_id,
                            sequence: message.log_time as u32,
                            log_time: message.log_time,
                            publish_time: message.log_time,
                        },
                        message.payload.as_slice(),
                    )
                    .expect("write message");
            }

            writer.finish().expect("finish");
        }
        output.into_inner()
    }

    fn build_indexed_mcap_with_loose_message() -> Vec<u8> {
        let mut output = Cursor::new(Vec::<u8>::new());
        let channel_id;
        {
            let mut writer = mcap::WriteOptions::new()
                .emit_summary_offsets(false)
                .calculate_data_section_crc(false)
                .calculate_summary_section_crc(false)
                .calculate_chunk_crcs(false)
                .library("test-recorder/0.0")
                .create(&mut output)
                .expect("writer");
            let schema_id = writer
                .add_schema("example", "jsonschema", br#"{"type":"object"}"#)
                .expect("schema");
            channel_id = writer
                .add_channel(schema_id, "/mixed", "json", &BTreeMap::new())
                .expect("channel");
            writer
                .write_to_known_channel(
                    &MessageHeader {
                        channel_id,
                        sequence: 1,
                        log_time: 10,
                        publish_time: 10,
                    },
                    &[10],
                )
                .expect("chunked message");
            writer.finish().expect("finish");
        }

        let mut bytes = output.into_inner();
        let loose_message = message_record(channel_id, 2, 1, &[1]);
        let data_end_offset = record_offset(&bytes, mcap::records::op::DATA_END);
        bytes.splice(
            data_end_offset..data_end_offset,
            loose_message.iter().copied(),
        );
        patch_footer_summary_start(&mut bytes, loose_message.len() as u64);
        patch_statistics_message_count(&mut bytes, 2);
        bytes
    }

    fn message_record(channel_id: u16, sequence: u32, log_time: u64, payload: &[u8]) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&channel_id.to_le_bytes());
        body.extend_from_slice(&sequence.to_le_bytes());
        body.extend_from_slice(&log_time.to_le_bytes());
        body.extend_from_slice(&log_time.to_le_bytes());
        body.extend_from_slice(payload);
        wrap_record(mcap::records::op::MESSAGE, &body)
    }

    fn wrap_record(opcode: u8, body: &[u8]) -> Vec<u8> {
        let mut record = Vec::with_capacity(9 + body.len());
        record.push(opcode);
        record.extend_from_slice(&(body.len() as u64).to_le_bytes());
        record.extend_from_slice(body);
        record
    }

    fn record_offset(bytes: &[u8], target_opcode: u8) -> usize {
        let mut offset = mcap::MAGIC.len();
        let records_end = bytes.len() - mcap::MAGIC.len();
        while offset < records_end {
            let opcode = bytes[offset];
            let length =
                u64::from_le_bytes(bytes[offset + 1..offset + 9].try_into().unwrap()) as usize;
            if opcode == target_opcode {
                return offset;
            }
            offset += 9 + length;
        }
        panic!("record opcode 0x{target_opcode:02x} not found");
    }

    fn patch_footer_summary_start(bytes: &mut [u8], delta: u64) {
        let offset = record_offset(bytes, mcap::records::op::FOOTER) + 9;
        let current = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        bytes[offset..offset + 8].copy_from_slice(&(current + delta).to_le_bytes());
    }

    fn patch_statistics_message_count(bytes: &mut [u8], message_count: u64) {
        let offset = record_offset(bytes, mcap::records::op::STATISTICS) + 9;
        bytes[offset..offset + 8].copy_from_slice(&message_count.to_le_bytes());
    }

    /// Runs the merge phase over in-memory inputs (as `merge` configures it: dedup metadata on),
    /// returning the finished MCAP bytes.
    fn merge_bytes(
        inputs: &[(&str, &[u8])],
        coalesce_channels: CoalesceChannels,
        allow_duplicate_metadata: bool,
    ) -> Result<Vec<u8>> {
        let input_refs = inputs
            .iter()
            .map(|(name, data)| InputRef { name, data })
            .collect::<Vec<_>>();

        let mut output = Cursor::new(Vec::<u8>::new());
        {
            let mut writer = common::create_writer(
                &mut output,
                &common::WriterConfig {
                    profile: common::common_profile(&input_refs)?,
                    use_chunks: true,
                    chunk_size: 1024,
                    compression: None,
                    include_crc: true,
                },
                false,
            )?;
            write_merged(
                &mut writer,
                &input_refs,
                coalesce_channels,
                true,
                allow_duplicate_metadata,
            )?;
            writer.finish()?;
        }
        Ok(output.into_inner())
    }

    fn merge_command(files: Vec<&str>, output: Option<&str>) -> crate::cli::MergeCommand {
        crate::cli::MergeCommand {
            files: files.into_iter().map(Into::into).collect(),
            output: output.map(Into::into),
            output_file: None,
            compression: crate::cli::CompressionFormat::Zstd,
            chunk_size: 1024,
            no_crc: false,
            no_chunks: false,
            allow_duplicate_metadata: false,
            coalesce_channels: CoalesceChannels::Auto,
        }
    }

    #[test]
    fn run_rejects_remote_input_without_scan_opt_in() {
        let err = crate::rewrite::run(
            crate::rewrite::RewriteOptions::from(&merge_command(
                vec!["http://example.com/a.mcap"],
                Some("out.mcap"),
            )),
            crate::source::SourceOptions::default(),
        )
        .expect_err("remote merge input should require opt-in");

        assert!(err.to_string().contains("--allow-remote-scan"));
    }

    #[test]
    fn run_rejects_same_input_and_output_without_truncating() {
        let input = build_mcap("profile", &[], &[], &[], true, true);
        let dir = tempfile::TempDir::new().expect("temp dir");
        let path = dir.path().join("same-path.mcap");
        std::fs::write(&path, &input).expect("write input");
        let path_str = path.to_str().expect("utf-8 path");

        let err = crate::rewrite::run(
            crate::rewrite::RewriteOptions::from(&merge_command(vec![path_str], Some(path_str))),
            crate::source::SourceOptions::default(),
        )
        .expect_err("same input/output should fail");

        assert!(err.to_string().contains("input and output paths"));
        assert_eq!(std::fs::read(&path).expect("read input"), input);
    }

    /// Runs the `merge` command over the given input bytes written to a temp file and returns the
    /// output bytes. `configure` can set merge-only flags on the command.
    fn run_merge_command(
        input: &[u8],
        configure: impl FnOnce(&mut crate::cli::MergeCommand),
    ) -> Vec<u8> {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let input_path = dir.path().join("in.mcap");
        let output_path = dir.path().join("out.mcap");
        std::fs::write(&input_path, input).expect("write input");

        let mut command = merge_command(
            vec![input_path.to_str().expect("utf-8 path")],
            Some(output_path.to_str().expect("utf-8 path")),
        );
        configure(&mut command);

        crate::rewrite::run(
            crate::rewrite::RewriteOptions::from(&command),
            crate::source::SourceOptions::default(),
        )
        .expect("merge of one input should succeed");
        std::fs::read(&output_path).expect("read output")
    }

    #[test]
    fn merge_single_input_preserves_channel_ids_and_sorts_by_log_time() {
        // A single input takes the rewrite path: it keeps the input's channel IDs (rather than
        // renumbering them the way a multi-input merge does) while still applying the merge presets
        // (log-time order).
        let input = build_mcap(
            "profile",
            &[
                TestMessage {
                    channel_id: 7,
                    topic: "/demo".to_string(),
                    metadata: BTreeMap::new(),
                    log_time: 30,
                    payload: vec![1],
                },
                TestMessage {
                    channel_id: 7,
                    topic: "/demo".to_string(),
                    metadata: BTreeMap::new(),
                    log_time: 10,
                    payload: vec![2],
                },
                TestMessage {
                    channel_id: 7,
                    topic: "/demo".to_string(),
                    metadata: BTreeMap::new(),
                    log_time: 20,
                    payload: vec![3],
                },
            ],
            &[],
            &[],
            true,
            true,
        );

        let output = run_merge_command(&input, |_| {});

        let summary = mcap::Summary::read(&output)
            .expect("summary")
            .expect("present");
        assert!(
            summary.channels.contains_key(&7),
            "single-input merge preserves the input channel ID"
        );
        let log_times = mcap::MessageStream::new(&output)
            .expect("stream")
            .map(|message| message.expect("message").log_time)
            .collect::<Vec<_>>();
        assert_eq!(log_times, vec![10, 20, 30], "merge sorts by log time");
    }

    #[test]
    fn merge_single_input_deduplicates_identical_metadata() {
        // The merge preset also deduplicates metadata on the single-input path.
        let metadata = mcap::records::Metadata {
            name: "m".to_string(),
            metadata: BTreeMap::from([("k".to_string(), "v".to_string())]),
        };
        let input = build_mcap(
            "profile",
            &[TestMessage {
                channel_id: 1,
                topic: "/demo".to_string(),
                metadata: BTreeMap::new(),
                log_time: 1,
                payload: vec![1],
            }],
            &[metadata.clone(), metadata],
            &[],
            true,
            true,
        );

        let output = run_merge_command(&input, |command| command.allow_duplicate_metadata = true);

        let summary = mcap::Summary::read(&output)
            .expect("summary")
            .expect("present");
        assert_eq!(
            summary.metadata_indexes.len(),
            1,
            "merge-of-one deduplicates identical metadata records"
        );
    }

    #[test]
    fn merge_orders_messages_by_log_time_then_input_index() {
        let left = build_mcap(
            "profile",
            &[
                TestMessage {
                    channel_id: 1,
                    topic: "/left".to_string(),
                    metadata: BTreeMap::new(),
                    log_time: 5,
                    payload: vec![1],
                },
                TestMessage {
                    channel_id: 1,
                    topic: "/left".to_string(),
                    metadata: BTreeMap::new(),
                    log_time: 10,
                    payload: vec![3],
                },
            ],
            &[],
            &[],
            true,
            true,
        );
        let right = build_mcap(
            "profile",
            &[
                TestMessage {
                    channel_id: 1,
                    topic: "/right".to_string(),
                    metadata: BTreeMap::new(),
                    log_time: 3,
                    payload: vec![2],
                },
                TestMessage {
                    channel_id: 1,
                    topic: "/right".to_string(),
                    metadata: BTreeMap::new(),
                    log_time: 10,
                    payload: vec![4],
                },
            ],
            &[],
            &[],
            true,
            true,
        );

        let merged = merge_bytes(
            &[("left", left.as_slice()), ("right", right.as_slice())],
            CoalesceChannels::Auto,
            false,
        )
        .expect("merge");
        let mut ordered_messages = Vec::<(u64, String, Vec<u8>)>::new();
        for message in mcap::MessageStream::new(&merged).expect("stream") {
            let message = message.expect("message");
            ordered_messages.push((
                message.log_time,
                message.channel.topic.clone(),
                message.data.to_vec(),
            ));
        }
        assert_eq!(
            ordered_messages,
            vec![
                (3, "/right".to_string(), vec![2]),
                (5, "/left".to_string(), vec![1]),
                // Tie at log_time=10 resolves by input index: left before right.
                (10, "/left".to_string(), vec![3]),
                (10, "/right".to_string(), vec![4]),
            ]
        );
    }

    #[test]
    fn merge_sorts_unsorted_inputs_and_preserves_tie_order() {
        let left = build_mcap(
            "profile",
            &[
                TestMessage {
                    channel_id: 1,
                    topic: "/left".to_string(),
                    metadata: BTreeMap::new(),
                    log_time: 20,
                    payload: vec![5],
                },
                TestMessage {
                    channel_id: 1,
                    topic: "/left".to_string(),
                    metadata: BTreeMap::new(),
                    log_time: 5,
                    payload: vec![1],
                },
                TestMessage {
                    channel_id: 1,
                    topic: "/left".to_string(),
                    metadata: BTreeMap::new(),
                    log_time: 10,
                    payload: vec![2],
                },
                TestMessage {
                    channel_id: 1,
                    topic: "/left".to_string(),
                    metadata: BTreeMap::new(),
                    log_time: 10,
                    payload: vec![3],
                },
            ],
            &[],
            &[],
            true,
            true,
        );
        let right = build_mcap(
            "profile",
            &[
                TestMessage {
                    channel_id: 1,
                    topic: "/right".to_string(),
                    metadata: BTreeMap::new(),
                    log_time: 7,
                    payload: vec![6],
                },
                TestMessage {
                    channel_id: 1,
                    topic: "/right".to_string(),
                    metadata: BTreeMap::new(),
                    log_time: 10,
                    payload: vec![4],
                },
            ],
            &[],
            &[],
            true,
            true,
        );

        let merged = merge_bytes(
            &[("left", left.as_slice()), ("right", right.as_slice())],
            CoalesceChannels::Auto,
            false,
        )
        .expect("merge");
        let ordered_messages = mcap::MessageStream::new(&merged)
            .expect("stream")
            .map(|message| {
                let message = message.expect("message");
                (
                    message.log_time,
                    message.channel.topic.clone(),
                    message.data.to_vec(),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(
            ordered_messages,
            vec![
                (5, "/left".to_string(), vec![1]),
                (7, "/right".to_string(), vec![6]),
                (10, "/left".to_string(), vec![2]),
                (10, "/left".to_string(), vec![3]),
                (10, "/right".to_string(), vec![4]),
                (20, "/left".to_string(), vec![5]),
            ]
        );
    }

    #[test]
    fn merge_sorts_non_indexed_unsorted_inputs() {
        let left = build_non_indexed_mcap(
            "profile",
            &[
                TestMessage {
                    channel_id: 1,
                    topic: "/left".to_string(),
                    metadata: BTreeMap::new(),
                    log_time: 10,
                    payload: vec![2],
                },
                TestMessage {
                    channel_id: 1,
                    topic: "/left".to_string(),
                    metadata: BTreeMap::new(),
                    log_time: 1,
                    payload: vec![1],
                },
            ],
        );
        let right = build_non_indexed_mcap(
            "profile",
            &[TestMessage {
                channel_id: 1,
                topic: "/right".to_string(),
                metadata: BTreeMap::new(),
                log_time: 5,
                payload: vec![3],
            }],
        );

        let merged = merge_bytes(
            &[("left", left.as_slice()), ("right", right.as_slice())],
            CoalesceChannels::Auto,
            false,
        )
        .expect("merge");
        let ordered_log_times = mcap::MessageStream::new(&merged)
            .expect("stream")
            .map(|message| message.expect("message").log_time)
            .collect::<Vec<_>>();

        assert_eq!(ordered_log_times, vec![1, 5, 10]);
    }

    #[test]
    fn indexed_fast_path_requires_all_messages_in_indexes() {
        let input = build_mcap(
            "profile",
            &[TestMessage {
                channel_id: 1,
                topic: "/demo".to_string(),
                metadata: BTreeMap::new(),
                log_time: 1,
                payload: vec![1],
            }],
            &[],
            &[],
            true,
            true,
        );
        let mut summary = mcap::Summary::read(&input)
            .expect("summary")
            .expect("summary present");
        assert!(common::summary_indexes_all_messages(&input, &summary));

        summary.stats.as_mut().expect("stats present").message_count += 1;
        assert!(!common::summary_indexes_all_messages(&input, &summary));
    }

    #[test]
    fn merge_keeps_loose_messages_when_indexed_summary_is_incomplete() {
        let input = build_indexed_mcap_with_loose_message();

        let merged = merge_bytes(
            &[("mixed", input.as_slice())],
            CoalesceChannels::Auto,
            false,
        )
        .expect("merge");
        let messages = mcap::MessageStream::new(&merged)
            .expect("stream")
            .map(|message| {
                let message = message.expect("message");
                (message.log_time, message.data.to_vec())
            })
            .collect::<Vec<_>>();

        assert_eq!(messages, vec![(1, vec![1]), (10, vec![10])]);
    }

    #[test]
    fn merge_sets_empty_profile_when_inputs_disagree() {
        let a = build_mcap("a", &[], &[], &[], true, true);
        let b = build_mcap("b", &[], &[], &[], true, true);

        let merged = merge_bytes(
            &[("a", a.as_slice()), ("b", b.as_slice())],
            CoalesceChannels::Auto,
            false,
        )
        .expect("merge");

        let header = match mcap::read::LinearReader::new(&merged)
            .expect("reader")
            .next()
            .expect("header")
            .expect("record")
        {
            mcap::records::Record::Header(header) => header,
            _ => panic!("expected header"),
        };
        assert!(header.profile.is_empty());
    }

    #[test]
    fn merge_stamps_cli_writer_library() {
        let a = build_mcap("p", &[], &[], &[], true, true);
        let b = build_mcap("p", &[], &[], &[], true, true);

        let merged = merge_bytes(
            &[("a", a.as_slice()), ("b", b.as_slice())],
            CoalesceChannels::Auto,
            false,
        )
        .expect("merge");

        // The inputs' `test-recorder/0.0` library is overwritten with the CLI's own identity.
        let library = crate::parse::read_header(&merged)
            .expect("read header")
            .expect("header present")
            .library;
        assert_eq!(library, *crate::cli::LIBRARY_IDENTIFIER);
    }

    #[test]
    fn merge_rejects_duplicate_metadata_name_by_default() {
        let first = build_mcap(
            "p",
            &[],
            &[mcap::records::Metadata {
                name: "robot".to_string(),
                metadata: BTreeMap::from([(String::from("a"), String::from("1"))]),
            }],
            &[],
            true,
            true,
        );
        let second = build_mcap(
            "p",
            &[],
            &[mcap::records::Metadata {
                name: "robot".to_string(),
                metadata: BTreeMap::from([(String::from("a"), String::from("2"))]),
            }],
            &[],
            true,
            true,
        );

        let err = merge_bytes(
            &[("a", first.as_slice()), ("b", second.as_slice())],
            CoalesceChannels::Auto,
            false,
        )
        .expect_err("merge should fail");
        assert!(
            format!("{err:#}").contains("metadata name 'robot' was previously encountered"),
            "unexpected error: {err:#}"
        );
    }

    #[test]
    fn merge_allow_duplicate_metadata_deduplicates_identical_records() {
        let first = build_mcap(
            "p",
            &[],
            &[mcap::records::Metadata {
                name: "robot".to_string(),
                metadata: BTreeMap::from([(String::from("a"), String::from("1"))]),
            }],
            &[],
            true,
            true,
        );
        let second = build_mcap(
            "p",
            &[],
            &[mcap::records::Metadata {
                name: "robot".to_string(),
                metadata: BTreeMap::from([(String::from("a"), String::from("1"))]),
            }],
            &[],
            true,
            true,
        );

        let merged = merge_bytes(
            &[("a", first.as_slice()), ("b", second.as_slice())],
            CoalesceChannels::Auto,
            true,
        )
        .expect("merge");

        let summary = mcap::Summary::read(&merged)
            .expect("summary")
            .expect("present");
        assert_eq!(summary.stats.as_ref().expect("stats").metadata_count, 1);
        assert_eq!(summary.metadata_indexes.len(), 1);
    }

    #[test]
    fn merge_allow_duplicate_metadata_keeps_same_name_with_different_content() {
        let first = build_mcap(
            "p",
            &[],
            &[mcap::records::Metadata {
                name: "robot".to_string(),
                metadata: BTreeMap::from([(String::from("a"), String::from("1"))]),
            }],
            &[],
            true,
            true,
        );
        let second = build_mcap(
            "p",
            &[],
            &[mcap::records::Metadata {
                name: "robot".to_string(),
                metadata: BTreeMap::from([(String::from("a"), String::from("2"))]),
            }],
            &[],
            true,
            true,
        );

        let merged = merge_bytes(
            &[("a", first.as_slice()), ("b", second.as_slice())],
            CoalesceChannels::Auto,
            true,
        )
        .expect("merge");

        let summary = mcap::Summary::read(&merged)
            .expect("summary")
            .expect("present");
        assert_eq!(summary.stats.as_ref().expect("stats").metadata_count, 2);
        assert_eq!(summary.metadata_indexes.len(), 2);
    }

    #[test]
    fn merge_force_coalesces_channels_ignoring_metadata() {
        let left = build_mcap(
            "profile",
            &[TestMessage {
                channel_id: 1,
                topic: "/topic".to_string(),
                metadata: BTreeMap::from([(String::from("host"), String::from("left"))]),
                log_time: 0,
                payload: vec![1],
            }],
            &[],
            &[],
            true,
            true,
        );
        let right = build_mcap(
            "profile",
            &[TestMessage {
                channel_id: 1,
                topic: "/topic".to_string(),
                metadata: BTreeMap::from([(String::from("host"), String::from("right"))]),
                log_time: 1,
                payload: vec![2],
            }],
            &[],
            &[],
            true,
            true,
        );

        let merged = merge_bytes(
            &[("left", left.as_slice()), ("right", right.as_slice())],
            CoalesceChannels::Force,
            false,
        )
        .expect("merge");

        let summary = mcap::Summary::read(&merged)
            .expect("summary")
            .expect("present");
        assert_eq!(summary.channels.len(), 1);
    }

    #[test]
    fn merge_auto_keeps_channels_distinct_when_metadata_differs() {
        let left = build_mcap(
            "profile",
            &[TestMessage {
                channel_id: 1,
                topic: "/topic".to_string(),
                metadata: BTreeMap::from([(String::from("host"), String::from("left"))]),
                log_time: 0,
                payload: vec![1],
            }],
            &[],
            &[],
            true,
            true,
        );
        let right = build_mcap(
            "profile",
            &[TestMessage {
                channel_id: 1,
                topic: "/topic".to_string(),
                metadata: BTreeMap::from([(String::from("host"), String::from("right"))]),
                log_time: 1,
                payload: vec![2],
            }],
            &[],
            &[],
            true,
            true,
        );

        let merged = merge_bytes(
            &[("left", left.as_slice()), ("right", right.as_slice())],
            CoalesceChannels::Auto,
            false,
        )
        .expect("merge");

        let summary = mcap::Summary::read(&merged)
            .expect("summary")
            .expect("present");
        assert_eq!(summary.channels.len(), 2);
    }

    #[test]
    fn merge_auto_coalesces_channels_when_metadata_matches() {
        let metadata = BTreeMap::from([(String::from("host"), String::from("same"))]);
        let left = build_mcap(
            "profile",
            &[TestMessage {
                channel_id: 1,
                topic: "/topic".to_string(),
                metadata: metadata.clone(),
                log_time: 0,
                payload: vec![1],
            }],
            &[],
            &[],
            true,
            true,
        );
        let right = build_mcap(
            "profile",
            &[TestMessage {
                channel_id: 1,
                topic: "/topic".to_string(),
                metadata,
                log_time: 1,
                payload: vec![2],
            }],
            &[],
            &[],
            true,
            true,
        );

        let merged = merge_bytes(
            &[("left", left.as_slice()), ("right", right.as_slice())],
            CoalesceChannels::Auto,
            false,
        )
        .expect("merge");

        let summary = mcap::Summary::read(&merged)
            .expect("summary")
            .expect("present");
        assert_eq!(summary.channels.len(), 1);
    }

    #[test]
    fn merge_none_does_not_coalesce_channels() {
        let left = build_mcap(
            "profile",
            &[TestMessage {
                channel_id: 1,
                topic: "/topic".to_string(),
                metadata: BTreeMap::new(),
                log_time: 0,
                payload: vec![1],
            }],
            &[],
            &[],
            true,
            true,
        );
        let right = build_mcap(
            "profile",
            &[TestMessage {
                channel_id: 1,
                topic: "/topic".to_string(),
                metadata: BTreeMap::new(),
                log_time: 1,
                payload: vec![2],
            }],
            &[],
            &[],
            true,
            true,
        );

        let merged = merge_bytes(
            &[("left", left.as_slice()), ("right", right.as_slice())],
            CoalesceChannels::None,
            false,
        )
        .expect("merge");

        let summary = mcap::Summary::read(&merged)
            .expect("summary")
            .expect("present");
        assert_eq!(summary.channels.len(), 2);
    }

    #[test]
    fn merge_copies_attachments_with_and_without_indexes() {
        let indexed = build_mcap(
            "profile",
            &[],
            &[],
            &[mcap::Attachment {
                log_time: 1,
                create_time: 1,
                name: "indexed.bin".to_string(),
                media_type: "application/octet-stream".to_string(),
                data: std::borrow::Cow::Borrowed(&[1, 2, 3]),
            }],
            true,
            true,
        );

        let unindexed = build_mcap(
            "profile",
            &[],
            &[],
            &[mcap::Attachment {
                log_time: 2,
                create_time: 2,
                name: "unindexed.bin".to_string(),
                media_type: "application/octet-stream".to_string(),
                data: std::borrow::Cow::Borrowed(&[4, 5, 6]),
            }],
            false,
            true,
        );

        let merged = merge_bytes(
            &[
                ("indexed", indexed.as_slice()),
                ("unindexed", unindexed.as_slice()),
            ],
            CoalesceChannels::Auto,
            false,
        )
        .expect("merge");

        let summary = mcap::Summary::read(&merged)
            .expect("summary")
            .expect("present");
        assert_eq!(summary.stats.as_ref().expect("stats").attachment_count, 2);
        assert_eq!(summary.attachment_indexes.len(), 2);
    }

    #[test]
    fn merge_copies_attachments_without_statistics_record() {
        let no_stats = build_mcap_with_options(
            "profile",
            &[],
            &[],
            &[mcap::Attachment {
                log_time: 3,
                create_time: 3,
                name: "nostats.bin".to_string(),
                media_type: "application/octet-stream".to_string(),
                data: std::borrow::Cow::Borrowed(&[7, 8, 9]),
            }],
            true,
            true,
            false,
        );

        let merged = merge_bytes(
            &[("no-stats", no_stats.as_slice())],
            CoalesceChannels::Auto,
            false,
        )
        .expect("merge");

        let summary = mcap::Summary::read(&merged)
            .expect("summary")
            .expect("present");
        assert_eq!(summary.stats.as_ref().expect("stats").attachment_count, 1);
        assert_eq!(summary.attachment_indexes.len(), 1);
    }
}
