use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet};
use std::io::{IsTerminal as _, Seek, Write};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use mcap::records::{MessageHeader, Record};

use crate::cli::{CoalesceChannels, CompressionFormat, MergeCommand};
use crate::context::CommandContext;

#[derive(Debug, Clone)]
struct MergeOptions {
    files: Vec<PathBuf>,
    output_file: Option<PathBuf>,
    compression: Option<mcap::Compression>,
    chunk_size: u64,
    include_crc: bool,
    chunked: bool,
    allow_duplicate_metadata: bool,
    coalesce_channels: CoalesceChannels,
}

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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct MetadataKey {
    name: String,
    entries: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
struct InputRef<'a> {
    name: &'a str,
    data: &'a [u8],
}

struct InputMessageReader<'a> {
    name: &'a str,
    records: mcap::read::ChunkFlattener<'a>,
    schemas: HashMap<u16, Arc<mcap::Schema<'static>>>,
    channels: HashMap<u16, Arc<mcap::Channel<'static>>>,
}

type InputMessage = (Arc<mcap::Channel<'static>>, MessageHeader, Vec<u8>);

#[derive(Debug, Clone)]
struct PendingMessage {
    input_idx: usize,
    input_channel_id: u16,
    channel: Arc<mcap::Channel<'static>>,
    sequence: u32,
    log_time: u64,
    publish_time: u64,
    data: Vec<u8>,
}

impl PartialEq for PendingMessage {
    fn eq(&self, other: &Self) -> bool {
        self.log_time == other.log_time
            && self.input_idx == other.input_idx
            && self.input_channel_id == other.input_channel_id
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
        other
            .log_time
            .cmp(&self.log_time)
            .then_with(|| other.input_idx.cmp(&self.input_idx))
            .then_with(|| other.input_channel_id.cmp(&self.input_channel_id))
    }
}

#[derive(Default)]
struct MetadataState {
    seen_metadata: HashSet<MetadataKey>,
    metadata_names: HashSet<String>,
}

#[derive(Default)]
struct IdMaps {
    schema_ids: HashMap<(usize, u16), u16>,
    schema_ids_by_content: HashMap<SchemaKey, u16>,
    channel_ids: HashMap<(usize, u16), u16>,
    channel_ids_by_content: HashMap<ChannelKey, u16>,
    next_output_channel_id: u16,
}

pub fn run(ctx: &CommandContext, args: MergeCommand) -> Result<()> {
    let opts = build_merge_options(args);
    let source_options = crate::commands::common::SourceOptions::new(ctx.allow_remote_scan());

    let mut mapped_inputs = Vec::with_capacity(opts.files.len());
    let mut input_names = Vec::with_capacity(opts.files.len());
    for path in &opts.files {
        let mapped = crate::commands::common::load_path(path, source_options)?;
        mapped_inputs.push(mapped);
        input_names.push(crate::commands::common::redacted_display(path));
    }

    let input_refs: Vec<InputRef<'_>> = mapped_inputs
        .iter()
        .zip(input_names.iter())
        .map(|(mapped, name)| InputRef {
            name: name.as_str(),
            data: mapped.as_slice(),
        })
        .collect();

    if let Some(output_path) = &opts.output_file {
        let output = std::fs::File::create(output_path)
            .with_context(|| format!("failed to open '{}' for writing", output_path.display()))?;
        merge_inputs(&input_refs, output, &opts, false)
    } else {
        if std::io::stdout().is_terminal() {
            bail!("{}", crate::commands::common::PLEASE_REDIRECT);
        }
        let stdout = std::io::stdout();
        let output = mcap::write::NoSeek::new(stdout.lock());
        merge_inputs(&input_refs, output, &opts, true)
    }
}

fn build_merge_options(args: MergeCommand) -> MergeOptions {
    let compression = match args.compression {
        CompressionFormat::Zstd => Some(mcap::Compression::Zstd),
        CompressionFormat::Lz4 => Some(mcap::Compression::Lz4),
        CompressionFormat::None => None,
    };

    MergeOptions {
        files: args.files,
        output_file: args.output_file,
        compression,
        chunk_size: args.chunk_size,
        include_crc: args.include_crc,
        chunked: args.chunked,
        allow_duplicate_metadata: args.allow_duplicate_metadata,
        coalesce_channels: args.coalesce_channels,
    }
}

fn merge_inputs<W: Write + Seek>(
    inputs: &[InputRef<'_>],
    sink: W,
    opts: &MergeOptions,
    disable_seeking: bool,
) -> Result<()> {
    let profiles = inputs
        .iter()
        .map(read_profile)
        .collect::<Result<Vec<_>>>()?;
    let output_profile = output_profile(&profiles);

    let mut write_options = mcap::WriteOptions::new()
        .profile(output_profile)
        .use_chunks(opts.chunked)
        .chunk_size(Some(opts.chunk_size))
        .compression(opts.compression)
        .calculate_chunk_crcs(opts.include_crc)
        .calculate_data_section_crc(opts.include_crc)
        .calculate_summary_section_crc(opts.include_crc)
        .calculate_attachment_crcs(opts.include_crc)
        .disable_seeking(disable_seeking);

    if !opts.chunked {
        write_options = write_options.emit_message_indexes(false);
    }

    let mut writer = write_options
        .create(sink)
        .context("failed to create mcap writer")?;

    let summaries = inputs
        .iter()
        // Match Go CLI behavior by treating summary lookup as best effort and
        // falling back to linear scans when summary parsing fails.
        .map(|input| mcap::Summary::read(input.data).unwrap_or_default())
        .collect::<Vec<_>>();

    merge_messages(
        inputs,
        &mut writer,
        opts.coalesce_channels,
        opts.allow_duplicate_metadata,
    )?;

    for (idx, input) in inputs.iter().enumerate() {
        write_attachments(&mut writer, input, summaries[idx].as_ref())?;
    }

    writer.finish().context("failed to finish mcap writer")?;
    Ok(())
}

fn read_profile(input: &InputRef<'_>) -> Result<String> {
    let mut reader = mcap::read::LinearReader::new(input.data)
        .with_context(|| format!("failed to read '{}'", input.name))?;
    match reader.next() {
        Some(Ok(Record::Header(header))) => Ok(header.profile),
        Some(Ok(_)) | None => Ok(String::new()),
        Some(Err(err)) => Err(anyhow::Error::from(err))
            .with_context(|| format!("failed to read header from '{}'", input.name)),
    }
}

fn output_profile(profiles: &[String]) -> String {
    let Some(first) = profiles.first() else {
        return String::new();
    };
    if profiles.iter().all(|profile| profile == first) {
        first.clone()
    } else {
        String::new()
    }
}

fn write_merged_metadata<W: Write + Seek>(
    writer: &mut mcap::Writer<W>,
    state: &mut MetadataState,
    metadata_record: mcap::records::Metadata,
    allow_duplicate_metadata: bool,
) -> Result<()> {
    if state.metadata_names.contains(&metadata_record.name) && !allow_duplicate_metadata {
        bail!(
            "metadata name '{}' was previously encountered. Supply --allow-duplicate-metadata to override.",
            metadata_record.name
        );
    }

    let key = metadata_key(&metadata_record);
    if state.seen_metadata.insert(key) {
        writer.write_metadata(&metadata_record)?;
        state.metadata_names.insert(metadata_record.name.clone());
    }
    Ok(())
}

fn metadata_key(metadata_record: &mcap::records::Metadata) -> MetadataKey {
    MetadataKey {
        name: metadata_record.name.clone(),
        entries: metadata_record
            .metadata
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
    }
}

fn write_attachments<W: Write + Seek>(
    writer: &mut mcap::Writer<W>,
    input: &InputRef<'_>,
    summary: Option<&mcap::Summary>,
) -> Result<()> {
    if let Some(summary) = summary {
        let attachment_count = summary.stats.as_ref().map(|stats| stats.attachment_count);
        if let Some(attachment_count) = attachment_count {
            if attachment_count == summary.attachment_indexes.len() as u32 {
                let mut indexes = summary.attachment_indexes.clone();
                indexes.sort_by_key(|index| index.offset);
                for index in indexes {
                    let attachment =
                        mcap::read::attachment(input.data, &index).with_context(|| {
                            format!(
                                "failed to read attachment '{}' at offset {} from '{}'",
                                index.name, index.offset, input.name
                            )
                        })?;
                    writer.attach(&attachment).with_context(|| {
                        format!(
                            "failed to write attachment '{}' from '{}'",
                            index.name, input.name
                        )
                    })?;
                }
                return Ok(());
            }
        }
    }

    for record in mcap::read::LinearReader::new(input.data)
        .with_context(|| format!("failed to read '{}'", input.name))?
    {
        if let Record::Attachment { header, data, .. } =
            record.with_context(|| format!("failed to parse '{}'", input.name))?
        {
            writer.attach(&mcap::Attachment {
                log_time: header.log_time,
                create_time: header.create_time,
                name: header.name,
                media_type: header.media_type,
                data: std::borrow::Cow::Borrowed(data.as_ref()),
            })?;
        }
    }

    Ok(())
}

impl<'a> InputMessageReader<'a> {
    fn new(input: &InputRef<'a>) -> Result<Self> {
        Ok(Self {
            name: input.name,
            records: mcap::read::ChunkFlattener::new(input.data)
                .with_context(|| format!("failed to stream records from '{}'", input.name))?,
            schemas: HashMap::new(),
            channels: HashMap::new(),
        })
    }
}

fn next_message_from_input<W: Write + Seek>(
    input: &mut InputMessageReader<'_>,
    writer: &mut mcap::Writer<W>,
    metadata_state: &mut MetadataState,
    allow_duplicate_metadata: bool,
) -> Result<Option<InputMessage>> {
    for record in input.records.by_ref() {
        let record = record.with_context(|| format!("failed to parse '{}'", input.name))?;
        match record {
            Record::Schema { header, data } => {
                let schema = Arc::new(mcap::Schema {
                    id: header.id,
                    name: header.name.clone(),
                    encoding: header.encoding.clone(),
                    data: std::borrow::Cow::Owned(data.into_owned()),
                });
                if let Some(existing) = input.schemas.get(&header.id) {
                    if existing.name != schema.name
                        || existing.encoding != schema.encoding
                        || existing.data.as_ref() != schema.data.as_ref()
                    {
                        return Err(mcap::McapError::ConflictingSchemas(header.name).into());
                    }
                } else {
                    input.schemas.insert(header.id, schema);
                }
            }
            Record::Channel(channel) => {
                let schema = if channel.schema_id == 0 {
                    None
                } else {
                    Some(
                        input
                            .schemas
                            .get(&channel.schema_id)
                            .cloned()
                            .ok_or_else(|| {
                                anyhow::anyhow!(
                                    "encountered channel '{}' with unknown schema {} in '{}'",
                                    channel.topic,
                                    channel.schema_id,
                                    input.name
                                )
                            })?,
                    )
                };
                let parsed_channel = Arc::new(mcap::Channel {
                    id: channel.id,
                    topic: channel.topic.clone(),
                    schema: schema.clone(),
                    message_encoding: channel.message_encoding.clone(),
                    metadata: channel.metadata.clone(),
                });

                if let Some(existing) = input.channels.get(&channel.id) {
                    if existing.topic != parsed_channel.topic
                        || existing.schema.as_ref().map(|schema| schema.id)
                            != parsed_channel.schema.as_ref().map(|schema| schema.id)
                        || existing.message_encoding != parsed_channel.message_encoding
                        || existing.metadata != parsed_channel.metadata
                    {
                        return Err(mcap::McapError::ConflictingChannels(channel.topic).into());
                    }
                } else {
                    input.channels.insert(channel.id, parsed_channel);
                }
            }
            Record::Metadata(metadata) => {
                write_merged_metadata(writer, metadata_state, metadata, allow_duplicate_metadata)?;
            }
            Record::Message { header, data } => {
                let channel = input
                    .channels
                    .get(&header.channel_id)
                    .cloned()
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "encountered message referencing unknown channel {} in '{}'",
                            header.channel_id,
                            input.name
                        )
                    })?;
                return Ok(Some((channel, header, data.into_owned())));
            }
            _ => {}
        }
    }
    Ok(None)
}

fn merge_messages<W: Write + Seek>(
    inputs: &[InputRef<'_>],
    writer: &mut mcap::Writer<W>,
    coalesce_channels: CoalesceChannels,
    allow_duplicate_metadata: bool,
) -> Result<()> {
    let mut streams = inputs
        .iter()
        .map(InputMessageReader::new)
        .collect::<Result<Vec<_>>>()?;

    let mut id_maps = IdMaps {
        next_output_channel_id: 1,
        ..IdMaps::default()
    };
    let mut metadata_state = MetadataState::default();

    let mut heap = BinaryHeap::<PendingMessage>::new();
    for (input_idx, stream) in streams.iter_mut().enumerate() {
        if let Some((channel, header, data)) = next_message_from_input(
            stream,
            writer,
            &mut metadata_state,
            allow_duplicate_metadata,
        )
        .with_context(|| {
            format!(
                "failed reading initial message from '{}'",
                inputs[input_idx].name
            )
        })? {
            heap.push(PendingMessage {
                input_idx,
                input_channel_id: header.channel_id,
                channel,
                sequence: header.sequence,
                log_time: header.log_time,
                publish_time: header.publish_time,
                data,
            });
        }
    }

    while let Some(message) = heap.pop() {
        let output_channel_id = ensure_output_channel_id(
            &mut id_maps,
            writer,
            message.input_idx,
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

        if let Some((channel, header, data)) = next_message_from_input(
            &mut streams[message.input_idx],
            writer,
            &mut metadata_state,
            allow_duplicate_metadata,
        )
        .with_context(|| {
            format!(
                "failed reading next message from '{}'",
                inputs[message.input_idx].name
            )
        })? {
            heap.push(PendingMessage {
                input_idx: message.input_idx,
                input_channel_id: header.channel_id,
                channel,
                sequence: header.sequence,
                log_time: header.log_time,
                publish_time: header.publish_time,
                data,
            });
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

    fn merge_bytes(
        inputs: &[(&str, &[u8])],
        coalesce_channels: CoalesceChannels,
        allow_duplicate_metadata: bool,
    ) -> Result<Vec<u8>> {
        let options = MergeOptions {
            files: Vec::new(),
            output_file: None,
            compression: None,
            chunk_size: 1024,
            include_crc: true,
            chunked: true,
            allow_duplicate_metadata,
            coalesce_channels,
        };
        let input_refs = inputs
            .iter()
            .map(|(name, data)| InputRef { name, data })
            .collect::<Vec<_>>();

        let mut output = Cursor::new(Vec::<u8>::new());
        merge_inputs(&input_refs, &mut output, &options, false)?;
        Ok(output.into_inner())
    }

    #[test]
    fn build_merge_options_maps_cli_fields() {
        let options = build_merge_options(MergeCommand {
            files: vec!["a.mcap".into(), "b.mcap".into()],
            output_file: Some("out.mcap".into()),
            compression: CompressionFormat::Lz4,
            chunk_size: 4096,
            include_crc: false,
            chunked: false,
            allow_duplicate_metadata: true,
            coalesce_channels: CoalesceChannels::Force,
        });

        assert_eq!(
            options.files,
            vec![PathBuf::from("a.mcap"), PathBuf::from("b.mcap")]
        );
        assert_eq!(options.output_file, Some(PathBuf::from("out.mcap")));
        assert!(matches!(options.compression, Some(mcap::Compression::Lz4)));
        assert_eq!(options.chunk_size, 4096);
        assert!(!options.include_crc);
        assert!(!options.chunked);
        assert!(options.allow_duplicate_metadata);
        assert_eq!(options.coalesce_channels, CoalesceChannels::Force);
    }

    #[test]
    fn run_rejects_remote_input_without_scan_opt_in() {
        let err = run(
            &CommandContext::default(),
            MergeCommand {
                files: vec!["http://example.com/a.mcap".into()],
                output_file: Some("out.mcap".into()),
                compression: CompressionFormat::Zstd,
                chunk_size: 1024,
                include_crc: true,
                chunked: true,
                allow_duplicate_metadata: false,
                coalesce_channels: CoalesceChannels::Auto,
            },
        )
        .expect_err("remote merge input should require opt-in");

        assert!(err.to_string().contains("--allow-remote-scan"));
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
            Record::Header(header) => header,
            _ => panic!("expected header"),
        };
        assert!(header.profile.is_empty());
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
