use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap};
use std::io::{self, IsTerminal as _, Write as _};
use std::sync::Arc;

use anyhow::{bail, Context as _, Result};
use mcap::sans_io::indexed_reader::ReadOrder;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};

use crate::cli::CatCommand;
use crate::context::CommandContext;
use crate::{parse, render, source};

const MESSAGE_PREVIEW_LEN: usize = 10;

pub fn run(ctx: &CommandContext, args: CatCommand) -> Result<()> {
    let opts = CatOptions::from_args(&args)?;
    let source_options = source::SourceOptions::new(ctx.allow_remote_scan());
    let stdout = std::io::stdout();
    let mut writer = std::io::BufWriter::new(stdout.lock());

    if args.files.is_empty() {
        let stdin = std::io::stdin();
        if stdin.is_terminal() {
            bail!("supply a file");
        }
        if cat_streaming(&mut writer, stdin.lock(), &opts)? {
            return Ok(());
        }
    } else {
        for file in args.files {
            if cat_file(&mut writer, &file, &opts, source_options)? {
                return Ok(());
            }
        }
    }

    flush_or_ignore_broken_pipe(&mut writer)
}

fn cat_file(
    writer: &mut impl std::io::Write,
    file: &std::path::Path,
    opts: &CatOptions,
    source_options: source::SourceOptions,
) -> Result<bool> {
    if let Some(remote) = source::try_open_remote_mcap(file, source_options)? {
        let mut json_transcoders = JsonTranscoders::default();
        match cat_remote_indexed(
            writer,
            file,
            &remote,
            opts,
            source_options,
            &mut json_transcoders,
        )? {
            RemoteCatResult::BrokenPipe => return Ok(true),
            RemoteCatResult::Done => return Ok(false),
            RemoteCatResult::NeedsFullScan => {}
        }
    }
    let mcap = source::load_path(file, source_options)?;
    cat_mcap(writer, &mcap, opts)
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct CatOptions {
    topics: Vec<String>,
    start: u64,
    end: Option<u64>,
    json: bool,
}

impl CatOptions {
    fn from_args(args: &CatCommand) -> Result<Self> {
        let topics = args
            .topics
            .split(',')
            .filter(|topic| !topic.is_empty())
            .map(str::to_string)
            .collect();
        let mut start = args.start_nsecs;
        if args.start_secs > 0 {
            start = args
                .start_secs
                .checked_mul(1_000_000_000)
                .context("start seconds timestamp overflows nanoseconds")?;
        }
        let mut end = args.end_nsecs;
        if args.end_secs > 0 {
            end = args
                .end_secs
                .checked_mul(1_000_000_000)
                .context("end seconds timestamp overflows nanoseconds")?;
        }
        Ok(Self {
            topics,
            start,
            end: (end != 0).then_some(end),
            json: args.json,
        })
    }

    fn include_topic(&self, topic: &str) -> bool {
        self.topics.is_empty() || self.topics.iter().any(|included| included == topic)
    }

    fn include_time(&self, log_time: u64) -> bool {
        log_time >= self.start && self.end.is_none_or(|end| log_time < end)
    }
}

fn cat_mcap(writer: &mut impl std::io::Write, mcap: &[u8], opts: &CatOptions) -> Result<bool> {
    let mut json_transcoders = JsonTranscoders::default();
    if let Some(broken_pipe) = cat_indexed(writer, mcap, opts, &mut json_transcoders)? {
        return Ok(broken_pipe);
    }
    cat_linear(writer, mcap, opts, &mut json_transcoders)
}

fn cat_indexed(
    writer: &mut impl std::io::Write,
    mcap: &[u8],
    opts: &CatOptions,
    json_transcoders: &mut JsonTranscoders,
) -> Result<Option<bool>> {
    let summary = match mcap::Summary::read(mcap) {
        Ok(Some(summary)) => summary,
        Ok(None) => return Ok(None),
        // A spec-valid file may repeat a channel in the summary without repeating its schema,
        // leaving the schema defined only inside a chunk. That can't be resolved from the summary
        // alone, so fall back to a linear scan, which registers in-chunk definitions as it reads.
        Err(mcap::McapError::UnknownSchema(..)) => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    if summary.chunk_indexes.is_empty() {
        return Ok(None);
    }

    let needs_in_chunk_definitions = needs_in_chunk_definitions(&summary);
    let mut schemas = summary.schemas.clone();
    let mut channel_defs = HashMap::<u16, mcap::records::Channel>::new();
    let mut channels = summary.channels.clone();
    // When channels/schemas are defined only inside chunks (not repeated in the summary), collect
    // their definitions from every chunk up front. Collecting lazily per requested chunk would miss
    // a definition that lives in a chunk skipped by a topic or time filter (e.g. a channel defined
    // in an early chunk but referenced by messages in a later one).
    if needs_in_chunk_definitions {
        for chunk_index in &summary.chunk_indexes {
            parse::collect_chunk_definitions_from_mcap(
                mcap,
                chunk_index,
                &mut schemas,
                &mut channel_defs,
            )?;
        }
    }

    let included_topics: BTreeSet<String> = summary
        .channels
        .values()
        .filter(|channel| opts.include_topic(&channel.topic))
        .map(|channel| channel.topic.clone())
        .collect();
    if !opts.topics.is_empty() && included_topics.is_empty() && !needs_in_chunk_definitions {
        return Ok(Some(false));
    }

    let mut indexed_opts =
        mcap::sans_io::IndexedReaderOptions::new().with_order(ReadOrder::LogTime);
    if opts.start != 0 {
        indexed_opts = indexed_opts.log_time_on_or_after(opts.start);
    }
    if let Some(end) = opts.end {
        indexed_opts = indexed_opts.log_time_before(end);
    }
    // Reader-level topic filtering keys on `summary.channels`, so skip it when chunk-local channels
    // may exist (see `needs_in_chunk_definitions`) and let the per-message `include_topic` check
    // below filter instead, to avoid silently dropping matching chunk-local messages.
    if !opts.topics.is_empty() && !included_topics.is_empty() && !needs_in_chunk_definitions {
        indexed_opts = indexed_opts.include_topics(included_topics.iter().cloned());
    }

    let mut reader = mcap::sans_io::IndexedReader::new_with_options(&summary, indexed_opts)?;

    while let Some(event) = reader.next_event() {
        match event? {
            mcap::sans_io::IndexedReadEvent::ReadChunkRequest { offset, length } => {
                let start = offset as usize;
                let end = start
                    .checked_add(length)
                    .ok_or_else(|| anyhow::anyhow!("chunk read overflow at offset {offset}"))?;
                if end > mcap.len() {
                    anyhow::bail!("chunk read out of bounds at offset {offset} length {length}");
                }
                reader.insert_chunk_record_data(offset, &mcap[start..end])?;
            }
            mcap::sans_io::IndexedReadEvent::Message { header, data } => {
                let channel =
                    resolve_channel(header.channel_id, &schemas, &channel_defs, &mut channels)?;
                if !opts.include_topic(&channel.topic) {
                    continue;
                }
                let message = CatMessage {
                    channel: &channel,
                    sequence: header.sequence,
                    log_time: header.log_time,
                    publish_time: header.publish_time,
                    data,
                };
                if write_message(writer, message, opts, json_transcoders)? {
                    return Ok(Some(true));
                }
            }
        }
    }

    Ok(Some(false))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RemoteCatResult {
    BrokenPipe,
    Done,
    NeedsFullScan,
}

fn cat_remote_indexed(
    writer: &mut impl std::io::Write,
    file: &std::path::Path,
    remote: &source::RemoteMcap,
    opts: &CatOptions,
    source_options: source::SourceOptions,
    json_transcoders: &mut JsonTranscoders,
) -> Result<RemoteCatResult> {
    let summary = remote.summary();
    if summary.chunk_indexes.is_empty() {
        if !source_options.allow_remote_scan {
            bail!(
                "{}: remote file has no chunk index; reading messages requires opt-in; {}",
                source::redacted_display(file),
                source::remote_scan_opt_in_suffix()
            );
        }
        return Ok(RemoteCatResult::NeedsFullScan);
    }
    let has_chunks_without_message_indexes = summary
        .chunk_indexes
        .iter()
        .any(|chunk| chunk.message_index_offsets.is_empty());
    if has_chunks_without_message_indexes && !source_options.allow_remote_scan {
        bail!(
            "{}: remote file has chunk indexes without message indexes; reading messages requires opt-in; {}",
            source::redacted_display(file),
            source::remote_scan_opt_in_suffix()
        );
    }
    let needs_in_chunk_definitions = needs_in_chunk_definitions(summary);
    let mut schemas = summary.schemas.clone();
    let mut channel_defs = HashMap::<u16, mcap::records::Channel>::new();
    let mut channels = summary.channels.clone();

    let included_topics: BTreeSet<String> = summary
        .channels
        .values()
        .filter(|channel| opts.include_topic(&channel.topic))
        .map(|channel| channel.topic.clone())
        .collect();
    if !opts.topics.is_empty() && included_topics.is_empty() && !needs_in_chunk_definitions {
        return Ok(RemoteCatResult::Done);
    }
    let planned_chunks =
        planned_chunk_reads(summary, opts, &included_topics, needs_in_chunk_definitions);
    if !planned_chunks.is_empty() && !source_options.allow_remote_scan {
        // When chunk-local definitions must be collected, every chunk is read up front (a
        // definition can live in a chunk the filter would otherwise skip), so size the warning from
        // the full set rather than the filtered plan to avoid under-quoting the bytes fetched.
        let (chunk_count, compressed_bytes) = if needs_in_chunk_definitions {
            (
                summary.chunk_indexes.len(),
                summary
                    .chunk_indexes
                    .iter()
                    .map(|chunk| chunk.compressed_size)
                    .sum::<u64>(),
            )
        } else {
            (
                planned_chunks.len(),
                planned_chunks
                    .iter()
                    .map(|chunk| chunk.compressed_size)
                    .sum::<u64>(),
            )
        };
        bail!(
            "{}: remote cat would read {} message chunks ({} compressed); {}",
            source::redacted_display(file),
            chunk_count,
            render::human_bytes(compressed_bytes),
            source::remote_scan_opt_in_suffix()
        );
    }

    // When channels/schemas are defined only inside chunks, fetch every chunk once up front to
    // collect their definitions, caching the compressed data so the indexed read below doesn't
    // re-fetch. Lazy per-chunk collection would miss a definition in a chunk skipped by a topic or
    // time filter. The remote-scan gate above already required opt-in to reach here.
    let mut chunk_data_cache: HashMap<u64, Vec<u8>> = HashMap::new();
    if needs_in_chunk_definitions && !planned_chunks.is_empty() {
        for chunk_index in &summary.chunk_indexes {
            let chunk_len = usize::try_from(chunk_index.chunk_length).with_context(|| {
                format!(
                    "chunk length out of range for this platform: {}",
                    chunk_index.chunk_length
                )
            })?;
            let chunk = remote.read_range(chunk_index.chunk_start_offset, chunk_len)?;
            parse::collect_chunk_definitions_from_record_bytes(
                &chunk,
                &mut schemas,
                &mut channel_defs,
            )?;
            let data_offset = chunk_index.compressed_data_offset()?;
            let compressed_start = usize::try_from(data_offset - chunk_index.chunk_start_offset)
                .with_context(|| {
                    format!("chunk data offset out of range for this platform: {data_offset}")
                })?;
            let compressed = chunk
                .get(compressed_start..)
                .ok_or_else(|| anyhow::anyhow!("chunk data out of bounds at offset {data_offset}"))?
                .to_vec();
            chunk_data_cache.insert(data_offset, compressed);
        }
    }

    let mut indexed_opts =
        mcap::sans_io::IndexedReaderOptions::new().with_order(ReadOrder::LogTime);
    if opts.start != 0 {
        indexed_opts = indexed_opts.log_time_on_or_after(opts.start);
    }
    if let Some(end) = opts.end {
        indexed_opts = indexed_opts.log_time_before(end);
    }
    // Reader-level topic filtering keys on `summary.channels`, so skip it when chunk-local channels
    // may exist (see `needs_in_chunk_definitions`) and let the per-message `include_topic` check
    // below filter instead, to avoid silently dropping matching chunk-local messages.
    if !opts.topics.is_empty() && !included_topics.is_empty() && !needs_in_chunk_definitions {
        indexed_opts = indexed_opts.include_topics(included_topics.iter().cloned());
    }

    let mut reader = mcap::sans_io::IndexedReader::new_with_options(summary, indexed_opts)?;
    while let Some(event) = reader.next_event() {
        match event? {
            mcap::sans_io::IndexedReadEvent::ReadChunkRequest { offset, length } => {
                if let Some(cached) = chunk_data_cache.get(&offset) {
                    let compressed = cached.get(..length).ok_or_else(|| {
                        anyhow::anyhow!(
                            "chunk read out of bounds at offset {offset} length {length}"
                        )
                    })?;
                    reader.insert_chunk_record_data(offset, compressed)?;
                } else {
                    let chunk = remote.read_range(offset, length)?;
                    reader.insert_chunk_record_data(offset, &chunk)?;
                }
            }
            mcap::sans_io::IndexedReadEvent::Message { header, data } => {
                let channel =
                    resolve_channel(header.channel_id, &schemas, &channel_defs, &mut channels)?;
                if !opts.include_topic(&channel.topic) {
                    continue;
                }
                let message = CatMessage {
                    channel: &channel,
                    sequence: header.sequence,
                    log_time: header.log_time,
                    publish_time: header.publish_time,
                    data,
                };
                if write_message(writer, message, opts, json_transcoders)? {
                    return Ok(RemoteCatResult::BrokenPipe);
                }
            }
        }
    }

    Ok(RemoteCatResult::Done)
}

/// Returns whether chunk definitions must be read before indexed iteration.
///
/// When true, callers must (a) collect in-chunk definitions before resolving messages, and (b) skip
/// reader-level topic filtering -- which keys on `summary.channels` only -- and filter per message
/// instead, otherwise a chunk-local channel matching a `--topics` filter would be silently dropped.
///
/// Note: a file mixing summary channels with chunk-local ones can't be produced by the standard
/// writer (its `repeat_channels`/`repeat_schemas` options are all-or-nothing). The mixed + `--topics`
/// path this guards is only possible when chunk indexes include message-index channel IDs, so it is
/// defensive against partial-repetition files from other tools and isn't covered by an
/// `mcap::Writer`-based regression test.
fn needs_in_chunk_definitions(summary: &mcap::Summary) -> bool {
    if !summary.chunk_indexes.is_empty() && summary.channels.is_empty() {
        return true;
    }
    summary.chunk_indexes.iter().any(|chunk| {
        chunk
            .message_index_offsets
            .keys()
            .any(|channel_id| !summary.channels.contains_key(channel_id))
    })
}

fn resolve_channel(
    channel_id: u16,
    schemas: &HashMap<u16, Arc<mcap::Schema<'static>>>,
    channel_defs: &HashMap<u16, mcap::records::Channel>,
    channels: &mut HashMap<u16, Arc<mcap::Channel<'static>>>,
) -> Result<Arc<mcap::Channel<'static>>> {
    if let Some(channel) = channels.get(&channel_id) {
        return Ok(channel.clone());
    }

    let channel_def = channel_defs
        .get(&channel_id)
        .ok_or_else(|| anyhow::anyhow!("unknown channel {channel_id}"))?;
    let channel = build_channel(channel_def, schemas)?;
    channels.insert(channel_id, channel.clone());
    Ok(channel)
}

// Keep this planner conservative: it intentionally mirrors IndexedReader chunk filtering as an
// upper bound so the remote-scan gate fires before any possible chunk payload fetch.
fn planned_chunk_reads<'a>(
    summary: &'a mcap::Summary,
    opts: &CatOptions,
    included_topics: &BTreeSet<String>,
    needs_in_chunk_definitions: bool,
) -> Vec<&'a mcap::records::ChunkIndex> {
    let channel_ids: BTreeSet<u16> = if opts.topics.is_empty() || needs_in_chunk_definitions {
        BTreeSet::new()
    } else {
        summary
            .channels
            .iter()
            .filter(|(_, channel)| included_topics.contains(&channel.topic))
            .map(|(id, _)| *id)
            .collect()
    };

    summary
        .chunk_indexes
        .iter()
        .filter(|chunk| {
            if opts.start != 0 && chunk.message_end_time < opts.start {
                return false;
            }
            if let Some(end) = opts.end {
                if chunk.message_start_time >= end {
                    return false;
                }
            }
            if channel_ids.is_empty() {
                return true;
            }
            if chunk.message_index_offsets.is_empty() {
                return true;
            }
            chunk
                .message_index_offsets
                .keys()
                .any(|channel_id| channel_ids.contains(channel_id))
        })
        .collect()
}

fn cat_linear(
    writer: &mut impl std::io::Write,
    mcap: &[u8],
    opts: &CatOptions,
    json_transcoders: &mut JsonTranscoders,
) -> Result<bool> {
    for message in mcap::MessageStream::new(mcap)? {
        let message = message?;
        if !opts.include_time(message.log_time) || !opts.include_topic(&message.channel.topic) {
            continue;
        }
        let message = CatMessage {
            channel: &message.channel,
            sequence: message.sequence,
            log_time: message.log_time,
            publish_time: message.publish_time,
            data: message.data.as_ref(),
        };
        if write_message(writer, message, opts, json_transcoders)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn cat_streaming(
    writer: &mut impl std::io::Write,
    mut source: impl std::io::Read,
    opts: &CatOptions,
) -> Result<bool> {
    let mut reader = mcap::sans_io::LinearReader::new();
    let mut schemas = HashMap::<u16, Arc<mcap::Schema<'static>>>::new();
    let mut channel_defs = HashMap::<u16, mcap::records::Channel>::new();
    let mut channels = HashMap::<u16, Arc<mcap::Channel<'static>>>::new();
    let mut json_transcoders = JsonTranscoders::default();

    while let Some(event) = reader.next_event() {
        match event? {
            mcap::sans_io::LinearReadEvent::ReadRequest(need) => {
                let read = source
                    .read(reader.insert(need))
                    .context("failed to read input from stdin")?;
                reader.notify_read(read);
            }
            mcap::sans_io::LinearReadEvent::Record { data, opcode } => {
                let record = mcap::parse_record(opcode, data)?;
                if handle_linear_record(
                    writer,
                    record,
                    opts,
                    &mut schemas,
                    &mut channel_defs,
                    &mut channels,
                    &mut json_transcoders,
                )? {
                    return Ok(true);
                }
            }
        }
    }

    Ok(false)
}

fn handle_linear_record(
    writer: &mut impl std::io::Write,
    record: mcap::records::Record<'_>,
    opts: &CatOptions,
    schemas: &mut HashMap<u16, Arc<mcap::Schema<'static>>>,
    channel_defs: &mut HashMap<u16, mcap::records::Channel>,
    channels: &mut HashMap<u16, Arc<mcap::Channel<'static>>>,
    json_transcoders: &mut JsonTranscoders,
) -> Result<bool> {
    match record {
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
                let resolved = build_channel(&channel, schemas)?;
                channels.insert(channel.id, resolved);
            }
            channel_defs.insert(channel.id, channel);
        }
        mcap::records::Record::Message { header, data } => {
            if !opts.include_time(header.log_time) {
                return Ok(false);
            }

            let channel = resolve_channel(header.channel_id, schemas, channel_defs, channels)?;

            if !opts.include_topic(&channel.topic) {
                return Ok(false);
            }

            let message = CatMessage {
                channel: &channel,
                sequence: header.sequence,
                log_time: header.log_time,
                publish_time: header.publish_time,
                data: data.as_ref(),
            };
            return write_message(writer, message, opts, json_transcoders);
        }
        _ => {}
    }

    Ok(false)
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

struct CatMessage<'a, 'schema, 'data> {
    channel: &'a mcap::Channel<'schema>,
    sequence: u32,
    log_time: u64,
    publish_time: u64,
    data: &'data [u8],
}

fn write_message(
    writer: &mut impl std::io::Write,
    message: CatMessage<'_, '_, '_>,
    opts: &CatOptions,
    json_transcoders: &mut JsonTranscoders,
) -> Result<bool> {
    if opts.json {
        write_json_message(
            writer,
            message.channel,
            message.sequence,
            message.log_time,
            message.publish_time,
            message.data,
            json_transcoders,
        )
    } else {
        let schema_name = message
            .channel
            .schema
            .as_ref()
            .map(|schema| schema.name.as_str())
            .unwrap_or("no schema");
        write_message_fields(
            writer,
            message.log_time,
            &message.channel.topic,
            schema_name,
            message.data,
            MESSAGE_PREVIEW_LEN,
        )
    }
}

fn write_message_fields(
    writer: &mut impl std::io::Write,
    log_time: u64,
    topic: &str,
    schema_name: &str,
    data: &[u8],
    max_preview_bytes: usize,
) -> Result<bool> {
    let result: io::Result<()> = (|| {
        render::write_raw_time(writer, log_time)?;
        write!(writer, " {} [{}] ", topic, schema_name)?;
        write_payload_preview(writer, data, max_preview_bytes)?;
        writeln!(writer)
    })();
    io_result_to_broken_pipe(result)
}

fn write_json_message(
    writer: &mut impl std::io::Write,
    channel: &mcap::Channel<'_>,
    sequence: u32,
    log_time: u64,
    publish_time: u64,
    data: &[u8],
    json_transcoders: &mut JsonTranscoders,
) -> Result<bool> {
    let encoded_data = json_transcoders.encode(channel, data)?;
    // Unlike the Go CLI's current manual string concatenation, escaping here keeps
    // JSON valid for topics containing quotes or backslashes.
    let topic = serde_json::to_string(&channel.topic).context("failed to encode topic")?;
    let result: io::Result<()> = (|| {
        write!(
            writer,
            "{{\"topic\":{topic},\"sequence\":{sequence},\"log_time\":"
        )?;
        writer.write_all(render::decimal_time(log_time).as_bytes())?;
        write!(writer, ",\"publish_time\":")?;
        writer.write_all(render::decimal_time(publish_time).as_bytes())?;
        writer.write_all(b",\"data\":")?;
        writer.write_all(encoded_data.as_ref())?;
        writer.write_all(b"}\n")
    })();
    io_result_to_broken_pipe(result)
}

fn io_result_to_broken_pipe(result: io::Result<()>) -> Result<bool> {
    match result {
        Ok(()) => Ok(false),
        Err(err) if err.kind() == io::ErrorKind::BrokenPipe => Ok(true),
        Err(err) => Err(err.into()),
    }
}

fn flush_or_ignore_broken_pipe(writer: &mut impl std::io::Write) -> Result<()> {
    if let Err(err) = writer.flush() {
        if err.kind() == io::ErrorKind::BrokenPipe {
            return Ok(());
        }
        return Err(err.into());
    }
    Ok(())
}

#[derive(Default)]
struct JsonTranscoders {
    protobuf_descriptors: HashMap<u16, MessageDescriptor>,
    ros1_transcoders: HashMap<u16, Ros1MessageDef>,
}

impl JsonTranscoders {
    fn encode<'a>(&mut self, channel: &mcap::Channel<'_>, data: &'a [u8]) -> Result<Cow<'a, [u8]>> {
        let Some(schema) = channel.schema.as_ref() else {
            return encode_schemaless_json(&channel.message_encoding, data);
        };
        if schema.encoding.is_empty() {
            return encode_schemaless_json(&channel.message_encoding, data);
        }

        match schema.encoding.as_str() {
            "jsonschema" => Ok(Cow::Borrowed(data)),
            "protobuf" => {
                let descriptor = match self.protobuf_descriptors.get(&schema.id) {
                    Some(descriptor) => descriptor.clone(),
                    None => {
                        let pool = DescriptorPool::decode(schema.data.as_ref())
                            .context("failed to build file descriptor set")?;
                        let descriptor = pool.get_message_by_name(&schema.name).ok_or_else(|| {
                            anyhow::anyhow!("failed to find descriptor: {}", schema.name)
                        })?;
                        self.protobuf_descriptors
                            .insert(schema.id, descriptor.clone());
                        descriptor
                    }
                };
                let message = DynamicMessage::decode(descriptor, data)
                    .context("failed to parse message")?;
                let json = serde_json::to_vec(&message).context("failed to marshal message")?;
                Ok(Cow::Owned(json))
            }
            "ros1msg" => {
                let transcoder = match self.ros1_transcoders.get(&schema.id) {
                    Some(transcoder) => transcoder,
                    None => {
                        let transcoder = Ros1MessageDef::parse(&schema.name, schema.data.as_ref())
                            .with_context(|| {
                                format!("failed to build transcoder for {}", channel.topic)
                            })?;
                        self.ros1_transcoders.insert(schema.id, transcoder);
                        self.ros1_transcoders
                            .get(&schema.id)
                            .expect("transcoder was just inserted")
                    }
                };
                let json = transcoder
                    .transcode(data)
                    .with_context(|| format!("failed to transcode {} record on {}", schema.name, channel.topic))?;
                Ok(Cow::Owned(json))
            }
            encoding => bail!(
                "JSON output only supported for ros1msg, protobuf, and jsonschema schemas. Found: {encoding}"
            ),
        }
    }
}

fn encode_schemaless_json<'a>(message_encoding: &str, data: &'a [u8]) -> Result<Cow<'a, [u8]>> {
    match message_encoding {
        "json" => Ok(Cow::Borrowed(data)),
        encoding => bail!(
            "for schema-less channels, JSON output is only supported with 'json' message encoding. found: {encoding}"
        ),
    }
}

#[derive(Debug, Clone)]
struct Ros1MessageDef {
    root_type: String,
    definitions: HashMap<String, Ros1Definition>,
}

#[derive(Debug, Clone)]
struct Ros1Definition {
    package: String,
    fields: Vec<Ros1Field>,
}

#[derive(Debug, Clone)]
struct Ros1Field {
    field_type: Ros1FieldType,
    name: String,
}

#[derive(Debug, Clone)]
struct Ros1FieldType {
    base: String,
    array: Option<Option<usize>>,
}

impl Ros1MessageDef {
    fn parse(root_type: &str, data: &[u8]) -> Result<Self> {
        let schema = std::str::from_utf8(data).context("schema data is not utf8")?;
        let mut definitions = HashMap::<String, Ros1Definition>::new();
        let mut current_type = root_type.to_string();
        definitions.insert(current_type.clone(), Ros1Definition::new(&current_type));

        for line in schema.lines() {
            let line = line.trim();
            if line.starts_with("MSG:") {
                current_type =
                    normalize_ros1_type(line.trim_start_matches("MSG:").trim(), root_type);
                definitions
                    .entry(current_type.clone())
                    .or_insert_with(|| Ros1Definition::new(&current_type));
                continue;
            }
            if line.starts_with('=') {
                continue;
            }
            let Some(field) = parse_ros1_field(line) else {
                continue;
            };
            definitions
                .entry(current_type.clone())
                .or_insert_with(|| Ros1Definition::new(&current_type))
                .fields
                .push(field);
        }

        Ok(Self {
            root_type: root_type.to_string(),
            definitions,
        })
    }

    fn transcode(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut cursor = 0usize;
        let mut out = Vec::new();
        self.write_message(&mut out, &self.root_type, data, &mut cursor)?;
        Ok(out)
    }

    fn write_message(
        &self,
        out: &mut Vec<u8>,
        type_name: &str,
        data: &[u8],
        cursor: &mut usize,
    ) -> Result<()> {
        let definition = self
            .definitions
            .get(type_name)
            .ok_or_else(|| anyhow::anyhow!("unknown ROS1 message type {type_name}"))?;
        out.push(b'{');
        for (index, field) in definition.fields.iter().enumerate() {
            if index > 0 {
                out.push(b',');
            }
            serde_json::to_writer(&mut *out, &field.name)?;
            out.push(b':');
            self.write_value(out, &definition.package, &field.field_type, data, cursor)?;
        }
        out.push(b'}');
        Ok(())
    }

    fn write_value(
        &self,
        out: &mut Vec<u8>,
        package: &str,
        field_type: &Ros1FieldType,
        data: &[u8],
        cursor: &mut usize,
    ) -> Result<()> {
        if let Some(array_len) = field_type.array {
            let len = match array_len {
                Some(len) => len,
                None => read_u32(data, cursor)? as usize,
            };
            out.push(b'[');
            for index in 0..len {
                if index > 0 {
                    out.push(b',');
                }
                self.write_single_value(out, package, &field_type.base, data, cursor)?;
            }
            out.push(b']');
            return Ok(());
        }

        self.write_single_value(out, package, &field_type.base, data, cursor)
    }

    fn write_single_value(
        &self,
        out: &mut Vec<u8>,
        package: &str,
        base_type: &str,
        data: &[u8],
        cursor: &mut usize,
    ) -> Result<()> {
        match base_type {
            "bool" => out.extend_from_slice(if read_u8(data, cursor)? == 0 {
                b"false"
            } else {
                b"true"
            }),
            "int8" | "byte" => write!(out, "{}", read_i8(data, cursor)?)?,
            "uint8" | "char" => write!(out, "{}", read_u8(data, cursor)?)?,
            "int16" => write!(out, "{}", read_i16(data, cursor)?)?,
            "uint16" => write!(out, "{}", read_u16(data, cursor)?)?,
            "int32" => write!(out, "{}", read_i32(data, cursor)?)?,
            "uint32" => write!(out, "{}", read_u32(data, cursor)?)?,
            "int64" => write!(out, "{}", read_i64(data, cursor)?)?,
            "uint64" => write!(out, "{}", read_u64(data, cursor)?)?,
            "float32" => write_ros1_float(out, read_f32(data, cursor)? as f64)?,
            "float64" => write_ros1_float(out, read_f64(data, cursor)?)?,
            "string" => {
                let len = read_u32(data, cursor)? as usize;
                let bytes = read_exact(data, cursor, len)?;
                let value = String::from_utf8_lossy(bytes);
                serde_json::to_writer(&mut *out, value.as_ref())?;
            }
            "time" => {
                let sec = read_u32(data, cursor)? as u64;
                let nsec = read_u32(data, cursor)? as u64;
                write!(out, "{sec}.{nsec:09}")?;
            }
            "duration" => {
                let sec = read_i32(data, cursor)?;
                let nsec = read_i32(data, cursor)?;
                write_signed_decimal_time(out, sec, nsec)?;
            }
            nested_type => {
                let resolved = resolve_ros1_type(package, nested_type);
                self.write_message(out, &resolved, data, cursor)?;
            }
        }
        Ok(())
    }
}

impl Ros1Definition {
    fn new(type_name: &str) -> Self {
        let package = type_name
            .split_once('/')
            .map(|(package, _)| package.to_string())
            .unwrap_or_default();
        Self {
            package,
            fields: Vec::new(),
        }
    }
}

fn normalize_ros1_type(type_name: &str, root_type: &str) -> String {
    if type_name.contains('/') {
        type_name.to_string()
    } else {
        resolve_ros1_type(
            root_type
                .split_once('/')
                .map(|(package, _)| package)
                .unwrap_or(""),
            type_name,
        )
    }
}

fn resolve_ros1_type(package: &str, type_name: &str) -> String {
    if type_name.contains('/') {
        type_name.to_string()
    } else if type_name == "Header" {
        "std_msgs/Header".to_string()
    } else {
        format!("{package}/{type_name}")
    }
}

fn parse_ros1_field(line: &str) -> Option<Ros1Field> {
    let line = line
        .split_once('#')
        .map(|(prefix, _)| prefix)
        .unwrap_or(line)
        .trim();
    if line.is_empty() || line.contains('=') {
        return None;
    }
    let mut parts = line.split_whitespace();
    let type_token = parts.next()?;
    let name = parts.next()?.to_string();
    Some(Ros1Field {
        field_type: parse_ros1_field_type(type_token),
        name,
    })
}

fn parse_ros1_field_type(type_token: &str) -> Ros1FieldType {
    if let Some(array_start) = type_token.find('[') {
        let base = strip_bound(&type_token[..array_start]).to_string();
        let Some(array_end) = type_token[array_start + 1..]
            .find(']')
            .map(|relative| array_start + 1 + relative)
        else {
            return Ros1FieldType { base, array: None };
        };
        let array_suffix = &type_token[array_start + 1..array_end];
        let array = if array_suffix.is_empty() || array_suffix.starts_with("<=") {
            Some(None)
        } else {
            Some(array_suffix.parse::<usize>().ok())
        };
        Ros1FieldType { base, array }
    } else {
        Ros1FieldType {
            base: strip_bound(type_token).to_string(),
            array: None,
        }
    }
}

fn strip_bound(type_token: &str) -> &str {
    type_token
        .split_once("<=")
        .map(|(base, _)| base)
        .unwrap_or(type_token)
}

fn write_ros1_float(writer: &mut impl std::io::Write, value: f64) -> std::io::Result<()> {
    if value.is_nan() {
        writer.write_all(br#""NaN""#)
    } else if value == f64::INFINITY {
        writer.write_all(br#""Infinity""#)
    } else if value == f64::NEG_INFINITY {
        writer.write_all(br#""-Infinity""#)
    } else {
        serde_json::to_writer(writer, &value)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
        Ok(())
    }
}

fn write_signed_decimal_time(
    writer: &mut impl std::io::Write,
    seconds: i32,
    nanos: i32,
) -> std::io::Result<()> {
    let total_nanos = seconds as i128 * 1_000_000_000i128 + nanos as i128;
    let sign = if total_nanos < 0 { "-" } else { "" };
    let abs = total_nanos.abs();
    write!(
        writer,
        "{sign}{}.{:09}",
        abs / 1_000_000_000,
        abs % 1_000_000_000
    )
}

fn read_exact<'a>(data: &'a [u8], cursor: &mut usize, len: usize) -> Result<&'a [u8]> {
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| anyhow::anyhow!("ROS1 cursor overflow"))?;
    let slice = data
        .get(*cursor..end)
        .ok_or_else(|| anyhow::anyhow!("ROS1 message ended unexpectedly"))?;
    *cursor = end;
    Ok(slice)
}

fn read_u8(data: &[u8], cursor: &mut usize) -> Result<u8> {
    Ok(read_exact(data, cursor, 1)?[0])
}

fn read_i8(data: &[u8], cursor: &mut usize) -> Result<i8> {
    Ok(read_u8(data, cursor)? as i8)
}

fn read_u16(data: &[u8], cursor: &mut usize) -> Result<u16> {
    Ok(u16::from_le_bytes(read_exact(data, cursor, 2)?.try_into()?))
}

fn read_i16(data: &[u8], cursor: &mut usize) -> Result<i16> {
    Ok(i16::from_le_bytes(read_exact(data, cursor, 2)?.try_into()?))
}

fn read_u32(data: &[u8], cursor: &mut usize) -> Result<u32> {
    Ok(u32::from_le_bytes(read_exact(data, cursor, 4)?.try_into()?))
}

fn read_i32(data: &[u8], cursor: &mut usize) -> Result<i32> {
    Ok(i32::from_le_bytes(read_exact(data, cursor, 4)?.try_into()?))
}

fn read_u64(data: &[u8], cursor: &mut usize) -> Result<u64> {
    Ok(u64::from_le_bytes(read_exact(data, cursor, 8)?.try_into()?))
}

fn read_i64(data: &[u8], cursor: &mut usize) -> Result<i64> {
    Ok(i64::from_le_bytes(read_exact(data, cursor, 8)?.try_into()?))
}

fn read_f32(data: &[u8], cursor: &mut usize) -> Result<f32> {
    Ok(f32::from_le_bytes(read_exact(data, cursor, 4)?.try_into()?))
}

fn read_f64(data: &[u8], cursor: &mut usize) -> Result<f64> {
    Ok(f64::from_le_bytes(read_exact(data, cursor, 8)?.try_into()?))
}

fn write_payload_preview(
    writer: &mut impl std::io::Write,
    data: &[u8],
    max_bytes: usize,
) -> std::io::Result<()> {
    let preview = if data.len() > max_bytes {
        &data[..max_bytes]
    } else {
        data
    };

    write!(writer, "[")?;
    for (idx, byte) in preview.iter().enumerate() {
        if idx > 0 {
            write!(writer, " ")?;
        }
        write!(writer, "{byte}")?;
    }
    write!(writer, "]")?;

    if data.len() > max_bytes {
        write!(writer, "...")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        borrow::Cow,
        collections::{BTreeMap, BTreeSet},
        io::{Cursor, Read, Write},
        net::TcpListener,
        path::Path,
        sync::Arc,
        thread,
    };

    use super::{
        cat_indexed, cat_mcap, cat_streaming, needs_in_chunk_definitions, parse_ros1_field_type,
        planned_chunk_reads, write_payload_preview, write_ros1_float, write_signed_decimal_time,
        CatOptions, JsonTranscoders, Ros1MessageDef, MESSAGE_PREVIEW_LEN,
    };

    const NO_MESSAGE_INDEX_LOG_TIME_LINES: &[&str] = &[
        "0 /demo [Example] [1]",
        "1 /demo [Example] [3]",
        "2 /demo [Example] [2]",
    ];

    fn sample_message(schema_name: Option<&str>, data: Vec<u8>) -> mcap::Message<'static> {
        let schema = schema_name.map(|name| {
            Arc::new(mcap::Schema {
                id: 1,
                name: name.to_string(),
                encoding: "jsonschema".to_string(),
                data: Cow::Owned(Vec::new()),
            })
        });
        mcap::Message {
            channel: Arc::new(mcap::Channel {
                id: 1,
                topic: "/demo".to_string(),
                schema,
                message_encoding: "json".to_string(),
                metadata: BTreeMap::new(),
            }),
            sequence: 1,
            log_time: 42,
            publish_time: 43,
            data: Cow::Owned(data),
        }
    }

    fn payload_preview_string(data: &[u8], max_bytes: usize) -> String {
        let mut out = Vec::new();
        write_payload_preview(&mut out, data, max_bytes).expect("payload preview should serialize");
        String::from_utf8(out).expect("payload preview should be utf8")
    }

    fn message_line_string(message: &mcap::Message<'_>, max_preview_bytes: usize) -> String {
        let mut out = Vec::new();
        let schema_name = message
            .channel
            .schema
            .as_ref()
            .map(|schema| schema.name.as_str())
            .unwrap_or("no schema");
        let broken_pipe = super::write_message_fields(
            &mut out,
            message.log_time,
            &message.channel.topic,
            schema_name,
            message.data.as_ref(),
            max_preview_bytes,
        )
        .expect("message line should write");
        assert!(!broken_pipe);
        String::from_utf8(out)
            .expect("message line should be utf8")
            .trim_end_matches('\n')
            .to_string()
    }

    fn message_lines_from_stream(mcap: &[u8]) -> Vec<String> {
        mcap::MessageStream::new(mcap)
            .expect("message stream should open")
            .map(|message| {
                message_line_string(
                    &message.expect("message stream should read"),
                    MESSAGE_PREVIEW_LEN,
                )
            })
            .collect()
    }

    fn summary_with_channels(
        summary_channel_ids: &[u16],
        indexed_channel_ids: &[u16],
    ) -> mcap::Summary {
        let mut summary = mcap::Summary::default();
        for channel_id in summary_channel_ids {
            summary.channels.insert(
                *channel_id,
                Arc::new(mcap::Channel {
                    id: *channel_id,
                    topic: format!("/topic_{channel_id}"),
                    schema: None,
                    message_encoding: "json".to_string(),
                    metadata: BTreeMap::new(),
                }),
            );
        }
        summary.chunk_indexes.push(mcap::records::ChunkIndex {
            message_start_time: 0,
            message_end_time: 10,
            chunk_start_offset: 0,
            chunk_length: 0,
            message_index_offsets: indexed_channel_ids
                .iter()
                .map(|channel_id| (*channel_id, 0))
                .collect(),
            message_index_length: 0,
            compression: String::new(),
            compressed_size: 0,
            uncompressed_size: 0,
        });
        summary
    }

    fn build_out_of_order_chunked_mcap() -> Vec<u8> {
        let mut cursor = Cursor::new(Vec::new());
        {
            let mut writer = mcap::WriteOptions::new()
                .chunk_size(Some(1024))
                .create(&mut cursor)
                .expect("writer");

            let schema_id = writer
                .add_schema("Example", "jsonschema", br#"{"type":"object"}"#)
                .expect("schema");
            let channel_id = writer
                .add_channel(schema_id, "/demo", "json", &BTreeMap::new())
                .expect("channel");

            writer
                .write_to_known_channel(
                    &mcap::records::MessageHeader {
                        channel_id,
                        sequence: 1,
                        log_time: 30,
                        publish_time: 30,
                    },
                    &[1],
                )
                .expect("write message 1");
            writer.flush().expect("flush chunk 1");

            writer
                .write_to_known_channel(
                    &mcap::records::MessageHeader {
                        channel_id,
                        sequence: 2,
                        log_time: 10,
                        publish_time: 10,
                    },
                    &[2],
                )
                .expect("write message 2");
            writer.flush().expect("flush chunk 2");

            writer.finish().expect("finish");
        }
        cursor.into_inner()
    }

    fn build_out_of_order_chunked_mcap_without_message_indexes() -> Vec<u8> {
        let mut cursor = Cursor::new(Vec::new());
        {
            let mut writer = mcap::WriteOptions::new()
                .chunk_size(None)
                .emit_message_indexes(false)
                .create(&mut cursor)
                .expect("writer");

            let schema_id = writer
                .add_schema("Example", "jsonschema", br#"{"type":"object"}"#)
                .expect("schema");
            let channel_id = writer
                .add_channel(schema_id, "/demo", "json", &BTreeMap::new())
                .expect("channel");

            for (sequence, log_time, data) in [(1, 0, 1), (2, 2, 2), (3, 1, 3)] {
                writer
                    .write_to_known_channel(
                        &mcap::records::MessageHeader {
                            channel_id,
                            sequence,
                            log_time,
                            publish_time: log_time,
                        },
                        &[data],
                    )
                    .expect("write message");
            }

            writer.finish().expect("finish");
        }
        cursor.into_inner()
    }

    fn build_out_of_order_linear_mcap_without_summary() -> Vec<u8> {
        let mut cursor = Cursor::new(Vec::new());
        {
            let mut writer = mcap::WriteOptions::new()
                .chunk_size(None)
                .emit_summary_records(false)
                .emit_summary_offsets(false)
                .create(&mut cursor)
                .expect("writer");

            let schema_id = writer
                .add_schema("Example", "jsonschema", br#"{"type":"object"}"#)
                .expect("schema");
            let channel_id = writer
                .add_channel(schema_id, "/demo", "json", &BTreeMap::new())
                .expect("channel");

            writer
                .write_to_known_channel(
                    &mcap::records::MessageHeader {
                        channel_id,
                        sequence: 1,
                        log_time: 30,
                        publish_time: 30,
                    },
                    &[1],
                )
                .expect("write message 1");

            writer
                .write_to_known_channel(
                    &mcap::records::MessageHeader {
                        channel_id,
                        sequence: 2,
                        log_time: 10,
                        publish_time: 10,
                    },
                    &[2],
                )
                .expect("write message 2");

            writer.finish().expect("finish");
        }
        cursor.into_inner()
    }

    fn serve_http(body: &'static [u8]) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
        let addr = listener.local_addr().expect("test server addr");
        thread::spawn(move || {
            for stream in listener.incoming().take(64) {
                let mut stream = stream.expect("accept test connection");
                let mut request = [0u8; 4096];
                let read = stream.read(&mut request).expect("read request");
                let request = String::from_utf8_lossy(&request[..read]);
                let requested_range = request
                    .lines()
                    .find_map(|line| line.strip_prefix("Range: bytes="))
                    .or_else(|| {
                        request
                            .lines()
                            .find_map(|line| line.strip_prefix("range: bytes="))
                    })
                    .and_then(|range| range.split_once('-'))
                    .and_then(|(start, end)| {
                        // Supports `S-E` (bounded), `-N` (suffix), and `S-` (open ended)
                        // forms, resolving each to an inclusive (start, end) over the body.
                        let len = body.len();
                        match (start.trim(), end.trim()) {
                            ("", suffix) => {
                                let n = suffix.parse::<usize>().ok()?;
                                Some((len.saturating_sub(n), len.saturating_sub(1)))
                            }
                            (start, "") => {
                                Some((start.parse::<usize>().ok()?, len.saturating_sub(1)))
                            }
                            (start, end) => {
                                Some((start.parse::<usize>().ok()?, end.parse::<usize>().ok()?))
                            }
                        }
                    });
                if let Some((start, end)) = requested_range {
                    let end = end.min(body.len().saturating_sub(1));
                    let start = start.min(end);
                    let content = &body[start..=end];
                    let response = format!(
                        "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{end}/{}\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n",
                        content.len(),
                        body.len(),
                    );
                    stream
                        .write_all(response.as_bytes())
                        .expect("write headers");
                    stream.write_all(content).expect("write range body");
                } else {
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    stream
                        .write_all(response.as_bytes())
                        .expect("write headers");
                    stream.write_all(body).expect("write body");
                }
            }
        });
        format!("http://{addr}/demo.mcap")
    }

    fn build_multi_topic_mcap() -> Vec<u8> {
        let mut cursor = Cursor::new(Vec::new());
        {
            let mut writer = mcap::WriteOptions::new()
                .chunk_size(Some(1024))
                .create(&mut cursor)
                .expect("writer");
            let schema_id = writer
                .add_schema("Example", "jsonschema", br#"{"type":"object"}"#)
                .expect("schema");
            let camera_id = writer
                .add_channel(schema_id, "/camera", "json", &BTreeMap::new())
                .expect("camera channel");
            let radar_id = writer
                .add_channel(schema_id, "/radar", "json", &BTreeMap::new())
                .expect("radar channel");
            for (sequence, channel_id, log_time, data) in [
                (1, camera_id, 10, br#"{"camera":1}"#.as_slice()),
                (2, radar_id, 20, br#"{"radar":1}"#.as_slice()),
                (3, camera_id, 30, br#"{"camera":2}"#.as_slice()),
            ] {
                writer
                    .write_to_known_channel(
                        &mcap::records::MessageHeader {
                            channel_id,
                            sequence,
                            log_time,
                            publish_time: log_time,
                        },
                        data,
                    )
                    .expect("write message");
            }
            writer.finish().expect("finish");
        }
        cursor.into_inner()
    }

    #[test]
    fn payload_preview_includes_full_message_when_short() {
        assert_eq!(payload_preview_string(&[1, 2, 3], 10), "[1 2 3]");
    }

    #[test]
    fn payload_preview_truncates_with_ellipsis() {
        let data: Vec<u8> = (0..12).collect();
        assert_eq!(
            payload_preview_string(&data, 10),
            "[0 1 2 3 4 5 6 7 8 9]..."
        );
    }

    #[test]
    fn message_line_includes_schema_name_when_present() {
        let message = sample_message(Some("Example"), vec![1, 2, 3]);
        assert_eq!(
            message_line_string(&message, 10),
            "42 /demo [Example] [1 2 3]"
        );
    }

    #[test]
    fn message_line_uses_no_schema_for_schemaless_channel() {
        let message = sample_message(None, vec![1, 2, 3]);
        assert_eq!(
            message_line_string(&message, 10),
            "42 /demo [no schema] [1 2 3]"
        );
    }

    #[test]
    fn remote_cat_with_scan_opt_in_falls_back_for_unchunked_messages() {
        let body: &'static [u8] =
            Box::leak(build_out_of_order_linear_mcap_without_summary().into_boxed_slice());
        let url = serve_http(body);
        let mut out = Vec::new();
        let broken_pipe = super::cat_file(
            &mut out,
            Path::new(&url),
            &CatOptions::default(),
            crate::source::SourceOptions::new(true),
        )
        .expect("remote cat should scan unchunked messages with opt-in");
        assert!(!broken_pipe);
        let output = String::from_utf8(out).expect("cat output should be utf8");
        assert!(output.contains("30 /demo [Example] [1]"));
        assert!(output.contains("10 /demo [Example] [2]"));
    }

    #[test]
    fn remote_cat_with_scan_opt_in_uses_chunk_index_without_message_indexes() {
        let body: &'static [u8] =
            Box::leak(build_out_of_order_chunked_mcap_without_message_indexes().into_boxed_slice());
        let url = serve_http(body);
        let mut out = Vec::new();
        let broken_pipe = super::cat_file(
            &mut out,
            Path::new(&url),
            &CatOptions::default(),
            crate::source::SourceOptions::new(true),
        )
        .expect("remote cat should use chunk indexes with opt-in");
        assert!(!broken_pipe);

        let output = String::from_utf8(out).expect("valid utf8 output");
        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(lines.as_slice(), NO_MESSAGE_INDEX_LOG_TIME_LINES);
    }

    #[test]
    fn remote_cat_no_chunk_index_error_includes_redacted_url() {
        let mut buffer = Vec::new();
        {
            let mut writer = mcap::Writer::new(Cursor::new(&mut buffer)).expect("writer");
            writer.finish().expect("finish writer");
        }
        let body: &'static [u8] = Box::leak(buffer.into_boxed_slice());
        let url = serve_http(body) + "?token=secret";
        let mut out = Vec::new();
        let err = super::cat_file(
            &mut out,
            Path::new(&url),
            &CatOptions::default(),
            crate::source::SourceOptions::default(),
        )
        .expect_err("remote cat without chunk indexes should require opt-in");
        let message = err.to_string();
        assert!(message.contains("--allow-remote-scan"));
        assert!(message.contains("/demo.mcap"));
        assert!(!message.contains("secret"));
    }

    #[test]
    fn remote_cat_requires_allow_remote_scan_before_chunk_reads() {
        let body: &'static [u8] = Box::leak(build_multi_topic_mcap().into_boxed_slice());
        let url = serve_http(body);
        let mut out = Vec::new();
        let err = super::cat_file(
            &mut out,
            Path::new(&url),
            &CatOptions::default(),
            crate::source::SourceOptions::default(),
        )
        .expect_err("remote cat should require opt-in before reading chunks");
        assert!(err.to_string().contains("remote cat would read"));
        assert!(err.to_string().contains("--allow-remote-scan"));
    }

    fn planned_chunks_for_opts<'a>(
        summary: &'a mcap::Summary,
        opts: &CatOptions,
    ) -> Vec<&'a mcap::records::ChunkIndex> {
        let needs_in_chunk_definitions = needs_in_chunk_definitions(summary);
        let included_topics: BTreeSet<String> = summary
            .channels
            .values()
            .filter(|channel| opts.include_topic(&channel.topic))
            .map(|channel| channel.topic.clone())
            .collect();
        if !opts.topics.is_empty() && included_topics.is_empty() && !needs_in_chunk_definitions {
            return Vec::new();
        }
        planned_chunk_reads(summary, opts, &included_topics, needs_in_chunk_definitions)
    }

    #[test]
    fn remote_chunk_plan_is_conservative_for_representative_filters() {
        let mcap = build_multi_topic_mcap();
        let summary = mcap::Summary::read(&mcap)
            .expect("summary read")
            .expect("summary should exist");
        assert!(!needs_in_chunk_definitions(&summary));

        assert!(
            !planned_chunks_for_opts(&summary, &CatOptions::default()).is_empty(),
            "unfiltered cat would need remote chunk payload reads"
        );

        assert!(
            !planned_chunks_for_opts(
                &summary,
                &CatOptions {
                    topics: vec!["/camera".to_string()],
                    ..CatOptions::default()
                },
            )
            .is_empty(),
            "matching topic filter would still need remote chunk payload reads"
        );

        assert!(
            planned_chunks_for_opts(
                &summary,
                &CatOptions {
                    topics: vec!["/missing".to_string()],
                    ..CatOptions::default()
                },
            )
            .is_empty(),
            "non-matching topic filter should not plan remote chunk reads"
        );

        assert!(
            !planned_chunks_for_opts(
                &summary,
                &CatOptions {
                    start: 20,
                    ..CatOptions::default()
                },
            )
            .is_empty(),
            "overlapping time filter would need remote chunk payload reads"
        );

        assert!(
            planned_chunks_for_opts(
                &summary,
                &CatOptions {
                    start: 100,
                    ..CatOptions::default()
                },
            )
            .is_empty(),
            "non-overlapping time filter should not plan remote chunk reads"
        );
    }

    #[test]
    fn mixed_chunk_local_channels_disable_summary_topic_pruning() {
        let summary = summary_with_channels(&[1], &[1, 2]);
        assert!(needs_in_chunk_definitions(&summary));

        assert!(
            !planned_chunks_for_opts(
                &summary,
                &CatOptions {
                    topics: vec!["/topic_1".to_string()],
                    ..CatOptions::default()
                },
            )
            .is_empty(),
            "mixed summaries still need chunk reads because chunk-local topics are unknown"
        );

        let complete_summary = summary_with_channels(&[1, 2], &[1, 2]);
        assert!(!needs_in_chunk_definitions(&complete_summary));
    }

    #[test]
    fn chunk_local_channels_keep_remote_topic_plan_conservative() {
        let mcap = include_bytes!(
            "../../../../tests/conformance/data/OneMessage/OneMessage-ch-chx-mx.mcap"
        );
        let summary = mcap::Summary::read(mcap)
            .expect("summary read")
            .expect("summary should exist");
        assert!(needs_in_chunk_definitions(&summary));

        assert!(
            !planned_chunks_for_opts(
                &summary,
                &CatOptions {
                    topics: vec!["example".to_string()],
                    ..CatOptions::default()
                },
            )
            .is_empty(),
            "topic filters cannot safely prune chunks before reading chunk-local channels"
        );
    }

    #[test]
    fn cat_prefers_log_time_order_when_index_available() {
        let mcap = build_out_of_order_chunked_mcap();
        let mut out = Vec::new();
        let broken_pipe =
            cat_mcap(&mut out, &mcap, &CatOptions::default()).expect("cat should succeed");
        assert!(!broken_pipe);

        let output = String::from_utf8(out).expect("valid utf8 output");
        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(
            lines,
            vec!["10 /demo [Example] [2]", "30 /demo [Example] [1]"]
        );
    }

    #[test]
    fn cat_prefers_log_time_order_with_chunk_indexes_without_message_indexes() {
        let mcap = build_out_of_order_chunked_mcap_without_message_indexes();
        let summary = mcap::Summary::read(&mcap)
            .expect("summary read")
            .expect("summary should exist");
        assert!(!summary.chunk_indexes.is_empty());
        assert!(summary
            .chunk_indexes
            .iter()
            .all(|chunk| chunk.message_index_offsets.is_empty()));
        assert!(
            !needs_in_chunk_definitions(&summary),
            "complete summaries do not need an up-front chunk definition scan"
        );

        let mut indexed_out = Vec::new();
        let mut json_transcoders = JsonTranscoders::default();
        let indexed_result = cat_indexed(
            &mut indexed_out,
            &mcap,
            &CatOptions::default(),
            &mut json_transcoders,
        )
        .expect("indexed cat should succeed");
        assert_eq!(indexed_result, Some(false));

        let output = String::from_utf8(indexed_out).expect("valid utf8 output");
        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(lines.as_slice(), NO_MESSAGE_INDEX_LOG_TIME_LINES);
    }

    #[test]
    fn cat_falls_back_to_linear_order_without_index() {
        let mcap = build_out_of_order_linear_mcap_without_summary();
        let mut out = Vec::new();
        let broken_pipe =
            cat_mcap(&mut out, &mcap, &CatOptions::default()).expect("cat should succeed");
        assert!(!broken_pipe);

        let output = String::from_utf8(out).expect("valid utf8 output");
        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(
            lines,
            vec!["30 /demo [Example] [1]", "10 /demo [Example] [2]"]
        );
    }

    #[test]
    fn cat_indexed_reads_chunk_index_without_message_indexes_and_in_chunk_channels() {
        let mcap =
            include_bytes!("../../../../tests/conformance/data/OneMessage/OneMessage-ch-chx.mcap");
        let expected = message_lines_from_stream(mcap);
        assert_eq!(expected.len(), 1);

        let mut indexed_out = Vec::new();
        let mut json_transcoders = JsonTranscoders::default();
        let indexed_result = cat_indexed(
            &mut indexed_out,
            mcap,
            &CatOptions::default(),
            &mut json_transcoders,
        )
        .expect("indexed cat should succeed");
        assert_eq!(indexed_result, Some(false));
        let indexed_output = String::from_utf8(indexed_out).expect("valid utf8 output");
        let indexed_lines: Vec<&str> = indexed_output.lines().collect();
        assert_eq!(indexed_lines, expected);

        let mut out = Vec::new();
        let broken_pipe = cat_mcap(&mut out, mcap, &CatOptions::default())
            .expect("cat should succeed through indexed chunk scan");
        assert!(!broken_pipe);

        let output = String::from_utf8(out).expect("valid utf8 output");
        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(lines, expected);
    }

    #[test]
    fn cat_indexed_reads_message_index_with_in_chunk_channels() {
        let mcap = include_bytes!(
            "../../../../tests/conformance/data/OneMessage/OneMessage-ch-chx-mx.mcap"
        );
        let expected = message_lines_from_stream(mcap);
        assert_eq!(expected.len(), 1);
        let summary = mcap::Summary::read(mcap)
            .expect("summary read")
            .expect("summary should exist");
        assert!(summary.channels.is_empty());
        assert!(summary
            .chunk_indexes
            .iter()
            .all(|chunk| !chunk.message_index_offsets.is_empty()));

        let mut out = Vec::new();
        let mut json_transcoders = JsonTranscoders::default();
        let indexed_result = cat_indexed(
            &mut out,
            mcap,
            &CatOptions::default(),
            &mut json_transcoders,
        )
        .expect("indexed cat should succeed");
        assert_eq!(indexed_result, Some(false));

        let output = String::from_utf8(out).expect("valid utf8 output");
        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(lines, expected);
    }

    #[test]
    fn cat_falls_back_for_summary_channel_with_in_chunk_schema() {
        // Spec-valid file: the channel is repeated in the summary, but its schema is defined only
        // inside the chunk. The indexed planner can't resolve the schema from the summary, so it
        // defers to the linear scan, which registers the in-chunk schema and resolves the message.
        let mcap = include_bytes!(
            "../../../../tests/conformance/data/OneMessage/OneMessage-ch-chx-mx-rch-st-sum.mcap"
        );
        let expected = message_lines_from_stream(mcap);
        assert_eq!(expected.len(), 1);

        let mut indexed_out = Vec::new();
        let mut json_transcoders = JsonTranscoders::default();
        let indexed_result = cat_indexed(
            &mut indexed_out,
            mcap,
            &CatOptions::default(),
            &mut json_transcoders,
        )
        .expect("indexed planner should fall back");
        assert_eq!(indexed_result, None);
        assert!(indexed_out.is_empty());

        let mut out = Vec::new();
        let broken_pipe = cat_mcap(&mut out, mcap, &CatOptions::default())
            .expect("cat should succeed through linear fallback");
        assert!(!broken_pipe);

        let output = String::from_utf8(out).expect("valid utf8 output");
        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(lines, expected);
    }

    fn build_multi_chunk_chunk_local_mcap() -> Vec<u8> {
        let mut buffer = Vec::new();
        {
            // A small schema/channel plus large messages and a mid-size chunk target keeps the
            // schema+channel+first message together in chunk 0, then forces the second message into
            // chunk 1. The chunk-flush check runs before each message, so the threshold must exceed
            // the schema+channel size (to avoid a message-less chunk) but be smaller than chunk 0
            // once the first message is added.
            let mut writer = mcap::WriteOptions::new()
                .chunk_size(Some(150))
                .repeat_channels(false)
                .repeat_schemas(false)
                .create(Cursor::new(&mut buffer))
                .expect("writer");
            let schema_id = writer.add_schema("Example", "json", b"x").expect("schema");
            let channel_id = writer
                .add_channel(schema_id, "/topic", "json", &BTreeMap::new())
                .expect("channel");
            let payload = vec![0u8; 200];
            for (sequence, log_time) in [(1u32, 10u64), (2, 20)] {
                writer
                    .write_to_known_channel(
                        &mcap::records::MessageHeader {
                            channel_id,
                            sequence,
                            log_time,
                            publish_time: log_time,
                        },
                        &payload,
                    )
                    .expect("write message");
            }
            writer.finish().expect("finish writer");
        }
        buffer
    }

    #[test]
    fn cat_indexed_resolves_chunk_local_channel_defined_in_filtered_out_chunk() {
        // Multiple chunks, channels defined only inside chunks. The channel is defined in the first
        // chunk, but a start-time filter excludes that chunk while keeping a later chunk's message.
        // The channel must still resolve from the up-front in-chunk definition collection.
        let mcap = build_multi_chunk_chunk_local_mcap();
        let summary = mcap::Summary::read(&mcap)
            .expect("summary read")
            .expect("summary should exist");
        assert!(summary.channels.is_empty());
        assert!(
            summary.chunk_indexes.len() >= 2,
            "expected multiple chunks, got {}",
            summary.chunk_indexes.len()
        );
        assert!(summary
            .chunk_indexes
            .iter()
            .all(|chunk| !chunk.message_index_offsets.is_empty()));
        assert!(needs_in_chunk_definitions(&summary));
        // The first chunk (which defines the channel) is fully before the start filter.
        assert!(summary.chunk_indexes[0].message_end_time < 15);

        let opts = CatOptions {
            start: 15,
            ..CatOptions::default()
        };
        let mut out = Vec::new();
        let mut json_transcoders = JsonTranscoders::default();
        let indexed_result = cat_indexed(&mut out, &mcap, &opts, &mut json_transcoders)
            .expect("indexed cat should resolve chunk-local channel");
        assert_eq!(indexed_result, Some(false));

        let output = String::from_utf8(out).expect("valid utf8 output");
        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(
            lines.len(),
            1,
            "only the second message is within the window"
        );
        assert!(lines[0].contains("/topic"), "unexpected line: {}", lines[0]);
        assert!(
            lines[0].contains("Example"),
            "schema should resolve from the defining chunk: {}",
            lines[0]
        );
    }

    #[test]
    fn cat_indexed_applies_topic_filter_to_chunk_local_channels() {
        // The channel is defined only inside the chunk, so reader-level topic filtering is disabled
        // and the per-message `include_topic` check is the sole filter. Exercise both a matching and
        // a non-matching topic end-to-end through `cat_indexed` (not the linear fallback).
        let mcap = include_bytes!(
            "../../../../tests/conformance/data/OneMessage/OneMessage-ch-chx-mx.mcap"
        );
        let summary = mcap::Summary::read(mcap)
            .expect("summary read")
            .expect("summary should exist");
        assert!(summary.channels.is_empty());
        assert!(needs_in_chunk_definitions(&summary));

        // Matching topic keeps the chunk-local channel's message.
        let mut matched = Vec::new();
        let mut json_transcoders = JsonTranscoders::default();
        let result = cat_indexed(
            &mut matched,
            mcap,
            &CatOptions {
                topics: vec!["example".to_string()],
                ..CatOptions::default()
            },
            &mut json_transcoders,
        )
        .expect("indexed cat should succeed");
        assert_eq!(result, Some(false));
        let matched = String::from_utf8(matched).expect("valid utf8 output");
        assert_eq!(matched.lines().count(), 1);

        // Non-matching topic drops the message via the per-message check (not silently via
        // reader-level filtering), and the indexed path still completes without a linear fallback.
        let mut filtered = Vec::new();
        let mut json_transcoders = JsonTranscoders::default();
        let result = cat_indexed(
            &mut filtered,
            mcap,
            &CatOptions {
                topics: vec!["nope".to_string()],
                ..CatOptions::default()
            },
            &mut json_transcoders,
        )
        .expect("indexed cat should succeed");
        assert_eq!(result, Some(false));
        assert!(filtered.is_empty());
    }

    #[test]
    fn cat_applies_topic_and_time_filters() {
        let mcap = build_multi_topic_mcap();
        let opts = CatOptions {
            topics: vec!["/camera".to_string()],
            start: 20,
            end: None,
            json: false,
        };
        let mut out = Vec::new();
        let broken_pipe = cat_mcap(&mut out, &mcap, &opts).expect("cat should succeed");
        assert!(!broken_pipe);

        let output = String::from_utf8(out).expect("valid utf8 output");
        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(
            lines,
            vec![r#"30 /camera [Example] [123 34 99 97 109 101 114 97 34 58]..."#]
        );
    }

    #[test]
    fn cat_streaming_reads_without_buffering_full_input() {
        let mcap = build_multi_topic_mcap();
        let opts = CatOptions {
            topics: vec!["/radar".to_string()],
            ..CatOptions::default()
        };
        let mut out = Vec::new();
        let broken_pipe = cat_streaming(&mut out, Cursor::new(mcap), &opts)
            .expect("streaming cat should succeed");
        assert!(!broken_pipe);

        let output = String::from_utf8(out).expect("valid utf8 output");
        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(
            lines,
            vec![r#"20 /radar [Example] [123 34 114 97 100 97 114 34 58 49]..."#]
        );
    }

    #[test]
    fn cat_json_wraps_jsonschema_messages() {
        let message = sample_message(Some("Example"), br#"{"value":1}"#.to_vec());
        let mut out = Vec::new();
        let mut transcoders = JsonTranscoders::default();
        let opts = CatOptions {
            json: true,
            ..CatOptions::default()
        };
        let cat_message = super::CatMessage {
            channel: &message.channel,
            sequence: message.sequence,
            log_time: message.log_time,
            publish_time: message.publish_time,
            data: message.data.as_ref(),
        };
        let broken_pipe = super::write_message(&mut out, cat_message, &opts, &mut transcoders)
            .expect("json message should write");
        assert!(!broken_pipe);

        assert_eq!(
            String::from_utf8(out).expect("valid utf8 output"),
            r#"{"topic":"/demo","sequence":1,"log_time":0.000000042,"publish_time":0.000000043,"data":{"value":1}}"#
                .to_string()
                + "\n"
        );
    }

    #[test]
    fn cat_json_passes_schemaless_json_messages() {
        let message = sample_message(None, br#"{"value":1}"#.to_vec());
        let mut out = Vec::new();
        let mut transcoders = JsonTranscoders::default();
        let opts = CatOptions {
            json: true,
            ..CatOptions::default()
        };
        let cat_message = super::CatMessage {
            channel: &message.channel,
            sequence: message.sequence,
            log_time: message.log_time,
            publish_time: message.publish_time,
            data: message.data.as_ref(),
        };
        let broken_pipe = super::write_message(&mut out, cat_message, &opts, &mut transcoders)
            .expect("schemaless json message should write");
        assert!(!broken_pipe);

        assert_eq!(
            String::from_utf8(out).expect("valid utf8 output"),
            r#"{"topic":"/demo","sequence":1,"log_time":0.000000042,"publish_time":0.000000043,"data":{"value":1}}"#
                .to_string()
                + "\n"
        );
    }

    #[test]
    fn protobuf_json_uses_lower_camel_case_and_omits_zero_values() {
        let descriptor = vec![
            10, 122, 10, 12, 115, 97, 109, 112, 108, 101, 46, 112, 114, 111, 116, 111, 18, 4, 116,
            101, 115, 116, 34, 92, 10, 6, 83, 97, 109, 112, 108, 101, 18, 29, 10, 10, 115, 110, 97,
            107, 101, 95, 99, 97, 115, 101, 24, 1, 32, 1, 40, 9, 82, 9, 115, 110, 97, 107, 101, 67,
            97, 115, 101, 18, 29, 10, 10, 122, 101, 114, 111, 95, 118, 97, 108, 117, 101, 24, 2,
            32, 1, 40, 13, 82, 9, 122, 101, 114, 111, 86, 97, 108, 117, 101, 18, 20, 10, 5, 99,
            111, 117, 110, 116, 24, 3, 32, 1, 40, 13, 82, 5, 99, 111, 117, 110, 116, 98, 6, 112,
            114, 111, 116, 111, 51,
        ];
        let schema = Arc::new(mcap::Schema {
            id: 1,
            name: "test.Sample".to_string(),
            encoding: "protobuf".to_string(),
            data: Cow::Owned(descriptor),
        });
        let channel = Arc::new(mcap::Channel {
            id: 1,
            topic: "proto".to_string(),
            schema: Some(schema),
            message_encoding: "protobuf".to_string(),
            metadata: BTreeMap::new(),
        });
        let mut transcoders = JsonTranscoders::default();
        let encoded = transcoders
            .encode(&channel, &[10, 5, b'h', b'e', b'l', b'l', b'o', 24, 7])
            .expect("protobuf should encode");
        assert_eq!(
            String::from_utf8(encoded.into_owned()).expect("valid utf8"),
            r#"{"snakeCase":"hello","count":7}"#
        );
    }

    #[test]
    fn ros1_transcoder_handles_nested_messages_and_arrays() {
        let schema = b"Header header\nint32[] values\nstring label\n================================================================================\nMSG: std_msgs/Header\nuint32 seq\ntime stamp\nstring frame_id\n";
        let transcoder =
            Ros1MessageDef::parse("demo/Example", schema).expect("schema should parse");
        let mut data = Vec::new();
        data.extend_from_slice(&7u32.to_le_bytes());
        data.extend_from_slice(&1u32.to_le_bytes());
        data.extend_from_slice(&2u32.to_le_bytes());
        data.extend_from_slice(&3u32.to_le_bytes());
        data.extend_from_slice(b"map");
        data.extend_from_slice(&2u32.to_le_bytes());
        data.extend_from_slice(&10i32.to_le_bytes());
        data.extend_from_slice(&20i32.to_le_bytes());
        data.extend_from_slice(&5u32.to_le_bytes());
        data.extend_from_slice(b"hello");

        let json = transcoder
            .transcode(&data)
            .expect("message should transcode");
        assert_eq!(
            String::from_utf8(json).expect("valid utf8"),
            r#"{"header":{"seq":7,"stamp":1.000000002,"frame_id":"map"},"values":[10,20],"label":"hello"}"#
        );
    }

    #[test]
    fn ros1_duration_formats_signed_total_nanoseconds() {
        let mut out = Vec::new();
        write_signed_decimal_time(&mut out, 5, -100).expect("duration should format");
        assert_eq!(String::from_utf8(out).expect("valid utf8"), "4.999999900");

        let mut out = Vec::new();
        write_signed_decimal_time(&mut out, -5, 100).expect("duration should format");
        assert_eq!(String::from_utf8(out).expect("valid utf8"), "-4.999999900");
    }

    #[test]
    fn malformed_ros1_array_type_does_not_panic() {
        let field_type = parse_ros1_field_type("int32[");
        assert_eq!(field_type.base, "int32");
        assert!(field_type.array.is_none());
    }

    #[test]
    fn bounded_ros1_array_type_is_variable_length_array() {
        let field_type = parse_ros1_field_type("int32[<=10]");
        assert_eq!(field_type.base, "int32");
        assert_eq!(field_type.array, Some(None));
    }

    #[test]
    fn bounded_ros1_scalar_type_strips_bound() {
        let field_type = parse_ros1_field_type("string<=10");
        assert_eq!(field_type.base, "string");
        assert!(field_type.array.is_none());
    }

    #[test]
    fn ros1_float_special_values_match_protojson_strings() {
        let mut out = Vec::new();
        write_ros1_float(&mut out, f64::NAN).expect("nan should write");
        assert_eq!(String::from_utf8(out).expect("valid utf8"), r#""NaN""#);

        let mut out = Vec::new();
        write_ros1_float(&mut out, f64::INFINITY).expect("infinity should write");
        assert_eq!(String::from_utf8(out).expect("valid utf8"), r#""Infinity""#);

        let mut out = Vec::new();
        write_ros1_float(&mut out, f64::NEG_INFINITY).expect("negative infinity should write");
        assert_eq!(
            String::from_utf8(out).expect("valid utf8"),
            r#""-Infinity""#
        );
    }
}
