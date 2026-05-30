//! Best-effort recovery for damaged MCAP files.
//!
//! Recovery currently takes a fully buffered input (`&[u8]`) and emits best-effort warnings to
//! stderr while salvaging records.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Seek, Write};

use crate::records::{self, op, MessageIndex, MessageIndexEntry, Record, OPCODE_LEN_SIZE};
use crate::sans_io::{LinearReadEvent, LinearReader, LinearReaderOptions};
use crate::{Compression, McapError, McapResult, WriteOptions, Writer, MAGIC};

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct RecoverOptions {
    pub compression: Option<Compression>,
    pub chunk_size: u64,
    pub always_decode_chunk: bool,
    pub disable_seeking: bool,
}

impl Default for RecoverOptions {
    fn default() -> Self {
        Self {
            #[cfg(feature = "zstd")]
            compression: Some(Compression::Zstd),
            #[cfg(not(feature = "zstd"))]
            compression: None,
            chunk_size: 4 * 1024 * 1024,
            always_decode_chunk: false,
            disable_seeking: false,
        }
    }
}

impl RecoverOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn compression(self, compression: Option<Compression>) -> Self {
        Self {
            compression,
            ..self
        }
    }

    pub fn chunk_size(self, chunk_size: u64) -> Self {
        Self { chunk_size, ..self }
    }

    pub fn always_decode_chunk(self, always_decode_chunk: bool) -> Self {
        Self {
            always_decode_chunk,
            ..self
        }
    }

    pub fn disable_seeking(self, disable_seeking: bool) -> Self {
        Self {
            disable_seeking,
            ..self
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct RecoverStats {
    pub messages: u64,
    pub attachments: u64,
    pub metadata: u64,
}

#[derive(Debug, Clone)]
struct RawChunk {
    header: records::ChunkHeader,
    data: Vec<u8>,
}

#[derive(Debug, Clone, Copy)]
enum RegistrationMode {
    WriteRecords,
    SummaryOnly,
}

#[derive(Debug, Clone)]
enum ChunkDefinition {
    Schema {
        id: u16,
        name: String,
        encoding: String,
        data: Vec<u8>,
    },
    Channel(records::Channel),
}

#[derive(Debug, Default, Clone)]
struct ChunkScan {
    definitions: Vec<ChunkDefinition>,
    indexes: Vec<MessageIndex>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SchemaDef {
    name: String,
    encoding: String,
    data: Vec<u8>,
}

#[derive(Default)]
struct RecoveryState {
    schema_map: BTreeMap<u16, u16>,
    channel_map: BTreeMap<u16, u16>,
    seen_schemas: BTreeMap<u16, SchemaDef>,
    seen_channels: BTreeMap<u16, records::Channel>,
    pending_channels: BTreeMap<u16, records::Channel>,
    warned_missing_channels: BTreeSet<u16>,
}

/// Recover records from a possibly damaged MCAP buffer into `sink`.
///
/// The entire input must already be buffered in memory. This is suitable for callers that have
/// mapped or loaded a file; streaming recovery is not currently exposed by this API.
pub fn recover_to_sink<W: Write + Seek>(
    input: &[u8],
    sink: W,
    opts: &RecoverOptions,
) -> McapResult<(RecoverStats, W)> {
    validate_start_magic(input)?;

    let mut write_options = WriteOptions::new()
        .chunk_size(Some(opts.chunk_size))
        .compression(opts.compression)
        .disable_seeking(opts.disable_seeking);

    if let Some(header) = sniff_header(input) {
        write_options = write_options
            .profile(header.profile)
            .library(header.library);
    }

    let mut writer = write_options.create(sink)?;
    let stats = recover_records(input, &mut writer, opts)?;
    writer.finish()?;
    let sink = writer.into_inner();
    Ok((stats, sink))
}

fn validate_start_magic(input: &[u8]) -> McapResult<()> {
    if input.len() < MAGIC.len() || !input.starts_with(MAGIC) {
        return Err(McapError::BadMagic);
    }
    Ok(())
}

fn sniff_header(input: &[u8]) -> Option<records::Header> {
    let offset = MAGIC.len();
    if input.len() < offset + OPCODE_LEN_SIZE {
        return None;
    }
    if input[offset] != op::HEADER {
        return None;
    }
    let length = u64::from_le_bytes(
        input[offset + 1..offset + OPCODE_LEN_SIZE]
            .try_into()
            .expect("record header len slice"),
    );
    let Ok(length) = usize::try_from(length) else {
        return None;
    };
    let body_start = offset + OPCODE_LEN_SIZE;
    let body_end = body_start.checked_add(length)?;
    let body = input.get(body_start..body_end)?;
    match crate::parse_record(op::HEADER, body) {
        Ok(Record::Header(header)) => Some(header),
        Ok(_) | Err(_) => None,
    }
}

fn recover_records<W: Write + Seek>(
    input: &[u8],
    writer: &mut Writer<W>,
    opts: &RecoverOptions,
) -> McapResult<RecoverStats> {
    let mut reader = LinearReader::new_with_options(
        LinearReaderOptions::default()
            .with_skip_end_magic(true)
            .with_emit_chunks(!opts.always_decode_chunk)
            // Recover should ignore chunk CRC mismatches and continue decoding payload data.
            .with_validate_chunk_crcs(false)
            .with_record_length_limit(input.len()),
    );

    let mut remaining = input;
    let mut state = RecoveryState::default();
    let mut stats = RecoverStats::default();
    let mut saw_any_record = false;
    let mut pending_chunk: Option<RawChunk> = None;
    let mut pending_indexes = Vec::new();
    let mut rebuild_chunk_indexes = opts.always_decode_chunk;

    while let Some(event) = reader.next_event() {
        match event {
            Ok(LinearReadEvent::ReadRequest(need)) => {
                let read = need.min(remaining.len());
                let dst = reader.insert(read);
                dst.copy_from_slice(&remaining[..read]);
                reader.notify_read(read);
                remaining = &remaining[read..];
            }
            Ok(LinearReadEvent::Record { opcode, data }) => {
                saw_any_record = true;
                if !rebuild_chunk_indexes && opcode != op::MESSAGE_INDEX {
                    flush_pending_chunk(
                        writer,
                        &mut state,
                        &mut stats,
                        &mut pending_chunk,
                        &mut pending_indexes,
                        false,
                    )?;
                }

                let record = match crate::parse_record(opcode, data) {
                    Ok(record) => record,
                    Err(err) => {
                        eprintln!(
                            "Warning: failed to parse record opcode 0x{opcode:02x}: {err:#}; skipping"
                        );
                        continue;
                    }
                };

                if !rebuild_chunk_indexes {
                    match record {
                        Record::Chunk { header, data } => {
                            pending_chunk = Some(RawChunk {
                                header,
                                data: data.into_owned(),
                            });
                            pending_indexes.clear();
                            continue;
                        }
                        Record::MessageIndex(index) => {
                            if pending_chunk.is_none() {
                                eprintln!(
                                    "Warning: got message index for channel {} but no preceding chunk; skipping",
                                    index.channel_id
                                );
                                continue;
                            }
                            pending_indexes.push(index);
                            continue;
                        }
                        Record::DataEnd(_) | Record::Footer(_) => break,
                        Record::Schema { .. } | Record::Channel(_) | Record::Message { .. } => {
                            // Match Go recover's mid-stream behavior: once a top-level schema,
                            // channel, or message proves the input is not fully chunked,
                            // subsequent chunk records are still copied raw but their indexes are
                            // rebuilt from chunk contents.
                            rebuild_chunk_indexes = true;
                        }
                        _ => {}
                    }
                } else {
                    match record {
                        Record::Chunk { header, data } => {
                            let mut chunk = Some(RawChunk {
                                header,
                                data: data.into_owned(),
                            });
                            let mut indexes = Vec::new();
                            flush_pending_chunk(
                                writer,
                                &mut state,
                                &mut stats,
                                &mut chunk,
                                &mut indexes,
                                true,
                            )?;
                            continue;
                        }
                        Record::MessageIndex(index) => {
                            eprintln!(
                                "Warning: got message index for channel {} outside a copied raw chunk; skipping",
                                index.channel_id
                            );
                            continue;
                        }
                        Record::DataEnd(_) | Record::Footer(_) => break,
                        _ => {}
                    }
                }

                recover_record(
                    writer,
                    &mut state,
                    &mut stats,
                    record,
                    RegistrationMode::WriteRecords,
                )?;
            }
            Err(err) => {
                if !opts.always_decode_chunk {
                    flush_pending_chunk(
                        writer,
                        &mut state,
                        &mut stats,
                        &mut pending_chunk,
                        &mut pending_indexes,
                        true,
                    )?;
                }
                if !saw_any_record {
                    return Err(err);
                }
                // LinearReader does not provide a resync primitive after stream-level decode
                // failures (e.g. corrupt compressed chunk payload), so stop and keep recovered
                // records written so far.
                eprintln!("Warning: {err:#} -- stopping recovery scan");
                break;
            }
        }
    }
    if !opts.always_decode_chunk {
        flush_pending_chunk(
            writer,
            &mut state,
            &mut stats,
            &mut pending_chunk,
            &mut pending_indexes,
            true,
        )?;
    }
    Ok(stats)
}

fn flush_pending_chunk<W: Write + Seek>(
    writer: &mut Writer<W>,
    state: &mut RecoveryState,
    stats: &mut RecoverStats,
    pending_chunk: &mut Option<RawChunk>,
    pending_indexes: &mut Vec<MessageIndex>,
    force_rebuild_indexes: bool,
) -> McapResult<()> {
    let Some(chunk) = pending_chunk.take() else {
        pending_indexes.clear();
        return Ok(());
    };

    let supplied_indexes = if !force_rebuild_indexes && !pending_indexes.is_empty() {
        Some(pending_indexes.as_slice())
    } else {
        None
    };

    if let Err(err) = validate_raw_chunk(&chunk, supplied_indexes) {
        eprintln!("Failed to write chunk, skipping: {err:#}");
        pending_indexes.clear();
        return Ok(());
    }

    let indexes = match update_info_from_chunk(writer, state, &chunk, supplied_indexes) {
        Ok(indexes) => indexes,
        Err(err) => {
            eprintln!("Failed to update info from chunk, skipping: {err:#}");
            pending_indexes.clear();
            return Ok(());
        }
    };

    writer.write_chunk_with_indexes(&chunk.header, &chunk.data, &indexes)?;
    stats.messages += indexes
        .iter()
        .map(|index| index.records.len() as u64)
        .sum::<u64>();
    pending_indexes.clear();
    Ok(())
}

fn validate_raw_chunk(
    chunk: &RawChunk,
    supplied_indexes: Option<&[MessageIndex]>,
) -> McapResult<()> {
    if chunk.header.compressed_size != chunk.data.len() as u64 {
        return Err(McapError::BadChunkLength {
            header: chunk.header.compressed_size,
            available: chunk.data.len() as u64,
        });
    }

    if let Some(indexes) = supplied_indexes {
        let mut seen_channels = BTreeSet::new();
        for index in indexes {
            if !seen_channels.insert(index.channel_id) {
                return Err(McapError::BadIndex);
            }
        }
    }

    Ok(())
}

fn update_info_from_chunk<W: Write + Seek>(
    writer: &mut Writer<W>,
    state: &mut RecoveryState,
    chunk: &RawChunk,
    supplied_indexes: Option<&[MessageIndex]>,
) -> McapResult<Vec<MessageIndex>> {
    let needs_chunk_scan = match supplied_indexes {
        Some(indexes) => {
            indexes.iter().all(|index| index.records.is_empty())
                || indexes.iter().any(|index| {
                    !index.records.is_empty() && !state.channel_map.contains_key(&index.channel_id)
                })
        }
        None => true,
    };

    let rebuilt_indexes = if needs_chunk_scan {
        let scan = scan_chunk_records(chunk)?;
        apply_chunk_definitions(writer, state, &scan.definitions)?;
        Some(scan.indexes)
    } else {
        None
    };

    // If indexes were supplied, the scan above is only needed for schema/channel side effects.
    // The supplied index offsets remain authoritative for the copied raw chunk.
    if let Some(indexes) = supplied_indexes {
        if indexes.iter().any(|index| !index.records.is_empty()) {
            return Ok(indexes.to_vec());
        }
    }

    // All-empty supplied indexes are treated as incomplete and replaced by indexes rebuilt from
    // chunk contents. Truly message-free chunks do not need message indexes.
    Ok(rebuilt_indexes.unwrap_or_default())
}

fn apply_chunk_definitions<W: Write + Seek>(
    writer: &mut Writer<W>,
    state: &mut RecoveryState,
    definitions: &[ChunkDefinition],
) -> McapResult<()> {
    for definition in definitions {
        match definition {
            ChunkDefinition::Schema {
                id,
                name,
                encoding,
                data,
            } => register_schema(
                writer,
                state,
                *id,
                name,
                encoding,
                data.as_slice(),
                RegistrationMode::SummaryOnly,
            )?,
            ChunkDefinition::Channel(channel) => {
                register_channel(
                    writer,
                    state,
                    channel.clone(),
                    RegistrationMode::SummaryOnly,
                )?;
            }
        }
    }
    Ok(())
}

fn scan_chunk_records(chunk: &RawChunk) -> McapResult<ChunkScan> {
    let mut reader = LinearReader::for_chunk_without_crc_validation(chunk.header.clone())?;
    let mut remaining = chunk.data.as_slice();
    let mut rebuilt_indexes: BTreeMap<u16, Vec<MessageIndexEntry>> = BTreeMap::new();
    let mut definitions = Vec::new();
    let mut uncompressed_offset = 0u64;

    while let Some(event) = reader.next_event() {
        match event {
            Ok(LinearReadEvent::ReadRequest(need)) => {
                let read = need.min(remaining.len());
                let dst = reader.insert(read);
                dst.copy_from_slice(&remaining[..read]);
                reader.notify_read(read);
                remaining = &remaining[read..];
            }
            Ok(LinearReadEvent::Record { opcode, data }) => {
                let record_offset = uncompressed_offset;
                uncompressed_offset += OPCODE_LEN_SIZE as u64 + data.len() as u64;
                let record = match crate::parse_record(opcode, data) {
                    Ok(record) => record,
                    Err(err) => {
                        eprintln!(
                            "Warning: failed to parse chunk record opcode 0x{opcode:02x}: {err:#}; skipping"
                        );
                        continue;
                    }
                };
                match record {
                    Record::Schema { header, data } => {
                        definitions.push(ChunkDefinition::Schema {
                            id: header.id,
                            name: header.name,
                            encoding: header.encoding,
                            data: data.into_owned(),
                        });
                    }
                    Record::Channel(channel) => {
                        definitions.push(ChunkDefinition::Channel(channel));
                    }
                    Record::Message { header, .. } => {
                        rebuilt_indexes.entry(header.channel_id).or_default().push(
                            MessageIndexEntry {
                                log_time: header.log_time,
                                offset: record_offset,
                            },
                        );
                    }
                    _ => {}
                }
            }
            Err(err) => return Err(err),
        }
    }

    Ok(ChunkScan {
        definitions,
        indexes: rebuilt_indexes
            .into_iter()
            .map(|(channel_id, records)| MessageIndex {
                channel_id,
                records,
            })
            .collect(),
    })
}

fn recover_record<W: Write + Seek>(
    writer: &mut Writer<W>,
    state: &mut RecoveryState,
    stats: &mut RecoverStats,
    record: Record<'_>,
    registration_mode: RegistrationMode,
) -> McapResult<()> {
    match record {
        Record::Schema { header, data } => register_schema(
            writer,
            state,
            header.id,
            &header.name,
            &header.encoding,
            data.as_ref(),
            registration_mode,
        )?,
        Record::Channel(channel) => {
            register_channel(writer, state, channel, registration_mode)?;
        }
        Record::Message { header, data } => {
            let Some(&channel_id) = state.channel_map.get(&header.channel_id) else {
                if state.warned_missing_channels.insert(header.channel_id) {
                    if let Some(pending_channel) = state.pending_channels.get(&header.channel_id) {
                        eprintln!(
                            "Warning: skipping messages for channel id {} (schema id {} not found)",
                            header.channel_id, pending_channel.schema_id
                        );
                    } else {
                        eprintln!(
                            "Warning: skipping messages for unknown channel id {}",
                            header.channel_id
                        );
                    }
                }
                return Ok(());
            };
            let recovered_header = records::MessageHeader {
                channel_id,
                sequence: header.sequence,
                log_time: header.log_time,
                publish_time: header.publish_time,
            };
            writer.write_to_known_channel(&recovered_header, data.as_ref())?;
            stats.messages += 1;
        }
        Record::Attachment { header, data, .. } => {
            writer.attach(&crate::Attachment {
                log_time: header.log_time,
                create_time: header.create_time,
                name: header.name,
                media_type: header.media_type,
                data,
            })?;
            stats.attachments += 1;
        }
        Record::Metadata(metadata) => {
            writer.write_metadata(&metadata)?;
            stats.metadata += 1;
        }
        _ => {}
    }

    Ok(())
}

fn register_schema<W: Write + Seek>(
    writer: &mut Writer<W>,
    state: &mut RecoveryState,
    input_schema_id: u16,
    name: &str,
    encoding: &str,
    data: &[u8],
    registration_mode: RegistrationMode,
) -> McapResult<()> {
    let schema = SchemaDef {
        name: name.to_string(),
        encoding: encoding.to_string(),
        data: data.to_vec(),
    };
    if let Some(existing) = state.seen_schemas.get(&input_schema_id) {
        if existing != &schema {
            eprintln!(
                "Warning: conflicting schema definition for id {input_schema_id}; keeping first"
            );
        }
        return Ok(());
    }

    let output_schema_id = match registration_mode {
        RegistrationMode::WriteRecords => {
            writer.add_schema_with_id(input_schema_id, name, encoding, data)?
        }
        RegistrationMode::SummaryOnly => {
            writer.register_schema_with_id(input_schema_id, name, encoding, data)?
        }
    };
    state.schema_map.insert(input_schema_id, output_schema_id);
    state.seen_schemas.insert(input_schema_id, schema);
    resolve_pending_channels(writer, state, input_schema_id, registration_mode)?;
    Ok(())
}

fn register_channel<W: Write + Seek>(
    writer: &mut Writer<W>,
    state: &mut RecoveryState,
    channel: records::Channel,
    registration_mode: RegistrationMode,
) -> McapResult<()> {
    if let Some(existing) = state.seen_channels.get(&channel.id) {
        if existing != &channel {
            eprintln!(
                "Warning: conflicting channel definition for id {}; keeping first",
                channel.id
            );
        }
        return Ok(());
    }

    if channel.schema_id != 0 && !state.schema_map.contains_key(&channel.schema_id) {
        state.pending_channels.insert(channel.id, channel);
        return Ok(());
    }

    write_channel_mapping(writer, state, channel, registration_mode)
}

fn resolve_pending_channels<W: Write + Seek>(
    writer: &mut Writer<W>,
    state: &mut RecoveryState,
    schema_id: u16,
    registration_mode: RegistrationMode,
) -> McapResult<()> {
    let to_resolve: Vec<u16> = state
        .pending_channels
        .iter()
        .filter_map(|(channel_id, channel)| {
            if channel.schema_id == schema_id {
                Some(*channel_id)
            } else {
                None
            }
        })
        .collect();

    for channel_id in to_resolve {
        let Some(channel) = state.pending_channels.remove(&channel_id) else {
            continue;
        };
        write_channel_mapping(writer, state, channel, registration_mode)?;
    }

    Ok(())
}

fn write_channel_mapping<W: Write + Seek>(
    writer: &mut Writer<W>,
    state: &mut RecoveryState,
    channel: records::Channel,
    registration_mode: RegistrationMode,
) -> McapResult<()> {
    let output_schema_id = if channel.schema_id == 0 {
        0
    } else {
        match state.schema_map.get(&channel.schema_id) {
            Some(schema_id) => *schema_id,
            None => return Ok(()),
        }
    };
    let output_channel_id = match registration_mode {
        RegistrationMode::WriteRecords => writer.add_channel_with_id(
            channel.id,
            output_schema_id,
            &channel.topic,
            &channel.message_encoding,
            &channel.metadata,
        )?,
        RegistrationMode::SummaryOnly => writer.register_channel_with_id(
            channel.id,
            output_schema_id,
            &channel.topic,
            &channel.message_encoding,
            &channel.metadata,
        )?,
    };

    state.channel_map.insert(channel.id, output_channel_id);
    state.seen_channels.insert(channel.id, channel);
    Ok(())
}
