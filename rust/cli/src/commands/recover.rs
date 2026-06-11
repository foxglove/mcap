use std::io::{IsTerminal as _, Read, Seek, Write};

use anyhow::{bail, Context, Result};
use log::{info, warn};

use mcap::records::Record;
use mcap::sans_io::{LinearReadEvent, LinearReader as SansIoReader, LinearReaderOptions};
use mcap::{Compression, WriteOptions};

use crate::cli::RecoverCommand;
use crate::commands::CommandOutcome;
use crate::context::CommandContext;
use crate::source;

// 1 GiB upper limit on top-level record lengths while scanning the stream. This only bounds
// records read by the linear reader (including a compressed chunk record's own length); it does
// not bound chunk decompression, so a chunk's decoded `uncompressed_size` can still exceed this.
const RECOVER_RECORD_LENGTH_LIMIT: usize = 1024 * 1024 * 1024;

#[derive(Debug, Clone, Copy)]
enum CompressionSelection {
    /// Match the input: reuse the first chunk's compression, or stay uncompressed if a
    /// message/attachment/other record is reached before any chunk.
    Preserve,
    Explicit(Option<Compression>),
}

// Recovery status model:
// - info!: non-lossy recovery decisions or metadata fallback. These may explain output differences
//   (for example, a corrupt leading header means the output gets a default profile) but do not
//   imply message/attachment/metadata loss by themselves.
// - warn!: corrupt or malformed input records that are skipped, messages dropped because their
//   channel could not be recovered, or an early stop caused by truncation/corrupt chunk payloads.
//   Every warning corresponds to data loss and therefore to exit code 3.
// - Err: output/write/setup failures, invalid CLI options, or inputs too broken to recover at all.
//   These are handled by `main` as hard failures and exit 1.
//
// Human-readable summaries, warnings, and errors go to stderr. Stdout is reserved exclusively for
// MCAP bytes when the user writes recovered output to stdout.

/// Statistics for one MCAP record kind.
///
/// `discarded` records are the concrete source for warning-level reporting and lossy exit status.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct RecordRecoveryStats {
    recovered: u64,
    discarded: u64,
}

/// Statistics describing what `recover` salvaged and what it had to discard.
///
/// Discarded counts and `truncated` cover real input data loss. Rebuilt indexes/CRCs and missing
/// records are not counted as loss.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct RecoverStats {
    headers: RecordRecoveryStats,
    schemas: RecordRecoveryStats,
    channels: RecordRecoveryStats,
    chunks: RecordRecoveryStats,
    messages: RecordRecoveryStats,
    attachments: RecordRecoveryStats,
    metadata: RecordRecoveryStats,
    other_records: RecordRecoveryStats,
    /// Recovery stopped before a clean end (truncated mid-record, or a mid-stream decode error
    /// halted the scan), so trailing data was lost.
    truncated: bool,
}

impl RecoverStats {
    /// True if any real data was lost (a complete record/message was discarded, or the scan
    /// stopped before a clean end so a partial trailing message was dropped).
    fn is_lossy(&self) -> bool {
        self.truncated
            || self.headers.discarded > 0
            || self.schemas.discarded > 0
            || self.channels.discarded > 0
            || self.chunks.discarded > 0
            || self.messages.discarded > 0
            || self.attachments.discarded > 0
            || self.metadata.discarded > 0
            || self.other_records.discarded > 0
    }

    fn discarded_counts(&self) -> Vec<(u64, &'static str)> {
        [
            (self.headers.discarded, "header"),
            (self.schemas.discarded, "schema"),
            (self.channels.discarded, "channel"),
            (self.chunks.discarded, "chunk"),
            (self.messages.discarded, "message"),
            (self.attachments.discarded, "attachment"),
            (self.metadata.discarded, "metadata record"),
            (self.other_records.discarded, "other record"),
        ]
        .into_iter()
        .filter(|(count, _)| *count > 0)
        .collect()
    }

    fn record_kind_mut(&mut self, opcode: u8) -> &mut RecordRecoveryStats {
        match opcode {
            mcap::records::op::HEADER => &mut self.headers,
            mcap::records::op::SCHEMA => &mut self.schemas,
            mcap::records::op::CHANNEL => &mut self.channels,
            mcap::records::op::CHUNK => &mut self.chunks,
            mcap::records::op::MESSAGE => &mut self.messages,
            mcap::records::op::ATTACHMENT => &mut self.attachments,
            mcap::records::op::METADATA => &mut self.metadata,
            _ => &mut self.other_records,
        }
    }
}

/// Recovers as much data as possible from a (possibly damaged) MCAP, writing a valid output.
///
/// Exit codes (mapped centrally by `CommandOutcome` in `main`):
/// - 0: successful recovery, no data loss (`CommandOutcome::Success`)
/// - 1: hard failure (`Err`, handled by `main`)
/// - 2: command-line parsing error (owned by clap)
/// - 3: successful recovery with warning-level data loss (`CommandOutcome::Warnings`)
pub fn run(ctx: &CommandContext, args: RecoverCommand) -> Result<CommandOutcome> {
    let source_options = source::SourceOptions::new(ctx.allow_remote_scan());
    let input = source::open_streaming_input(args.file.as_deref(), source_options)?;
    let compression = resolve_compression(&args.compression)?;

    let stats = if let Some(output) = &args.output {
        let file = std::fs::File::create(output)
            .with_context(|| format!("failed to open '{}' for writing", output.display()))?;
        let (stats, file) = recover_to_sink(input, file, compression, args.chunk_size, false)?;
        file.sync_all()
            .context("failed to flush output file contents")?;
        stats
    } else {
        if std::io::stdout().is_terminal() {
            bail!("{}", source::PLEASE_REDIRECT);
        }
        let stdout = std::io::stdout();
        let writer = mcap::write::NoSeek::new(stdout.lock());
        let (stats, _) = recover_to_sink(input, writer, compression, args.chunk_size, true)?;
        stats
    };

    eprintln!(
        "Recovered {}, {}, and {}.",
        count(stats.messages.recovered, "message"),
        count(stats.attachments.recovered, "attachment"),
        count(stats.metadata.recovered, "metadata record"),
    );

    // A clean recovery exits 0; warning-level data loss reports `CommandOutcome::Warnings` (exit 3)
    // so `main` sets the exit code only after every output sink has been flushed and dropped. This
    // diverges from the Go CLI, which always exits 0 once recovery starts.
    if stats.is_lossy() {
        let discarded: Vec<_> = stats
            .discarded_counts()
            .into_iter()
            .map(|(n, noun)| count(n, noun))
            .collect();
        let mut parts = Vec::new();
        if !discarded.is_empty() {
            parts.push(format!("discarded {}", discarded.join(" and ")));
        }
        if stats.truncated {
            parts.push("stopped early (input truncated), so trailing data may be lost".to_string());
        }
        eprintln!("Recovery was lossy: {}.", parts.join("; "));
        return Ok(CommandOutcome::Warnings);
    }
    Ok(CommandOutcome::Success)
}

/// Formats a count with a naive plural (`1 message`, `0 messages`); nouns pluralize with a
/// trailing `s` (`metadata record` -> `metadata records`).
fn count(n: u64, noun: &str) -> String {
    if n == 1 {
        format!("{n} {noun}")
    } else {
        format!("{n} {noun}s")
    }
}

/// Resolves the requested output compression. `preserve` decides the codec from the input stream
/// (see [`CompressionSelection::Preserve`]); the explicit codecs map straight through.
fn resolve_compression(spec: &str) -> Result<CompressionSelection> {
    match spec {
        "preserve" => Ok(CompressionSelection::Preserve),
        "none" | "" => Ok(CompressionSelection::Explicit(None)),
        "zstd" => Ok(CompressionSelection::Explicit(Some(Compression::Zstd))),
        "lz4" => Ok(CompressionSelection::Explicit(Some(Compression::Lz4))),
        other => bail!(
            "unrecognized compression '{other}': valid options are 'preserve', 'none', 'zstd', or 'lz4'"
        ),
    }
}

fn compression_from_str(name: &str) -> Option<Compression> {
    match name {
        "zstd" => Some(Compression::Zstd),
        "lz4" => Some(Compression::Lz4),
        // Empty string means uncompressed; an unrecognized codec can't be preserved, so fall back
        // to uncompressed output for the re-encoded data.
        _ => None,
    }
}

fn recover_to_sink<R: Read, W: Write + Seek>(
    input: R,
    sink: W,
    compression: CompressionSelection,
    chunk_size: u64,
    disable_seeking: bool,
) -> Result<(RecoverStats, W)> {
    let (stats, mut writer) =
        recover_records(input, sink, compression, chunk_size, disable_seeking)?;
    writer.finish().context("failed to finish mcap writer")?;
    Ok((stats, writer.into_inner()))
}

fn build_writer<W: Write + Seek>(
    sink: W,
    header: Option<mcap::records::Header>,
    compression: Option<Compression>,
    chunk_size: u64,
    disable_seeking: bool,
) -> Result<mcap::Writer<W>> {
    let mut write_options = WriteOptions::new()
        .chunk_size(Some(chunk_size))
        .compression(compression)
        .disable_seeking(disable_seeking);

    write_options = write_options.library(crate::cli::LIBRARY_IDENTIFIER.clone());
    if let Some(header) = header {
        write_options = write_options.profile(header.profile);
    }

    write_options
        .create(sink)
        .context("failed to create mcap writer")
}

fn compression_for_writer(
    selection: CompressionSelection,
    first_chunk_compression: Option<&str>,
) -> Option<Compression> {
    match selection {
        CompressionSelection::Explicit(compression) => compression,
        CompressionSelection::Preserve => first_chunk_compression.and_then(compression_from_str),
    }
}

fn ensure_writer<'a, W: Write + Seek>(
    writer: &'a mut Option<mcap::Writer<W>>,
    sink: &mut Option<W>,
    header: &Option<mcap::records::Header>,
    compression: CompressionSelection,
    first_chunk_compression: Option<&str>,
    chunk_size: u64,
    disable_seeking: bool,
) -> Result<&'a mut mcap::Writer<W>> {
    if writer.is_none() {
        let sink = sink
            .take()
            .expect("sink should be available until writer is initialized");
        *writer = Some(build_writer(
            sink,
            header.clone(),
            compression_for_writer(compression, first_chunk_compression),
            chunk_size,
            disable_seeking,
        )?);
    }
    Ok(writer.as_mut().expect("writer should be initialized"))
}

/// Streams every record from a (possibly damaged) MCAP, decoding chunks, and re-writes the records
/// through the writer. The writer rebuilds chunks, indexes, the summary section, and CRCs, so the
/// output is always a valid MCAP.
fn recover_records<R: Read, W: Write + Seek>(
    mut input: R,
    sink: W,
    compression: CompressionSelection,
    chunk_size: u64,
    disable_seeking: bool,
) -> Result<(RecoverStats, mcap::Writer<W>)> {
    let mut reader = SansIoReader::new_with_options(
        LinearReaderOptions::default()
            .with_skip_end_magic(true)
            .with_emit_chunks(true)
            // Recover decodes chunk payloads even when the stored chunk CRC is wrong.
            .with_validate_chunk_crcs(false)
            .with_record_length_limit(RECOVER_RECORD_LENGTH_LIMIT),
    );

    let mut sink = Some(sink);
    let mut writer = None;
    let mut header = None;
    let mut pending_records: Vec<Record<'static>> = Vec::new();
    let mut stats = RecoverStats::default();
    let mut saw_any_record = false;
    // Channels successfully registered with the writer; messages for other channels are dropped.
    let mut known_channels = std::collections::BTreeSet::new();

    while let Some(event) = reader.next_event() {
        match event {
            Ok(LinearReadEvent::ReadRequest(need)) => {
                let read = {
                    let dst = reader.insert(need);
                    match input.read(dst) {
                        Ok(read) => read,
                        Err(err) if saw_any_record => {
                            warn!("{err:#} -- stopping recovery scan");
                            stats.truncated = true;
                            break;
                        }
                        Err(err) => return Err(err).context("failed to read input"),
                    }
                };
                if read == 0 && !saw_any_record {
                    return Err(mcap::McapError::UnexpectedEof.into());
                }
                reader.notify_read(read);
            }
            Ok(LinearReadEvent::Record { opcode, data }) => {
                saw_any_record = true;
                let record = match mcap::parse_record(opcode, data) {
                    Ok(record) => record,
                    Err(err) => {
                        warn!("failed to parse record opcode 0x{opcode:02x}: {err:#}; skipping");
                        stats.record_kind_mut(opcode).discarded += 1;
                        continue;
                    }
                };

                match record {
                    Record::Header(parsed_header) => {
                        if writer.is_none() {
                            header = Some(parsed_header);
                        }
                        stats.headers.recovered += 1;
                    }
                    Record::Chunk {
                        header: chunk_header,
                        data,
                    } => {
                        let writer = ensure_writer(
                            &mut writer,
                            &mut sink,
                            &header,
                            compression,
                            Some(&chunk_header.compression),
                            chunk_size,
                            disable_seeking,
                        )?;
                        if !flush_pending_records(
                            writer,
                            &mut known_channels,
                            &mut stats,
                            &mut pending_records,
                        )? {
                            break;
                        }
                        if !recover_chunk_records(
                            writer,
                            &mut known_channels,
                            &mut stats,
                            chunk_header,
                            data.as_ref(),
                        )? {
                            break;
                        }
                    }
                    Record::DataEnd(_) | Record::Footer(_) => {
                        let writer = ensure_writer(
                            &mut writer,
                            &mut sink,
                            &header,
                            compression,
                            None,
                            chunk_size,
                            disable_seeking,
                        )?;
                        let _ = flush_pending_records(
                            writer,
                            &mut known_channels,
                            &mut stats,
                            &mut pending_records,
                        )?;
                        break;
                    }
                    record => {
                        if should_buffer_until_compression_known(compression, &writer, &record) {
                            pending_records.push(record.into_owned());
                            continue;
                        }
                        // For `preserve` without a pre-detected source compression, loose
                        // schemas/channels/metadata are buffered above until a chunk can choose the
                        // codec. Reaching this path before a writer exists means either compression
                        // is already known, or a loose record that carries no codec signal (a
                        // message, attachment, or other record) has proven we should start with
                        // uncompressed output.
                        let writer = ensure_writer(
                            &mut writer,
                            &mut sink,
                            &header,
                            compression,
                            None,
                            chunk_size,
                            disable_seeking,
                        )?;
                        if !flush_pending_records(
                            writer,
                            &mut known_channels,
                            &mut stats,
                            &mut pending_records,
                        )? {
                            break;
                        }
                        if !recover_record(writer, &mut known_channels, &mut stats, record)? {
                            break;
                        }
                    }
                }
            }
            Err(err) => {
                if !saw_any_record {
                    return Err(err).context("failed to read any records from input");
                }
                // Warning-level data loss: the sans-io reader has no resync primitive after a
                // stream-level decode failure (truncation, corrupt top-level framing), so stop and
                // keep what we have.
                warn!("{err:#} -- stopping recovery scan");
                stats.truncated = true;
                break;
            }
        }
    }

    let writer = match writer {
        Some(writer) => writer,
        None => build_writer(
            sink.expect("sink should be available if writer was never initialized"),
            header,
            compression_for_writer(compression, None),
            chunk_size,
            disable_seeking,
        )?,
    };
    let mut writer = writer;
    let _ = flush_pending_records(
        &mut writer,
        &mut known_channels,
        &mut stats,
        &mut pending_records,
    )?;

    Ok((stats, writer))
}

/// Under `preserve`, the output codec is only knowable once we reach the first chunk, so we defer
/// creating the writer by buffering the small, structural records that the spec allows ahead of the
/// first chunk (`[Header][Schema/Channel/Metadata...][Chunk]`).
///
/// Attachments are deliberately *not* buffered: per the spec they never appear inside a chunk
/// (op=0x09 "Attachment records must not appear within a chunk"), so a loose attachment carries no
/// signal about the following chunks' compression, yet it can be arbitrarily large. Buffering it
/// would risk holding the whole attachment in memory to learn nothing. A message/attachment/other
/// record reached before any chunk instead commits the output to uncompressed; an attachment is
/// byte-identical regardless of file compression, so preservation only differs for the (rare)
/// chunks after it.
fn should_buffer_until_compression_known<W: Write + Seek>(
    compression: CompressionSelection,
    writer: &Option<mcap::Writer<W>>,
    record: &Record<'_>,
) -> bool {
    matches!(compression, CompressionSelection::Preserve)
        && writer.is_none()
        && matches!(
            record,
            Record::Schema { .. } | Record::Channel(_) | Record::Metadata(_)
        )
}

fn flush_pending_records<W: Write + Seek>(
    writer: &mut mcap::Writer<W>,
    known_channels: &mut std::collections::BTreeSet<u16>,
    stats: &mut RecoverStats,
    pending_records: &mut Vec<Record<'static>>,
) -> Result<bool> {
    for record in std::mem::take(pending_records) {
        if !recover_record(writer, known_channels, stats, record)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn recover_chunk_records<W: Write + Seek>(
    writer: &mut mcap::Writer<W>,
    known_channels: &mut std::collections::BTreeSet<u16>,
    stats: &mut RecoverStats,
    chunk_header: mcap::records::ChunkHeader,
    chunk_data: &[u8],
) -> Result<bool> {
    let mut chunk_reader = match mcap::read::ChunkReader::new(chunk_header, chunk_data) {
        Ok(reader) => reader,
        Err(err) => {
            warn!("failed to decode chunk: {err:#}; stopping recovery scan");
            stats.chunks.discarded += 1;
            stats.truncated = true;
            return Ok(false);
        }
    };

    loop {
        match chunk_reader.next() {
            Some(Ok(record)) => {
                if !recover_record(writer, known_channels, stats, record)? {
                    return Ok(false);
                }
            }
            // Terminal value for a chunk whose stored CRC doesn't match its (decodable) bytes.
            // `ChunkReader` post-validates, yielding every record before reporting this, so all
            // records were already recovered -- not lossy. The bad input CRC is ignored: the writer
            // recomputes a correct CRC on re-encode. (A payload that fails to decode instead raises
            // a different mid-stream error, handled as loss below.)
            Some(Err(mcap::McapError::BadChunkCrc { saved, calculated })) => {
                info!(
                    "chunk CRC mismatch (expected {saved:08X}, got {calculated:08X}); records were decoded and CRC will be recomputed"
                );
                stats.chunks.recovered += 1;
                return Ok(true);
            }
            Some(Err(err)) => {
                warn!("{err:#} -- stopping recovery scan");
                stats.chunks.discarded += 1;
                stats.truncated = true;
                return Ok(false);
            }
            None => {
                stats.chunks.recovered += 1;
                return Ok(true);
            }
        }
    }
}

fn recover_record<W: Write + Seek>(
    writer: &mut mcap::Writer<W>,
    known_channels: &mut std::collections::BTreeSet<u16>,
    stats: &mut RecoverStats,
    record: Record<'_>,
) -> Result<bool> {
    match record {
        Record::Header(_) => {
            stats.headers.recovered += 1;
        }
        Record::Schema { header, data } => {
            if let Err(err) =
                writer.add_schema_with_id(header.id, &header.name, &header.encoding, data.as_ref())
            {
                warn!(
                    "skipping schema id {} ({}): {err:#}",
                    header.id, header.name
                );
                stats.schemas.discarded += 1;
            } else {
                stats.schemas.recovered += 1;
            }
        }
        Record::Channel(channel) => {
            match writer.add_channel_with_id(
                channel.id,
                channel.schema_id,
                &channel.topic,
                &channel.message_encoding,
                &channel.metadata,
            ) {
                Ok(_) => {
                    known_channels.insert(channel.id);
                    stats.channels.recovered += 1;
                }
                Err(err) => {
                    warn!(
                        "skipping channel id {} ({}): {err:#}",
                        channel.id, channel.topic
                    );
                    stats.channels.discarded += 1;
                }
            }
        }
        Record::Message { header, data } => {
            if !known_channels.contains(&header.channel_id) {
                warn!(
                    "skipping message for unknown channel id {}",
                    header.channel_id
                );
                stats.messages.discarded += 1;
                return Ok(true);
            }
            writer
                .write_to_known_channel(&header, data.as_ref())
                .context("failed to write recovered message")?;
            stats.messages.recovered += 1;
        }
        Record::Attachment { header, data, .. } => {
            writer
                .attach(&mcap::Attachment {
                    log_time: header.log_time,
                    create_time: header.create_time,
                    name: header.name,
                    media_type: header.media_type,
                    data,
                })
                .context("failed to write recovered attachment")?;
            stats.attachments.recovered += 1;
        }
        Record::Metadata(metadata) => {
            writer
                .write_metadata(&metadata)
                .context("failed to write recovered metadata")?;
            stats.metadata.recovered += 1;
        }
        // The data section is over; the summary section (if any) is not recovered.
        Record::DataEnd(_) | Record::Footer(_) => return Ok(false),
        // Indexes/statistics are regenerated.
        _ => {}
    }

    Ok(true)
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::collections::BTreeMap;
    use std::io::Cursor;

    use mcap::records::{op, MessageHeader, Record};

    use super::{recover_to_sink, RecoverStats};

    const OPCODE_LEN_SIZE: usize = 1 + 8;

    fn write_test_input(compression: Option<mcap::Compression>) -> Vec<u8> {
        let mut output = Cursor::new(Vec::new());
        {
            let mut writer = mcap::WriteOptions::new()
                .chunk_size(Some(1024 * 1024))
                .compression(compression)
                .library("test-recorder/0.0")
                .create(&mut output)
                .expect("writer");
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
                for (channel_id, byte) in [(camera_a, b'a'), (camera_b, b'b'), (radar, b'c')] {
                    writer
                        .write_to_known_channel(
                            &MessageHeader {
                                channel_id,
                                sequence: i,
                                log_time: i as u64,
                                publish_time: i as u64,
                            },
                            &[byte],
                        )
                        .expect("write");
                }
            }
            writer
                .attach(&mcap::Attachment {
                    log_time: 50,
                    create_time: 50,
                    name: "attachment".to_string(),
                    media_type: "application/octet-stream".to_string(),
                    data: Cow::Borrowed(&[1, 2, 3]),
                })
                .expect("attachment");
            writer
                .write_metadata(&mcap::records::Metadata {
                    name: "metadata".to_string(),
                    metadata: BTreeMap::from([("k".to_string(), "v".to_string())]),
                })
                .expect("metadata");
            writer.finish().expect("finish");
        }
        output.into_inner()
    }

    fn corrupt_leading_header_body(input: &[u8]) -> Vec<u8> {
        let header_offset = mcap::MAGIC.len();
        assert_eq!(input[header_offset], op::HEADER);
        let header_len = u64::from_le_bytes(
            input[header_offset + 1..header_offset + OPCODE_LEN_SIZE]
                .try_into()
                .expect("header length bytes"),
        ) as usize;
        let records_after_header = header_offset + OPCODE_LEN_SIZE + header_len;

        let mut corrupted = Vec::new();
        corrupted.extend_from_slice(mcap::MAGIC);
        corrupted.push(op::HEADER);
        corrupted.extend_from_slice(&1u64.to_le_bytes());
        corrupted.push(0);
        corrupted.extend_from_slice(&input[records_after_header..]);
        corrupted
    }

    fn input_with_huge_record_after_header() -> Vec<u8> {
        let input = write_test_input(None);
        let header_offset = mcap::MAGIC.len();
        assert_eq!(input[header_offset], op::HEADER);
        let header_len = u64::from_le_bytes(
            input[header_offset + 1..header_offset + OPCODE_LEN_SIZE]
                .try_into()
                .expect("header length bytes"),
        ) as usize;
        let records_after_header = header_offset + OPCODE_LEN_SIZE + header_len;

        let mut corrupted = input[..records_after_header].to_vec();
        corrupted.push(op::MESSAGE);
        corrupted.extend_from_slice(&u64::MAX.to_le_bytes());
        corrupted
    }

    fn bytes_before_data_end(bytes: &[u8]) -> &[u8] {
        let mut offset = mcap::MAGIC.len();
        let limit = bytes.len().saturating_sub(mcap::MAGIC.len());
        while offset + OPCODE_LEN_SIZE <= limit {
            let opcode = bytes[offset];
            let length = u64::from_le_bytes(
                bytes[offset + 1..offset + OPCODE_LEN_SIZE]
                    .try_into()
                    .expect("record length bytes"),
            ) as usize;
            if opcode == op::DATA_END {
                return &bytes[..offset];
            }
            offset += OPCODE_LEN_SIZE + length;
        }
        panic!("no DataEnd found");
    }

    fn append_chunks_from(source: &[u8], target: &mut Vec<u8>) {
        let mut offset = mcap::MAGIC.len();
        let limit = source.len().saturating_sub(mcap::MAGIC.len());
        while offset + OPCODE_LEN_SIZE <= limit {
            let opcode = source[offset];
            let length = u64::from_le_bytes(
                source[offset + 1..offset + OPCODE_LEN_SIZE]
                    .try_into()
                    .expect("record length bytes"),
            ) as usize;
            let end = offset + OPCODE_LEN_SIZE + length;
            if opcode == op::CHUNK {
                target.extend_from_slice(&source[offset..end]);
            }
            offset = end;
        }
    }

    fn write_metadata_then_zstd_chunk_input() -> Vec<u8> {
        let mut metadata_only = Cursor::new(Vec::new());
        {
            let mut writer = mcap::WriteOptions::new()
                .use_chunks(false)
                .create(&mut metadata_only)
                .expect("writer");
            writer
                .write_metadata(&mcap::records::Metadata {
                    name: "before-chunk".to_string(),
                    metadata: BTreeMap::from([("source".to_string(), "metadata".to_string())]),
                })
                .expect("metadata");
            writer.finish().expect("finish");
        }

        let zstd_chunked = write_test_input(Some(mcap::Compression::Zstd));
        let mut mixed = bytes_before_data_end(metadata_only.get_ref()).to_vec();
        append_chunks_from(&zstd_chunked, &mut mixed);
        mixed
    }

    fn write_attachment_then_zstd_chunk_input(attachment_size: usize) -> Vec<u8> {
        let mut attachment_only = Cursor::new(Vec::new());
        {
            let mut writer = mcap::WriteOptions::new()
                .use_chunks(false)
                .create(&mut attachment_only)
                .expect("writer");
            writer
                .attach(&mcap::Attachment {
                    log_time: 1,
                    create_time: 1,
                    name: "before-chunk".to_string(),
                    media_type: "application/octet-stream".to_string(),
                    data: Cow::Owned(vec![0xAB; attachment_size]),
                })
                .expect("attachment");
            writer.finish().expect("finish");
        }

        let zstd_chunked = write_test_input(Some(mcap::Compression::Zstd));
        let mut mixed = bytes_before_data_end(attachment_only.get_ref()).to_vec();
        append_chunks_from(&zstd_chunked, &mut mixed);
        mixed
    }

    fn recover_to_vec(input: &[u8], compression: &str) -> (Vec<u8>, RecoverStats) {
        let target = super::resolve_compression(compression).expect("compression");
        recover_to_vec_with_selection(input, target)
    }

    fn recover_to_vec_with_selection(
        input: &[u8],
        compression: super::CompressionSelection,
    ) -> (Vec<u8>, RecoverStats) {
        let (stats, output) = recover_to_sink(
            input,
            Cursor::new(Vec::new()),
            compression,
            1024 * 1024,
            false,
        )
        .expect("recover should succeed");
        (output.into_inner(), stats)
    }

    fn count_output_records(bytes: &[u8]) -> (usize, usize, usize) {
        let message_count = mcap::MessageStream::new(bytes)
            .expect("message stream")
            .count();
        let mut attachment_count = 0usize;
        let mut metadata_count = 0usize;
        for record in mcap::read::LinearReader::new(bytes)
            .expect("linear reader")
            .map(|record| record.expect("record parse"))
        {
            match record {
                Record::Attachment { .. } => attachment_count += 1,
                Record::Metadata(_) => metadata_count += 1,
                _ => {}
            }
        }
        (message_count, attachment_count, metadata_count)
    }

    fn chunk_compressions(bytes: &[u8]) -> Vec<String> {
        mcap::read::LinearReader::new(bytes)
            .expect("linear reader")
            .filter_map(|record| match record.expect("record parse") {
                Record::Chunk { header, .. } => Some(header.compression),
                _ => None,
            })
            .collect()
    }

    fn first_chunk_crc_offset(input: &[u8]) -> Option<usize> {
        let mut offset = mcap::MAGIC.len();
        let limit = input.len().saturating_sub(mcap::MAGIC.len());
        while offset + OPCODE_LEN_SIZE <= limit {
            let opcode = input[offset];
            let length = u64::from_le_bytes(
                input[offset + 1..offset + OPCODE_LEN_SIZE]
                    .try_into()
                    .expect("record length bytes"),
            ) as usize;
            let end = offset + OPCODE_LEN_SIZE + length;
            if opcode == op::CHUNK {
                // uncompressed_crc sits after message_start_time + message_end_time +
                // uncompressed_size (3 * u64) within the chunk record body.
                let crc_offset = offset + OPCODE_LEN_SIZE + 24;
                return (crc_offset + 4 <= end).then_some(crc_offset);
            }
            offset = end;
        }
        None
    }

    #[test]
    fn recovers_valid_input_with_attachments_and_metadata() {
        let input = write_test_input(Some(mcap::Compression::Zstd));
        let (output, stats) = recover_to_vec(&input, "preserve");
        let (messages, attachments, metadata) = count_output_records(&output);
        assert_eq!(messages, 300);
        assert_eq!(attachments, 1);
        assert_eq!(metadata, 1);
        assert_eq!(stats.headers.recovered, 1);
        assert_eq!(stats.messages.recovered, 300);
        assert_eq!(stats.attachments.recovered, 1);
        assert_eq!(stats.metadata.recovered, 1);
        assert!(!stats.is_lossy());
    }

    #[test]
    fn recover_stamps_cli_writer_library() {
        let input = write_test_input(Some(mcap::Compression::Zstd));
        let (output, _) = recover_to_vec(&input, "preserve");
        // The fixture's `test-recorder/0.0` library is overwritten with the CLI's own identity.
        let library = crate::parse::read_header(&output)
            .expect("read header")
            .expect("header present")
            .library;
        assert_eq!(library, *crate::cli::LIBRARY_IDENTIFIER);
    }

    #[test]
    fn recovers_data_after_corrupt_leading_header() {
        let input = corrupt_leading_header_body(&write_test_input(None));
        let (output, stats) = recover_to_vec(&input, "preserve");
        let (messages, attachments, metadata) = count_output_records(&output);
        assert_eq!(messages, 300);
        assert_eq!(attachments, 1);
        assert_eq!(metadata, 1);
        assert_eq!(stats.messages.recovered, 300);
        assert_eq!(stats.headers.discarded, 1);
        assert!(stats.is_lossy());
        // The source header was discarded, but the output is still stamped with the CLI writer
        // identity rather than falling back to the crate default.
        let library = crate::parse::read_header(&output)
            .expect("read header")
            .expect("header present")
            .library;
        assert_eq!(library, *crate::cli::LIBRARY_IDENTIFIER);
    }

    #[test]
    fn huge_record_length_after_header_is_lossy_not_oom() {
        let input = input_with_huge_record_after_header();
        let (output, stats) = recover_to_vec(&input, "preserve");
        let (messages, attachments, metadata) = count_output_records(&output);
        assert_eq!(messages, 0);
        assert_eq!(attachments, 0);
        assert_eq!(metadata, 0);
        assert!(stats.truncated);
        assert!(stats.is_lossy());
    }

    #[test]
    fn preserve_keeps_source_compression() {
        let input = write_test_input(Some(mcap::Compression::Zstd));
        let (output, _) = recover_to_vec(&input, "preserve");
        let compressions = chunk_compressions(&output);
        assert!(!compressions.is_empty());
        assert!(compressions.iter().all(|name| name == "zstd"));
    }

    #[test]
    fn preserve_uses_first_chunk_compression_after_loose_metadata() {
        let input = write_metadata_then_zstd_chunk_input();
        let (output, _) = recover_to_vec(&input, "preserve");
        let compressions = chunk_compressions(&output);
        assert!(!compressions.is_empty());
        assert!(compressions.iter().all(|name| name == "zstd"));
    }

    #[test]
    fn preserve_does_not_buffer_attachment_before_first_chunk() {
        // An attachment is never inside a chunk (spec op=0x09), so it carries no codec signal and
        // must not be buffered in memory. Under `preserve` it commits the output to uncompressed,
        // yet the attachment itself is still recovered intact.
        let input = write_attachment_then_zstd_chunk_input(4 * 1024 * 1024);
        let (output, stats) = recover_to_vec(&input, "preserve");
        let (_, attachments, _) = count_output_records(&output);
        assert_eq!(attachments, 1);
        assert_eq!(stats.attachments.recovered, 1);
        assert!(!stats.is_lossy());
        let compressions = chunk_compressions(&output);
        assert!(!compressions.is_empty());
        assert!(compressions.iter().all(|name| name.is_empty()));
    }

    #[test]
    fn preserve_keeps_uncompressed_source_uncompressed() {
        let input = write_test_input(None);
        let (output, _) = recover_to_vec(&input, "preserve");
        let compressions = chunk_compressions(&output);
        assert!(!compressions.is_empty());
        assert!(compressions.iter().all(|name| name.is_empty()));
    }

    #[test]
    fn rejects_unknown_compression_with_preserve_in_message() {
        let err =
            super::resolve_compression("snappy").expect_err("unknown codec should be rejected");
        let message = err.to_string();
        assert!(message.contains("preserve"), "message was: {message}");
        assert!(message.contains("zstd"), "message was: {message}");
    }

    #[test]
    fn explicit_compression_overrides_source() {
        let input = write_test_input(None);
        let (output, _) = recover_to_vec(&input, "zstd");
        let compressions = chunk_compressions(&output);
        assert!(!compressions.is_empty());
        assert!(compressions.iter().all(|name| name == "zstd"));
    }

    fn write_multi_chunk_input() -> Vec<u8> {
        let mut output = Cursor::new(Vec::new());
        {
            let mut writer = mcap::WriteOptions::new()
                .chunk_size(Some(128))
                .compression(None)
                .create(&mut output)
                .expect("writer");
            let channel = writer
                .add_channel_with_id(2, 0, "multi", "json", &BTreeMap::new())
                .expect("channel");
            for i in 0..100 {
                writer
                    .write_to_known_channel(
                        &MessageHeader {
                            channel_id: channel,
                            sequence: i,
                            log_time: i as u64,
                            publish_time: i as u64,
                        },
                        &[b'x'; 64],
                    )
                    .expect("write");
            }
            writer.finish().expect("finish");
        }
        output.into_inner()
    }

    #[test]
    fn rebuilds_valid_output_from_truncated_input() {
        let mut input = write_multi_chunk_input();
        // Truncate partway through the data section so trailing chunks/messages are lost.
        input.truncate(input.len() / 2);

        let (output, stats) = recover_to_vec(&input, "preserve");
        let (messages, _, _) = count_output_records(&output);
        assert!(messages > 0);
        assert!(messages < 100);
        assert_eq!(stats.messages.recovered as usize, messages);
        assert!(stats.truncated);
        assert!(stats.is_lossy());
    }

    #[test]
    fn recovers_all_records_despite_invalid_chunk_crc() {
        let mut input = write_test_input(Some(mcap::Compression::Zstd));
        let crc_offset = first_chunk_crc_offset(&input).expect("chunk crc offset");
        input[crc_offset] ^= 0xFF;

        // Recovery decodes chunk payloads regardless of the stored CRC, and re-encodes them into a
        // fresh, valid output whose CRCs are recomputed correctly.
        let (output, stats) = recover_to_vec(&input, "preserve");
        let (messages, attachments, metadata) = count_output_records(&output);
        assert_eq!(messages, 300);
        assert_eq!(attachments, 1);
        assert_eq!(metadata, 1);
        assert_eq!(stats.messages.recovered, 300);
        assert!(!stats.is_lossy());
    }
}
