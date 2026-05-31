use std::io::{IsTerminal as _, Seek, Write};

use anyhow::{bail, Context, Result};
use log::warn;

use mcap::records::Record;
use mcap::sans_io::{LinearReadEvent, LinearReader as SansIoReader, LinearReaderOptions};
use mcap::{Compression, WriteOptions};

use crate::cli::RecoverCommand;
use crate::commands::common;
use crate::context::CommandContext;

/// Statistics describing what `recover` salvaged and what it had to discard.
///
/// Discarded counts and `truncated` cover only *real data* loss. Rebuilt indexes, summary
/// sections, and duplicate schema/channel definitions are not counted as loss.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct RecoverStats {
    messages: u64,
    attachments: u64,
    metadata: u64,
    discarded_messages: u64,
    discarded_records: u64,
    /// Recovery stopped before a clean end (truncated mid-record, or a mid-stream decode error
    /// halted the scan), so trailing data was lost.
    truncated: bool,
}

impl RecoverStats {
    /// True if any real data was lost.
    #[allow(dead_code)] // Wired to a non-zero exit code in a future change (see README pre-1.0).
    fn is_lossy(&self) -> bool {
        self.truncated || self.discarded_messages > 0 || self.discarded_records > 0
    }
}

pub fn run(ctx: &CommandContext, args: RecoverCommand) -> Result<()> {
    let input = common::load_input(
        args.file.as_deref(),
        common::SourceOptions::new(ctx.allow_remote_scan()),
    )?;
    let bytes = input.as_slice();
    let compression = resolve_compression(&args.compression, bytes)?;

    let stats = if let Some(output) = &args.output {
        let file = std::fs::File::create(output)
            .with_context(|| format!("failed to open '{}' for writing", output.display()))?;
        let (stats, file) = recover_to_sink(bytes, file, compression, args.chunk_size, false)?;
        file.sync_all()
            .context("failed to flush output file contents")?;
        stats
    } else {
        if std::io::stdout().is_terminal() {
            bail!("{}", common::PLEASE_REDIRECT);
        }
        let stdout = std::io::stdout();
        let writer = mcap::write::NoSeek::new(stdout.lock());
        let (stats, _) = recover_to_sink(bytes, writer, compression, args.chunk_size, true)?;
        stats
    };

    eprintln!(
        "Recovered {} messages, {} attachments, and {} metadata records.",
        stats.messages, stats.attachments, stats.metadata
    );
    Ok(())
}

/// Resolves the requested output compression. `preserve` keeps the input file's compression
/// (detected from its first chunk; uncompressed if the input is unchunked).
fn resolve_compression(spec: &str, input: &[u8]) -> Result<Option<Compression>> {
    match spec {
        "preserve" => Ok(detect_source_compression(input)),
        "none" | "" => Ok(None),
        "zstd" => Ok(Some(Compression::Zstd)),
        "lz4" => Ok(Some(Compression::Lz4)),
        other => bail!(
            "unrecognized compression '{other}': valid options are 'preserve', 'none', 'zstd', or 'lz4'"
        ),
    }
}

fn detect_source_compression(input: &[u8]) -> Option<Compression> {
    // `read::LinearReader` yields chunk records without decompressing them, so this only needs to
    // parse the first chunk header.
    let reader = mcap::read::LinearReader::new_with_options(
        input,
        mcap::read::Options::IgnoreEndMagic.into(),
    )
    .ok()?;
    for record in reader {
        match record {
            Ok(Record::Chunk { header, .. }) => return compression_from_str(&header.compression),
            Ok(_) => continue,
            Err(_) => break,
        }
    }
    None
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

fn recover_to_sink<W: Write + Seek>(
    input: &[u8],
    sink: W,
    compression: Option<Compression>,
    chunk_size: u64,
    disable_seeking: bool,
) -> Result<(RecoverStats, W)> {
    let mut write_options = WriteOptions::new()
        .chunk_size(Some(chunk_size))
        .compression(compression)
        .disable_seeking(disable_seeking);

    if let Some(header) = read_header(input)? {
        write_options = write_options
            .profile(header.profile)
            .library(header.library);
    }

    let mut writer = write_options
        .create(sink)
        .context("failed to create mcap writer")?;
    let stats = recover_records(input, &mut writer)?;
    writer.finish().context("failed to finish mcap writer")?;
    Ok((stats, writer.into_inner()))
}

fn read_header(input: &[u8]) -> Result<Option<mcap::records::Header>> {
    let mut reader = mcap::read::LinearReader::new_with_options(
        input,
        mcap::read::Options::IgnoreEndMagic.into(),
    )?;
    match reader.next() {
        Some(Ok(Record::Header(header))) => Ok(Some(header)),
        Some(Ok(_)) | None => Ok(None),
        Some(Err(err)) => Err(err.into()),
    }
}

/// Reads every record from a (possibly damaged) MCAP, decoding chunks, and re-writes the records
/// through the writer. The writer rebuilds chunks, indexes, the summary section, and CRCs, so the
/// output is always a valid MCAP.
fn recover_records<W: Write + Seek>(
    input: &[u8],
    writer: &mut mcap::Writer<W>,
) -> Result<RecoverStats> {
    let mut reader = SansIoReader::new_with_options(
        LinearReaderOptions::default()
            .with_skip_end_magic(true)
            // Recover decodes chunk payloads even when the stored chunk CRC is wrong.
            .with_validate_chunk_crcs(false)
            .with_record_length_limit(input.len()),
    );

    let mut remaining = input;
    let mut stats = RecoverStats::default();
    let mut saw_any_record = false;
    // Channels successfully registered with the writer; messages for other channels are dropped.
    let mut known_channels = std::collections::BTreeSet::new();

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
                let record = match mcap::parse_record(opcode, data) {
                    Ok(record) => record,
                    Err(err) => {
                        warn!("failed to parse record opcode 0x{opcode:02x}: {err:#}; skipping");
                        stats.discarded_records += 1;
                        continue;
                    }
                };

                match record {
                    Record::Schema { header, data } => {
                        if let Err(err) = writer.add_schema_with_id(
                            header.id,
                            &header.name,
                            &header.encoding,
                            data.as_ref(),
                        ) {
                            warn!(
                                "skipping schema id {} ({}): {err:#}",
                                header.id, header.name
                            );
                            stats.discarded_records += 1;
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
                            }
                            Err(err) => {
                                warn!(
                                    "skipping channel id {} ({}): {err:#}",
                                    channel.id, channel.topic
                                );
                                stats.discarded_records += 1;
                            }
                        }
                    }
                    Record::Message { header, data } => {
                        if !known_channels.contains(&header.channel_id) {
                            warn!(
                                "skipping message for unknown channel id {}",
                                header.channel_id
                            );
                            stats.discarded_messages += 1;
                            continue;
                        }
                        writer
                            .write_to_known_channel(&header, data.as_ref())
                            .context("failed to write recovered message")?;
                        stats.messages += 1;
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
                        stats.attachments += 1;
                    }
                    Record::Metadata(metadata) => {
                        writer
                            .write_metadata(&metadata)
                            .context("failed to write recovered metadata")?;
                        stats.metadata += 1;
                    }
                    // The data section is over; the summary section (if any) is not recovered.
                    Record::DataEnd(_) | Record::Footer(_) => break,
                    // Header is applied at writer construction; indexes/statistics are regenerated.
                    _ => {}
                }
            }
            Err(err) => {
                if !saw_any_record {
                    return Err(err).context("failed to read any records from input");
                }
                // The sans-io reader has no resync primitive after a stream-level decode failure
                // (truncation, corrupt compressed chunk payload), so stop and keep what we have.
                warn!("{err:#} -- stopping recovery scan");
                stats.truncated = true;
                break;
            }
        }
    }

    Ok(stats)
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

    fn recover_to_vec(input: &[u8], compression: &str) -> (Vec<u8>, RecoverStats) {
        let target = super::resolve_compression(compression, input).expect("compression");
        let (stats, output) =
            recover_to_sink(input, Cursor::new(Vec::new()), target, 1024 * 1024, false)
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
        assert_eq!(stats.messages, 300);
        assert_eq!(stats.attachments, 1);
        assert_eq!(stats.metadata, 1);
        assert!(!stats.is_lossy());
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
    fn preserve_keeps_uncompressed_source_uncompressed() {
        let input = write_test_input(None);
        let (output, _) = recover_to_vec(&input, "preserve");
        let compressions = chunk_compressions(&output);
        assert!(!compressions.is_empty());
        assert!(compressions.iter().all(|name| name.is_empty()));
    }

    #[test]
    fn rejects_unknown_compression_with_preserve_in_message() {
        let err = super::resolve_compression("presrve", &[]).expect_err("typo should be rejected");
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
        assert_eq!(stats.messages as usize, messages);
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
        assert_eq!(stats.messages, 300);
        assert!(!stats.is_lossy());
    }
}
