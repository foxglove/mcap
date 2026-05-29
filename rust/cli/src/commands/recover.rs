use std::collections::{BTreeMap, BTreeSet};
use std::io::{IsTerminal as _, Seek, Write};

use anyhow::{bail, Context, Result};
use mcap::records::{self, op, Record};
use mcap::sans_io::{LinearReadEvent, LinearReader, LinearReaderOptions};

use crate::cli::RecoverCommand;
use crate::context::CommandContext;

#[derive(Debug, Clone)]
struct RecoverOptions {
    compression: Option<mcap::Compression>,
    chunk_size: u64,
    always_decode_chunk: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct RecoverStats {
    messages: u64,
    attachments: u64,
    metadata: u64,
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

pub fn run(_ctx: &CommandContext, args: RecoverCommand) -> Result<()> {
    let opts = RecoverOptions {
        compression: crate::commands::common::parse_output_compression(&args.compression)?,
        chunk_size: args.chunk_size,
        always_decode_chunk: args.always_decode_chunk,
    };
    let input = crate::commands::common::load_input(args.file.as_deref())?;

    let stats = if let Some(output) = &args.output {
        let writer = std::fs::File::create(output)
            .with_context(|| format!("failed to open '{}' for writing", output.display()))?;
        let (stats, writer) = recover_to_sink(input.as_slice(), writer, &opts, false)?;
        writer
            .sync_all()
            .context("failed to flush output file contents")?;
        stats
    } else {
        if std::io::stdout().is_terminal() {
            bail!("{}", crate::commands::common::PLEASE_REDIRECT);
        }
        let stdout = std::io::stdout();
        let writer = mcap::write::NoSeek::new(stdout.lock());
        let (stats, _) = recover_to_sink(input.as_slice(), writer, &opts, true)?;
        stats
    };

    eprintln!(
        "Recovered {} messages, {} attachments, and {} metadata records.",
        stats.messages, stats.attachments, stats.metadata
    );
    Ok(())
}
fn recover_to_sink<W: Write + Seek>(
    input: &[u8],
    sink: W,
    opts: &RecoverOptions,
    disable_seeking: bool,
) -> Result<(RecoverStats, W)> {
    validate_start_magic(input)?;

    let mut write_options = mcap::WriteOptions::new()
        .chunk_size(Some(opts.chunk_size))
        .compression(opts.compression)
        .disable_seeking(disable_seeking);

    if let Some(header) = sniff_header(input) {
        write_options = write_options
            .profile(header.profile)
            .library(header.library);
    }

    let mut writer = write_options
        .create(sink)
        .context("failed to create output MCAP writer")?;
    let stats = recover_records(input, &mut writer, opts)?;
    writer.finish().context("failed to finish recovered MCAP")?;
    let sink = writer.into_inner();
    Ok((stats, sink))
}

fn validate_start_magic(input: &[u8]) -> Result<()> {
    if input.len() < mcap::MAGIC.len() || !input.starts_with(mcap::MAGIC) {
        return Err(mcap::McapError::BadMagic.into());
    }
    Ok(())
}

fn sniff_header(input: &[u8]) -> Option<records::Header> {
    let offset = mcap::MAGIC.len();
    if input.len() < offset + 9 {
        return None;
    }
    if input[offset] != op::HEADER {
        return None;
    }
    let length = u64::from_le_bytes(
        input[offset + 1..offset + 9]
            .try_into()
            .expect("record header len slice"),
    );
    let Ok(length) = usize::try_from(length) else {
        return None;
    };
    let body_start = offset + 9;
    let body_end = body_start.checked_add(length)?;
    let body = input.get(body_start..body_end)?;
    match mcap::parse_record(op::HEADER, body) {
        Ok(Record::Header(header)) => Some(header),
        Ok(_) | Err(_) => None,
    }
}

fn recover_records<W: Write + Seek>(
    input: &[u8],
    writer: &mut mcap::Writer<W>,
    opts: &RecoverOptions,
) -> Result<RecoverStats> {
    if opts.always_decode_chunk {
        eprintln!(
            "Note: --always-decode-chunk has no effect; the Rust recover implementation always decodes chunks."
        );
    }

    let mut reader = LinearReader::new_with_options(
        LinearReaderOptions::default()
            .with_skip_end_magic(true)
            .with_emit_chunks(false)
            // Recover should ignore chunk CRC mismatches and continue decoding payload data.
            .with_validate_chunk_crcs(false)
            .with_record_length_limit(input.len()),
    );

    let mut remaining = input;
    let mut state = RecoveryState::default();
    let mut stats = RecoverStats::default();
    let mut saw_any_record = false;

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
                        eprintln!(
                            "Warning: failed to parse record opcode 0x{opcode:02x}: {err:#}; skipping"
                        );
                        continue;
                    }
                };
                recover_record(writer, &mut state, &mut stats, record)?;
            }
            Err(err) => {
                if !saw_any_record {
                    return Err(err.into());
                }
                // LinearReader does not provide a resync primitive after stream-level decode
                // failures (e.g. corrupt compressed chunk payload), so stop and keep recovered
                // records written so far.
                eprintln!("Warning: {err:#} -- stopping recovery scan");
                break;
            }
        }
    }
    Ok(stats)
}

fn recover_record<W: Write + Seek>(
    writer: &mut mcap::Writer<W>,
    state: &mut RecoveryState,
    stats: &mut RecoverStats,
    record: Record<'_>,
) -> Result<()> {
    match record {
        Record::Schema { header, data } => register_schema(
            writer,
            state,
            header.id,
            &header.name,
            &header.encoding,
            data.as_ref(),
        )?,
        Record::Channel(channel) => {
            register_channel(writer, state, channel)?;
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
            writer.attach(&mcap::Attachment {
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
    writer: &mut mcap::Writer<W>,
    state: &mut RecoveryState,
    input_schema_id: u16,
    name: &str,
    encoding: &str,
    data: &[u8],
) -> Result<()> {
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

    let output_schema_id = writer.add_schema(name, encoding, data)?;
    state.schema_map.insert(input_schema_id, output_schema_id);
    state.seen_schemas.insert(input_schema_id, schema);
    resolve_pending_channels(writer, state, input_schema_id)?;
    Ok(())
}

fn register_channel<W: Write + Seek>(
    writer: &mut mcap::Writer<W>,
    state: &mut RecoveryState,
    channel: records::Channel,
) -> Result<()> {
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

    write_channel_mapping(writer, state, channel)
}

fn resolve_pending_channels<W: Write + Seek>(
    writer: &mut mcap::Writer<W>,
    state: &mut RecoveryState,
    schema_id: u16,
) -> Result<()> {
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
        write_channel_mapping(writer, state, channel)?;
    }

    Ok(())
}

fn write_channel_mapping<W: Write + Seek>(
    writer: &mut mcap::Writer<W>,
    state: &mut RecoveryState,
    channel: records::Channel,
) -> Result<()> {
    let output_schema_id = if channel.schema_id == 0 {
        0
    } else {
        match state.schema_map.get(&channel.schema_id) {
            Some(schema_id) => *schema_id,
            None => return Ok(()),
        }
    };
    let output_channel_id = writer.add_channel(
        output_schema_id,
        &channel.topic,
        &channel.message_encoding,
        &channel.metadata,
    )?;

    state.channel_map.insert(channel.id, output_channel_id);
    state.seen_channels.insert(channel.id, channel);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::Cursor;

    use super::{recover_to_sink, RecoverOptions};
    use mcap::records::{op, MessageHeader, Record};

    fn write_test_input() -> Vec<u8> {
        let mut output = Cursor::new(Vec::new());
        {
            let mut writer = mcap::WriteOptions::new()
                .chunk_size(Some(1024 * 1024))
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
                    data: std::borrow::Cow::Borrowed(&[1, 2, 3]),
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

    fn recover_to_vec(input: &[u8], opts: &RecoverOptions) -> (Vec<u8>, super::RecoverStats) {
        let output = Cursor::new(Vec::new());
        let (stats, output) =
            recover_to_sink(input, output, opts, false).expect("recover should succeed");
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

    fn default_options() -> RecoverOptions {
        RecoverOptions {
            compression: Some(mcap::Compression::Zstd),
            chunk_size: 4 * 1024 * 1024,
            always_decode_chunk: false,
        }
    }

    fn corrupt_first_chunk_crc(input: &mut [u8]) {
        let mut offset = mcap::MAGIC.len();
        let limit = input.len().saturating_sub(mcap::MAGIC.len());
        while offset + 9 <= limit {
            let opcode = input[offset];
            let length = u64::from_le_bytes(
                input[offset + 1..offset + 9]
                    .try_into()
                    .expect("record length bytes"),
            ) as usize;
            let end = offset + 9 + length;
            if opcode == op::CHUNK {
                let crc_offset = offset + 9 + 24;
                if crc_offset + 4 <= end {
                    input[crc_offset] ^= 0xFF;
                }
                return;
            }
            offset = end;
        }
        panic!("no chunk record found");
    }

    #[test]
    fn recovers_valid_input_with_attachments_and_metadata() {
        let input = write_test_input();
        let (output, stats) = recover_to_vec(&input, &default_options());
        let (messages, attachments, metadata) = count_output_records(&output);
        assert_eq!(messages, 300);
        assert_eq!(attachments, 1);
        assert_eq!(metadata, 1);
        assert_eq!(stats.messages, 300);
        assert_eq!(stats.attachments, 1);
        assert_eq!(stats.metadata, 1);
    }

    #[test]
    fn recovers_messages_from_truncated_input() {
        let mut input = write_test_input();
        input.truncate(input.len() / 2);

        let (output, stats) = recover_to_vec(&input, &default_options());
        let (messages, attachments, metadata) = count_output_records(&output);
        assert!(messages > 0);
        assert!(messages <= 300);
        assert!(attachments <= 1);
        assert!(metadata <= 1);
        assert_eq!(stats.messages as usize, messages);
        assert_eq!(stats.attachments as usize, attachments);
        assert_eq!(stats.metadata as usize, metadata);
    }

    #[test]
    fn ignores_invalid_chunk_crc_and_recovers_all_records_from_intact_chunk_data() {
        let mut input = write_test_input();
        corrupt_first_chunk_crc(&mut input);

        // This validates that disabled chunk CRC validation does not drop otherwise readable
        // data. It intentionally does not cover corrupted compressed payload bytes.
        let (output, stats) = recover_to_vec(&input, &default_options());
        let (messages, attachments, metadata) = count_output_records(&output);
        assert_eq!(messages, 300);
        assert_eq!(attachments, 1);
        assert_eq!(metadata, 1);
        assert_eq!(stats.messages, 300);
        assert_eq!(stats.attachments, 1);
        assert_eq!(stats.metadata, 1);
    }
}
