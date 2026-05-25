use std::io::IsTerminal as _;

use anyhow::{bail, Context, Result};

use crate::cli::RecoverCommand;
use crate::context::CommandContext;

pub fn run(_ctx: &CommandContext, args: RecoverCommand) -> Result<()> {
    let compression = crate::commands::common::parse_output_compression(&args.compression)?;
    let input = crate::commands::common::load_input(args.file.as_deref())?;

    let stats = if let Some(output) = &args.output {
        let writer = std::fs::File::create(output)
            .with_context(|| format!("failed to open '{}' for writing", output.display()))?;
        let opts = mcap::recover::RecoverOptions {
            compression,
            chunk_size: args.chunk_size,
            always_decode_chunk: args.always_decode_chunk,
            disable_seeking: false,
        };
        let (stats, writer) = mcap::recover::recover_to_sink(input.as_slice(), writer, &opts)
            .context("failed to recover MCAP")?;
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
        let opts = mcap::recover::RecoverOptions {
            compression,
            chunk_size: args.chunk_size,
            always_decode_chunk: args.always_decode_chunk,
            disable_seeking: true,
        };
        let (stats, _) = mcap::recover::recover_to_sink(input.as_slice(), writer, &opts)
            .context("failed to recover MCAP")?;
        stats
    };

    eprintln!(
        "Recovered {} messages, {} attachments, and {} metadata records.",
        stats.messages, stats.attachments, stats.metadata
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::Cursor;

    use mcap::records::{op, MessageHeader, Record};
    use mcap::recover::{recover_to_sink, RecoverOptions, RecoverStats};

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

    fn write_multi_chunk_input() -> Vec<u8> {
        let mut output = Cursor::new(Vec::new());
        {
            let mut writer = mcap::WriteOptions::new()
                .chunk_size(Some(128))
                .create(&mut output)
                .expect("writer");
            let channel = writer
                .add_channel(0, "multi", "json", &BTreeMap::new())
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

    fn recover_to_vec(input: &[u8], opts: &RecoverOptions) -> (Vec<u8>, RecoverStats) {
        let output = Cursor::new(Vec::new());
        let (stats, output) = recover_to_sink(input, output, opts).expect("recover should succeed");
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

    fn count_messages_allowing_bad_chunk_crc(bytes: &[u8]) -> usize {
        mcap::MessageStream::new(bytes)
            .expect("message stream")
            .filter_map(|message| match message {
                Ok(_) => Some(()),
                Err(mcap::McapError::BadChunkCrc { .. }) => None,
                Err(err) => panic!("unexpected message read error: {err}"),
            })
            .count()
    }

    fn collect_chunks(bytes: &[u8]) -> Vec<(mcap::records::ChunkHeader, Vec<u8>)> {
        mcap::read::LinearReader::new(bytes)
            .expect("linear reader")
            .filter_map(|record| match record.expect("record parse") {
                Record::Chunk { header, data } => Some((header, data.into_owned())),
                _ => None,
            })
            .collect()
    }

    fn top_level_chunk_ranges(bytes: &[u8]) -> Vec<(usize, usize)> {
        let mut offset = mcap::MAGIC.len();
        let limit = bytes.len().saturating_sub(mcap::MAGIC.len());
        let mut ranges = Vec::new();
        while offset + mcap::records::OPCODE_LEN_SIZE <= limit {
            let opcode = bytes[offset];
            let length = u64::from_le_bytes(
                bytes[offset + 1..offset + mcap::records::OPCODE_LEN_SIZE]
                    .try_into()
                    .expect("record length bytes"),
            ) as usize;
            let record_len = mcap::records::OPCODE_LEN_SIZE + length;
            if opcode == op::CHUNK {
                ranges.push((offset, record_len));
            }
            offset += record_len;
        }
        ranges
    }

    fn default_options() -> RecoverOptions {
        RecoverOptions {
            compression: Some(mcap::Compression::Zstd),
            chunk_size: 4 * 1024 * 1024,
            always_decode_chunk: false,
            disable_seeking: false,
        }
    }

    fn corrupt_first_chunk_crc(input: &mut [u8]) {
        let mut offset = mcap::MAGIC.len();
        let limit = input.len().saturating_sub(mcap::MAGIC.len());
        while offset + mcap::records::OPCODE_LEN_SIZE <= limit {
            let opcode = input[offset];
            let length = u64::from_le_bytes(
                input[offset + 1..offset + mcap::records::OPCODE_LEN_SIZE]
                    .try_into()
                    .expect("record length bytes"),
            ) as usize;
            let end = offset + mcap::records::OPCODE_LEN_SIZE + length;
            if opcode == op::CHUNK {
                let crc_offset = offset + mcap::records::OPCODE_LEN_SIZE + 24;
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
    fn preserves_raw_chunks_by_default() {
        let input = write_test_input();
        let opts = RecoverOptions {
            compression: None,
            ..default_options()
        };

        let (output, stats) = recover_to_vec(&input, &opts);
        assert_eq!(collect_chunks(&output), collect_chunks(&input));
        assert_eq!(stats.messages, 300);
        let summary = mcap::Summary::read(&output)
            .expect("summary should parse")
            .expect("summary should be present");
        let summary_stats = summary.stats.expect("statistics should be present");
        assert_eq!(summary_stats.message_count, 300);
        assert_eq!(
            summary_stats.channel_message_counts.values().sum::<u64>(),
            300
        );
        assert_eq!(summary_stats.channel_message_counts.len(), 3);
        assert!(summary
            .chunk_indexes
            .iter()
            .all(|index| !index.message_index_offsets.is_empty()));
    }

    #[test]
    fn flushes_last_complete_raw_chunk_after_truncated_following_chunk() {
        let mut input = write_multi_chunk_input();
        let original_chunks = collect_chunks(&input);
        let chunk_ranges = top_level_chunk_ranges(&input);
        assert!(
            chunk_ranges.len() >= 2,
            "test input should contain multiple chunks"
        );
        let (second_chunk_offset, second_chunk_len) = chunk_ranges[1];
        input.truncate(second_chunk_offset + second_chunk_len / 2);

        let (output, stats) = recover_to_vec(&input, &default_options());
        assert_eq!(collect_chunks(&output), vec![original_chunks[0].clone()]);
        assert!(stats.messages > 0);
        assert!(stats.messages < 300);
    }

    #[test]
    fn always_decode_chunk_rewrites_chunks_with_requested_compression() {
        let input = write_test_input();
        let opts = RecoverOptions {
            compression: None,
            always_decode_chunk: true,
            ..default_options()
        };

        let (output, stats) = recover_to_vec(&input, &opts);
        let chunks = collect_chunks(&output);
        assert!(!chunks.is_empty());
        assert!(chunks
            .iter()
            .all(|(header, _)| header.compression.is_empty()));
        assert_eq!(stats.messages, 300);
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
        // data. The raw-passthrough path preserves the original chunk bytes, including the
        // bad stored CRC, matching Go recover behavior.
        let (output, stats) = recover_to_vec(&input, &default_options());
        let messages = count_messages_allowing_bad_chunk_crc(&output);
        let (_, attachments, metadata) = count_output_records(&output);
        assert_eq!(messages, 300);
        assert_eq!(attachments, 1);
        assert_eq!(metadata, 1);
        assert_eq!(stats.messages, 300);
        assert_eq!(stats.attachments, 1);
        assert_eq!(stats.metadata, 1);
    }
}
