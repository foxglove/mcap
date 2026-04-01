use std::borrow::Cow;
use std::io::{Seek, Write};

use anyhow::{bail, Context, Result};

use crate::cli::{ConvertCompression, SortCommand};
use crate::commands::common;
use crate::context::CommandContext;

#[derive(Debug, Clone)]
struct SortOptions {
    compression: Option<mcap::Compression>,
    chunk_size: u64,
    include_crc: bool,
    chunked: bool,
}

pub fn run(_ctx: &CommandContext, args: SortCommand) -> Result<()> {
    let opts = build_sort_options(&args)?;
    let input = common::map_file(&args.file)?;
    validate_sort_input(input.as_ref())?;
    let output = std::fs::File::create(&args.output_file)
        .with_context(|| format!("failed to open output '{}'", args.output_file.display()))?;
    sort_to_writer(input.as_ref(), output, &opts)
}

fn build_sort_options(args: &SortCommand) -> Result<SortOptions> {
    Ok(SortOptions {
        compression: convert_compression(args.compression),
        chunk_size: args.chunk_size,
        include_crc: args.include_crc,
        chunked: args.chunked,
    })
}

fn convert_compression(value: ConvertCompression) -> Option<mcap::Compression> {
    match value {
        ConvertCompression::Zstd => Some(mcap::Compression::Zstd),
        ConvertCompression::Lz4 => Some(mcap::Compression::Lz4),
        ConvertCompression::None => None,
    }
}

fn sort_to_writer<W: Write + Seek>(input: &[u8], sink: W, opts: &SortOptions) -> Result<()> {
    let header = common::read_header(input)?;
    let mut write_options = mcap::WriteOptions::new()
        .use_chunks(opts.chunked)
        .chunk_size(Some(opts.chunk_size))
        .compression(opts.compression)
        .calculate_chunk_crcs(opts.include_crc)
        .calculate_data_section_crc(opts.include_crc)
        .calculate_summary_section_crc(opts.include_crc)
        .calculate_attachment_crcs(opts.include_crc);
    if let Some(header) = header {
        write_options = write_options
            .profile(header.profile)
            .library(header.library);
    }

    let mut writer = write_options
        .create(sink)
        .context("failed to create mcap writer")?;

    let summary = mcap::Summary::read(input).context("failed to read file index")?;
    if let Some(summary) = &summary {
        copy_attachments_from_summary(input, summary, &mut writer)?;
        copy_metadata_from_summary(input, summary, &mut writer)?;
        if !summary.chunk_indexes.is_empty() {
            copy_messages_in_log_time_order(input, summary, &mut writer)?;
        }
    } else {
        copy_linear_non_message_records(input, &mut writer)?;
    }

    writer.finish().context("failed to finish mcap writer")?;
    Ok(())
}

fn validate_sort_input(input: &[u8]) -> Result<()> {
    let summary = mcap::Summary::read(input).context("failed to read file index")?;
    let has_chunk_indexes = summary
        .as_ref()
        .is_some_and(|summary| !summary.chunk_indexes.is_empty());
    if !has_chunk_indexes && file_has_messages(input)? {
        let reason = if summary.is_none() {
            "summary section not available"
        } else {
            "no chunk index records"
        };
        bail!(
            "Error reading file index: {reason}. You may need to run `mcap recover` if the file is corrupt or not chunk indexed."
        );
    }
    Ok(())
}

fn file_has_messages(input: &[u8]) -> Result<bool> {
    let mut messages = mcap::MessageStream::new(input)?;
    match messages.next() {
        Some(Ok(_)) => Ok(true),
        Some(Err(err)) => Err(err.into()),
        None => Ok(false),
    }
}

fn copy_attachments_from_summary<W: Write + Seek>(
    input: &[u8],
    summary: &mcap::Summary,
    writer: &mut mcap::Writer<W>,
) -> Result<()> {
    let mut indexes = summary.attachment_indexes.clone();
    indexes.sort_by_key(|index| index.offset);
    for index in &indexes {
        let attachment = mcap::read::attachment(input, index)
            .with_context(|| format!("failed to read attachment at offset {}", index.offset))?;
        writer
            .attach(&attachment)
            .with_context(|| format!("failed to write attachment {}", index.name))?;
    }
    Ok(())
}

fn copy_metadata_from_summary<W: Write + Seek>(
    input: &[u8],
    summary: &mcap::Summary,
    writer: &mut mcap::Writer<W>,
) -> Result<()> {
    let mut indexes = summary.metadata_indexes.clone();
    indexes.sort_by_key(|index| index.offset);
    for index in &indexes {
        let metadata = mcap::read::metadata(input, index)
            .with_context(|| format!("failed to read metadata at offset {}", index.offset))?;
        writer
            .write_metadata(&metadata)
            .with_context(|| format!("failed to write metadata {}", metadata.name))?;
    }
    Ok(())
}

fn copy_messages_in_log_time_order<W: Write + Seek>(
    input: &[u8],
    summary: &mcap::Summary,
    writer: &mut mcap::Writer<W>,
) -> Result<()> {
    let mut reader = mcap::sans_io::IndexedReader::new_with_options(
        summary,
        mcap::sans_io::IndexedReaderOptions::new()
            .with_order(mcap::sans_io::indexed_reader::ReadOrder::LogTime),
    )?;

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
    Ok(())
}

fn copy_linear_non_message_records<W: Write + Seek>(
    input: &[u8],
    writer: &mut mcap::Writer<W>,
) -> Result<()> {
    for record in mcap::read::LinearReader::new(input)? {
        match record? {
            mcap::records::Record::Attachment { header, data, .. } => {
                writer.attach(&mcap::Attachment {
                    log_time: header.log_time,
                    create_time: header.create_time,
                    name: header.name,
                    media_type: header.media_type,
                    data: Cow::Borrowed(data.as_ref()),
                })?;
            }
            mcap::records::Record::Metadata(metadata) => {
                writer.write_metadata(&metadata)?;
            }
            _ => {}
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::Cursor;
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::cli::SortCommand;
    use crate::context::CommandContext;
    use super::{sort_to_writer, validate_sort_input, SortOptions};
    use crate::cli::ConvertCompression;

    fn default_sort_options() -> SortOptions {
        SortOptions {
            compression: Some(mcap::Compression::Zstd),
            chunk_size: 4 * 1024 * 1024,
            include_crc: true,
            chunked: true,
        }
    }

    fn build_out_of_order_chunked_input(include_records: bool) -> Vec<u8> {
        let mut output = Cursor::new(Vec::new());
        {
            let mut writer = mcap::WriteOptions::new()
                .chunk_size(Some(1024))
                .create(&mut output)
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
                .expect("write message");
            writer.flush().expect("flush");

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
                .expect("write message");

            if include_records {
                writer
                    .attach(&mcap::Attachment {
                        log_time: 100,
                        create_time: 100,
                        name: "demo.bin".to_string(),
                        media_type: "application/octet-stream".to_string(),
                        data: std::borrow::Cow::Borrowed(&[9, 8, 7]),
                    })
                    .expect("attachment");
                writer
                    .write_metadata(&mcap::records::Metadata {
                        name: "demo".to_string(),
                        metadata: BTreeMap::from([("k".to_string(), "v".to_string())]),
                    })
                    .expect("metadata");
            }

            writer.finish().expect("finish");
        }
        output.into_inner()
    }

    fn build_summaryless_message_input() -> Vec<u8> {
        let mut output = Cursor::new(Vec::new());
        {
            let mut writer = mcap::WriteOptions::new()
                .use_chunks(false)
                .emit_summary_records(false)
                .emit_summary_offsets(false)
                .create(&mut output)
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
                        log_time: 5,
                        publish_time: 5,
                    },
                    &[1],
                )
                .expect("message");
            writer.finish().expect("finish");
        }
        output.into_inner()
    }

    fn build_summaryless_metadata_only_input() -> Vec<u8> {
        let mut output = Cursor::new(Vec::new());
        {
            let mut writer = mcap::WriteOptions::new()
                .use_chunks(false)
                .emit_summary_records(false)
                .emit_summary_offsets(false)
                .create(&mut output)
                .expect("writer");
            writer
                .write_metadata(&mcap::records::Metadata {
                    name: "demo".to_string(),
                    metadata: BTreeMap::from([("foo".to_string(), "bar".to_string())]),
                })
                .expect("metadata");
            writer.finish().expect("finish");
        }
        output.into_inner()
    }

    #[test]
    fn sorts_messages_in_log_time_order() {
        let input = build_out_of_order_chunked_input(false);
        let mut output = Cursor::new(Vec::new());
        sort_to_writer(&input, &mut output, &default_sort_options()).expect("sort should succeed");
        let output = output.into_inner();

        let log_times: Vec<u64> = mcap::MessageStream::new(&output)
            .expect("message stream")
            .map(|message| message.expect("message").log_time)
            .collect();
        assert_eq!(log_times, vec![10, 30]);
    }

    #[test]
    fn copies_attachments_and_metadata() {
        let input = build_out_of_order_chunked_input(true);
        let mut output = Cursor::new(Vec::new());
        sort_to_writer(&input, &mut output, &default_sort_options()).expect("sort should succeed");
        let output = output.into_inner();

        let summary = mcap::Summary::read(&output)
            .expect("summary read")
            .expect("summary should exist");
        assert_eq!(summary.attachment_indexes.len(), 1);
        assert_eq!(summary.metadata_indexes.len(), 1);

        let attachment = mcap::read::attachment(&output, &summary.attachment_indexes[0])
            .expect("attachment should parse");
        assert_eq!(attachment.name, "demo.bin");
        assert_eq!(attachment.data.as_ref(), &[9, 8, 7]);

        let metadata = mcap::read::metadata(&output, &summary.metadata_indexes[0])
            .expect("metadata should parse");
        assert_eq!(metadata.name, "demo");
        assert_eq!(metadata.metadata.get("k"), Some(&"v".to_string()));
    }

    #[test]
    fn run_rejects_unindexed_input_without_truncating_existing_output() {
        let input = build_summaryless_message_input();
        let input_path = unique_temp_path("input");
        let output_path = unique_temp_path("output");
        std::fs::write(&input_path, input).expect("write input fixture");
        std::fs::write(&output_path, b"do-not-truncate").expect("write output sentinel");

        let err = super::run(
            &CommandContext::default(),
            SortCommand {
                file: input_path.clone(),
                output_file: output_path.clone(),
                compression: ConvertCompression::Zstd,
                chunk_size: 4 * 1024 * 1024,
                include_crc: true,
                chunked: true,
            },
        )
        .expect_err("unindexed input with messages should fail");
        let text = err.to_string();
        assert!(text.contains("Error reading file index"));
        assert!(text.contains("mcap recover"));
        assert_eq!(
            std::fs::read(&output_path).expect("read output sentinel"),
            b"do-not-truncate"
        );

        let _ = std::fs::remove_file(input_path);
        let _ = std::fs::remove_file(output_path);
    }

    #[test]
    fn allows_summaryless_inputs_without_messages() {
        let input = build_summaryless_metadata_only_input();
        let mut output = Cursor::new(Vec::new());
        sort_to_writer(&input, &mut output, &default_sort_options())
            .expect("summaryless metadata-only file should sort");
        let output = output.into_inner();

        let mut metadata_count = 0usize;
        for record in mcap::read::LinearReader::new(&output)
            .expect("reader")
            .map(|record| record.expect("record"))
        {
            if matches!(record, mcap::records::Record::Metadata(_)) {
                metadata_count += 1;
            }
        }
        assert_eq!(metadata_count, 1);
    }

    #[test]
    fn rejects_invalid_output_compression() {
        assert!(matches!(
            super::convert_compression(ConvertCompression::None),
            None
        ));
        assert!(matches!(
            super::convert_compression(ConvertCompression::Lz4),
            Some(mcap::Compression::Lz4)
        ));
        assert!(matches!(
            super::convert_compression(ConvertCompression::Zstd),
            Some(mcap::Compression::Zstd)
        ));
    }

    #[test]
    fn validate_sort_input_rejects_unindexed_messages() {
        let input = build_summaryless_message_input();
        let err =
            validate_sort_input(&input).expect_err("unindexed input with messages should fail");
        let text = err.to_string();
        assert!(text.contains("Error reading file index"));
        assert!(text.contains("mcap recover"));
    }

    fn unique_temp_path(stem: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after epoch")
            .as_nanos();
        path.push(format!(
            "mcap_cli_sort_test_{stem}_{}_{}",
            std::process::id(),
            timestamp
        ));
        path
    }
}
