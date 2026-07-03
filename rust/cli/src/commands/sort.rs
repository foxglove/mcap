//! `sort` rewrites an MCAP with its messages in log-time order.
//!
//! Record placement convention (shared, standardized layout for rewriting commands):
//! metadata records are written first, immediately after the header; then messages; then
//! attachments, immediately before the data end record. Order within each group is preserved
//! (indexed inputs are copied in source-offset order; linear inputs in encounter order).
//!
//! This placement is invisible to indexed readers (they seek via the summary index, and neither
//! metadata nor attachments are duplicated into the summary), so it is chosen to serve
//! linear/streaming readers: small, file-level metadata is reachable up front, while potentially
//! large attachments stay out of the message read path. It also avoids fragmenting message chunks.
//!
//! The message-reading, record-copying, and placement helpers here are intended to be lifted into
//! a shared rewrite engine that `filter`/`merge`/`compress`/`decompress` will also use; this
//! command is the first to adopt the convention.
use std::borrow::Cow;
use std::io::{Seek, Write};

use anyhow::{Context, Result};

use crate::cli::{CompressionFormat, SortCommand};
use crate::commands::filter;
use crate::context::CommandContext;
use crate::{parse, source};

#[derive(Debug, Clone)]
struct SortOptions {
    compression: Option<mcap::Compression>,
    chunk_size: u64,
    include_crc: bool,
    chunked: bool,
}

#[derive(Debug)]
enum SortInput {
    Indexed(Box<mcap::Summary>),
    Linear,
}

pub fn run(ctx: &CommandContext, args: SortCommand) -> Result<()> {
    let opts = build_sort_options(&args);
    source::ensure_distinct_local_input_output(&args.file, &args.output_file)?;
    let input = source::load_path(
        &args.file,
        source::SourceOptions::new(ctx.allow_remote_scan()),
    )?;
    let sort_input = validate_sort_input(input.as_slice())?;
    let output = std::fs::File::create(&args.output_file)
        .with_context(|| format!("failed to open output '{}'", args.output_file.display()))?;
    sort_to_writer(input.as_slice(), output, sort_input, &opts)
}

fn build_sort_options(args: &SortCommand) -> SortOptions {
    SortOptions {
        compression: convert_compression(args.compression),
        chunk_size: args.chunk_size,
        include_crc: !args.no_crc,
        chunked: !args.no_chunks,
    }
}

fn convert_compression(value: CompressionFormat) -> Option<mcap::Compression> {
    match value {
        CompressionFormat::Zstd => Some(mcap::Compression::Zstd),
        CompressionFormat::Lz4 => Some(mcap::Compression::Lz4),
        CompressionFormat::None => None,
    }
}

fn sort_to_writer<W: Write + Seek>(
    input: &[u8],
    sink: W,
    sort_input: SortInput,
    opts: &SortOptions,
) -> Result<()> {
    let header = parse::read_header(input)?;
    let mut write_options = mcap::WriteOptions::new()
        .use_chunks(opts.chunked)
        .chunk_size(Some(opts.chunk_size))
        .compression(opts.compression)
        .calculate_chunk_crcs(opts.include_crc)
        .calculate_data_section_crc(opts.include_crc)
        .calculate_summary_section_crc(opts.include_crc)
        .calculate_attachment_crcs(opts.include_crc);
    write_options = write_options.library(crate::cli::LIBRARY_IDENTIFIER.clone());
    if let Some(header) = header {
        write_options = write_options.profile(header.profile);
    }

    let mut writer = write_options
        .create(sink)
        .context("failed to create mcap writer")?;

    // Standardized layout: metadata first (right after the header), then messages, then
    // attachments (just before the data end record). See the module docs for the rationale.
    match &sort_input {
        SortInput::Indexed(summary) => {
            copy_metadata_from_summary(input, summary, &mut writer)?;
            copy_messages_in_log_time_order(input, summary, &mut writer)?;
            copy_attachments_from_summary(input, summary, &mut writer)?;
        }
        SortInput::Linear => {
            copy_linear_metadata(input, &mut writer)?;
            copy_linear_messages_in_log_time_order(input, &mut writer)?;
            copy_linear_attachments(input, &mut writer)?;
        }
    }

    writer.finish().context("failed to finish mcap writer")?;
    Ok(())
}

fn validate_sort_input(input: &[u8]) -> Result<SortInput> {
    let summary = match mcap::Summary::read(input) {
        Ok(summary) => summary,
        Err(mcap::McapError::UnknownSchema(_, _))
            if !parse::summary_section_has_chunk_indexes(input)? =>
        {
            return Ok(SortInput::Linear);
        }
        Err(mcap::McapError::UnknownSchema(_, _)) => {
            return Err(filter::incomplete_indexed_summary_error());
        }
        Err(err) => return Err(err).context("failed to read file index"),
    };
    match summary {
        Some(summary) => {
            if !summary.chunk_indexes.is_empty() {
                if filter::summary_supports_indexed_transcode(&summary) {
                    return Ok(SortInput::Indexed(Box::new(summary)));
                }
                return Err(filter::incomplete_indexed_summary_error());
            }
            Ok(SortInput::Linear)
        }
        None => Ok(SortInput::Linear),
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

fn copy_linear_messages_in_log_time_order<W: Write + Seek>(
    input: &[u8],
    writer: &mut mcap::Writer<W>,
) -> Result<()> {
    // Without chunk indexes, sorting necessarily materializes messages before reordering.
    let mut messages = mcap::MessageStream::new(input)?
        .enumerate()
        .map(|(input_order, message)| message.map(|message| (input_order, message)))
        .collect::<mcap::McapResult<Vec<_>>>()
        .context("failed to read messages")?;
    messages.sort_by_key(|(input_order, message)| (message.log_time, *input_order));

    for (_, message) in messages {
        writer.write(&message)?;
    }

    Ok(())
}

fn copy_linear_metadata<W: Write + Seek>(input: &[u8], writer: &mut mcap::Writer<W>) -> Result<()> {
    for record in mcap::read::LinearReader::new(input)? {
        if let mcap::records::Record::Metadata(metadata) = record? {
            writer.write_metadata(&metadata)?;
        }
    }
    Ok(())
}

fn copy_linear_attachments<W: Write + Seek>(
    input: &[u8],
    writer: &mut mcap::Writer<W>,
) -> Result<()> {
    // Attachments are never stored inside chunks (spec op=0x09), so a top-level linear scan
    // surfaces them without decompressing any chunk data.
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::Cursor;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{sort_to_writer, validate_sort_input, SortInput, SortOptions};
    use crate::cli::CompressionFormat;
    use crate::cli::SortCommand;
    use crate::context::CommandContext;

    fn default_sort_options() -> SortOptions {
        SortOptions {
            compression: Some(mcap::Compression::Zstd),
            chunk_size: mcap::WriteOptions::DEFAULT_CHUNK_SIZE,
            include_crc: true,
            chunked: true,
        }
    }

    fn build_out_of_order_chunked_input(include_records: bool) -> Vec<u8> {
        build_out_of_order_chunked_input_with_options(include_records, true, true, true, true)
    }

    fn build_out_of_order_chunked_input_with_summary_repeats(
        include_records: bool,
        repeat_channels: bool,
        repeat_schemas: bool,
    ) -> Vec<u8> {
        build_out_of_order_chunked_input_with_options(
            include_records,
            repeat_channels,
            repeat_schemas,
            true,
            true,
        )
    }

    fn build_out_of_order_chunked_input_with_options(
        include_records: bool,
        repeat_channels: bool,
        repeat_schemas: bool,
        emit_message_indexes: bool,
        emit_chunk_indexes: bool,
    ) -> Vec<u8> {
        let mut output = Cursor::new(Vec::new());
        {
            let mut writer = mcap::WriteOptions::new()
                .chunk_size(Some(1024))
                .repeat_channels(repeat_channels)
                .repeat_schemas(repeat_schemas)
                .emit_message_indexes(emit_message_indexes)
                .emit_chunk_indexes(emit_chunk_indexes)
                .library("test-recorder/0.0")
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

    fn build_summaryless_equal_time_message_input() -> Vec<u8> {
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
            let first_channel_id = writer
                .add_channel(schema_id, "/first", "json", &BTreeMap::new())
                .expect("first channel");
            let second_channel_id = writer
                .add_channel(schema_id, "/second", "json", &BTreeMap::new())
                .expect("second channel");

            writer
                .write_to_known_channel(
                    &mcap::records::MessageHeader {
                        channel_id: second_channel_id,
                        sequence: 1,
                        log_time: 5,
                        publish_time: 5,
                    },
                    &[2],
                )
                .expect("second message");
            writer
                .write_to_known_channel(
                    &mcap::records::MessageHeader {
                        channel_id: first_channel_id,
                        sequence: 2,
                        log_time: 5,
                        publish_time: 5,
                    },
                    &[1],
                )
                .expect("first message");
            writer.finish().expect("finish");
        }
        output.into_inner()
    }

    fn build_summaryless_input_with_records() -> Vec<u8> {
        let mut output = Cursor::new(Vec::new());
        {
            let mut writer = mcap::WriteOptions::new()
                .use_chunks(false)
                .emit_summary_records(false)
                .emit_summary_offsets(false)
                .library("test-recorder/0.0")
                .create(&mut output)
                .expect("writer");
            let schema_id = writer
                .add_schema("Example", "jsonschema", br#"{"type":"object"}"#)
                .expect("schema");
            let channel_id = writer
                .add_channel(schema_id, "/demo", "json", &BTreeMap::new())
                .expect("channel");
            // Interleave metadata and an attachment with out-of-order messages so the sorted
            // output has to relocate them to the standardized positions.
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
            writer
                .write_metadata(&mcap::records::Metadata {
                    name: "demo".to_string(),
                    metadata: BTreeMap::from([("k".to_string(), "v".to_string())]),
                })
                .expect("metadata");
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
            writer
                .attach(&mcap::Attachment {
                    log_time: 100,
                    create_time: 100,
                    name: "demo.bin".to_string(),
                    media_type: "application/octet-stream".to_string(),
                    data: std::borrow::Cow::Borrowed(&[9, 8, 7]),
                })
                .expect("attachment");
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
        // Summary-section records use distinct opcodes (metadata/attachment *index*), so the first
        // hits for METADATA/ATTACHMENT/CHUNK are the data-section records.
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
        let summary = mcap::Summary::read(&input)
            .expect("summary read")
            .expect("summary should exist");
        sort_to_writer(
            &input,
            &mut output,
            SortInput::Indexed(Box::new(summary)),
            &default_sort_options(),
        )
        .expect("sort should succeed");
        let output = output.into_inner();

        let log_times: Vec<u64> = mcap::MessageStream::new(&output)
            .expect("message stream")
            .map(|message| message.expect("message").log_time)
            .collect();
        assert_eq!(log_times, vec![10, 30]);
    }

    #[test]
    fn sort_stamps_cli_writer_library() {
        let input = build_out_of_order_chunked_input(false);
        let mut output = Cursor::new(Vec::new());
        let summary = mcap::Summary::read(&input)
            .expect("summary read")
            .expect("summary should exist");
        sort_to_writer(
            &input,
            &mut output,
            SortInput::Indexed(Box::new(summary)),
            &default_sort_options(),
        )
        .expect("sort should succeed");
        let output = output.into_inner();

        // The fixture's `test-recorder/0.0` library is overwritten with the CLI's own identity.
        let library = crate::parse::read_header(&output)
            .expect("read header")
            .expect("header present")
            .library;
        assert_eq!(library, *crate::cli::LIBRARY_IDENTIFIER);
    }

    #[test]
    fn copies_attachments_and_metadata() {
        let input = build_out_of_order_chunked_input(true);
        let mut output = Cursor::new(Vec::new());
        let summary = mcap::Summary::read(&input)
            .expect("summary read")
            .expect("summary should exist");
        sort_to_writer(
            &input,
            &mut output,
            SortInput::Indexed(Box::new(summary)),
            &default_sort_options(),
        )
        .expect("sort should succeed");
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
    fn indexed_sort_places_metadata_first_and_attachments_last() {
        let input = build_out_of_order_chunked_input(true);
        let mut output = Cursor::new(Vec::new());
        let summary = mcap::Summary::read(&input)
            .expect("summary read")
            .expect("summary should exist");
        sort_to_writer(
            &input,
            &mut output,
            SortInput::Indexed(Box::new(summary)),
            &default_sort_options(),
        )
        .expect("sort should succeed");
        assert_standard_placement(&output.into_inner());
    }

    #[test]
    fn linear_sort_places_metadata_first_and_attachments_last() {
        let input = build_summaryless_input_with_records();
        // Confirm this fixture takes the linear path so the test exercises the linear helpers.
        assert!(matches!(
            validate_sort_input(&input).expect("sort input should be valid"),
            SortInput::Linear
        ));

        let mut output = Cursor::new(Vec::new());
        sort_to_writer(
            &input,
            &mut output,
            SortInput::Linear,
            &default_sort_options(),
        )
        .expect("sort should succeed");
        let output = output.into_inner();

        assert_standard_placement(&output);
        // Messages are still emitted in log-time order.
        let log_times: Vec<u64> = mcap::MessageStream::new(&output)
            .expect("message stream")
            .map(|message| message.expect("message").log_time)
            .collect();
        assert_eq!(log_times, vec![10, 30]);
    }

    #[test]
    fn run_sorts_summaryless_input_with_messages() {
        let input = build_summaryless_message_input();
        let input_path = unique_temp_path("input");
        let output_path = unique_temp_path("output");
        std::fs::write(&input_path, input).expect("write input fixture");
        std::fs::write(&output_path, b"replace-me").expect("write output sentinel");

        super::run(
            &CommandContext::default(),
            SortCommand {
                file: input_path.clone(),
                output_file: output_path.clone(),
                compression: CompressionFormat::Zstd,
                chunk_size: mcap::WriteOptions::DEFAULT_CHUNK_SIZE,
                no_crc: false,
                no_chunks: false,
            },
        )
        .expect("summaryless input with messages should sort");
        let output = std::fs::read(&output_path).expect("read output");
        let log_times: Vec<u64> = mcap::MessageStream::new(&output)
            .expect("message stream")
            .map(|message| message.expect("message").log_time)
            .collect();
        assert_eq!(log_times, vec![5]);

        let _ = std::fs::remove_file(input_path);
        let _ = std::fs::remove_file(output_path);
    }

    #[test]
    fn linear_sort_preserves_input_order_for_equal_log_times() {
        let input = build_summaryless_equal_time_message_input();
        let mut output = Cursor::new(Vec::new());

        sort_to_writer(
            &input,
            &mut output,
            SortInput::Linear,
            &default_sort_options(),
        )
        .expect("sort should succeed");
        let output = output.into_inner();

        let payloads: Vec<Vec<u8>> = mcap::MessageStream::new(&output)
            .expect("message stream")
            .map(|message| message.expect("message").data.to_vec())
            .collect();
        assert_eq!(payloads, vec![vec![2], vec![1]]);
    }

    #[test]
    fn run_rejects_same_input_and_output_file() {
        let input = build_out_of_order_chunked_input(false);
        let file_path = unique_temp_path("same-file");
        std::fs::write(&file_path, &input).expect("write input fixture");

        let err = super::run(
            &CommandContext::default(),
            SortCommand {
                file: file_path.clone(),
                output_file: file_path.clone(),
                compression: CompressionFormat::Zstd,
                chunk_size: mcap::WriteOptions::DEFAULT_CHUNK_SIZE,
                no_crc: false,
                no_chunks: false,
            },
        )
        .expect_err("same input/output path should fail");
        assert!(err.to_string().contains("input and output paths"));
        assert_eq!(std::fs::read(&file_path).expect("read input"), input);

        let _ = std::fs::remove_file(file_path);
    }

    #[test]
    fn run_treats_cloud_input_as_remote() {
        let err = super::run(
            &CommandContext::default(),
            SortCommand {
                file: PathBuf::from("s3://bucket/input.mcap?token=secret"),
                output_file: PathBuf::from("/tmp/mcap-cli-cloud-sort-output.mcap"),
                compression: CompressionFormat::Zstd,
                chunk_size: mcap::WriteOptions::DEFAULT_CHUNK_SIZE,
                no_crc: false,
                no_chunks: false,
            },
        )
        .expect_err("cloud input should require scan opt-in before download");
        assert!(err.to_string().contains("--allow-remote-scan"));
        assert!(!err.to_string().contains("token=secret"));
    }

    #[test]
    fn allows_summaryless_inputs_without_messages() {
        let input = build_summaryless_metadata_only_input();
        let mut output = Cursor::new(Vec::new());
        sort_to_writer(
            &input,
            &mut output,
            SortInput::Linear,
            &default_sort_options(),
        )
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
    fn convert_compression_maps_variants() {
        assert!(super::convert_compression(CompressionFormat::None).is_none());
        assert!(matches!(
            super::convert_compression(CompressionFormat::Lz4),
            Some(mcap::Compression::Lz4)
        ));
        assert!(matches!(
            super::convert_compression(CompressionFormat::Zstd),
            Some(mcap::Compression::Zstd)
        ));
    }

    #[test]
    fn summaryless_input_with_messages_uses_linear_sorting() {
        let input = build_summaryless_message_input();
        let sort_input = validate_sort_input(&input).expect("sort input should be valid");
        assert!(matches!(sort_input, SortInput::Linear));
    }

    #[test]
    fn chunked_input_without_chunk_index_uses_linear_sorting() {
        let input = build_out_of_order_chunked_input_with_options(false, true, true, true, false);
        let mut output = Cursor::new(Vec::new());
        let sort_input = validate_sort_input(&input).expect("sort input should be valid");
        assert!(matches!(sort_input, SortInput::Linear));

        sort_to_writer(&input, &mut output, sort_input, &default_sort_options())
            .expect("sort should succeed");
        let output = output.into_inner();
        let log_times: Vec<u64> = mcap::MessageStream::new(&output)
            .expect("message stream")
            .map(|message| message.expect("message").log_time)
            .collect();
        assert_eq!(log_times, vec![10, 30]);
    }

    #[test]
    fn chunked_input_without_chunk_index_falls_back_to_linear_sorting_on_unknown_schema() {
        let input = build_out_of_order_chunked_input_with_options(false, true, false, true, false);
        let mut output = Cursor::new(Vec::new());
        let sort_input = validate_sort_input(&input).expect("sort input should be valid");
        assert!(matches!(sort_input, SortInput::Linear));

        sort_to_writer(&input, &mut output, sort_input, &default_sort_options())
            .expect("sort should succeed");
        let output = output.into_inner();
        let log_times: Vec<u64> = mcap::MessageStream::new(&output)
            .expect("message stream")
            .map(|message| message.expect("message").log_time)
            .collect();
        assert_eq!(log_times, vec![10, 30]);
    }

    #[test]
    fn chunk_indexed_input_without_repeated_channels_errors() {
        let input = build_out_of_order_chunked_input_with_summary_repeats(false, false, false);
        let err = validate_sort_input(&input).expect_err("invalid indexed summary should fail");
        assert!(err.to_string().contains("mcap recover"));
    }

    #[test]
    fn chunk_indexed_input_without_channels_or_message_indexes_errors() {
        let input = build_out_of_order_chunked_input_with_options(false, false, false, false, true);
        let err = validate_sort_input(&input).expect_err("invalid indexed summary should fail");
        assert!(err.to_string().contains("mcap recover"));
    }

    #[test]
    fn chunk_indexed_input_without_repeated_schemas_errors() {
        let input = build_out_of_order_chunked_input_with_summary_repeats(false, true, false);
        let err = validate_sort_input(&input).expect_err("invalid indexed summary should fail");
        assert!(err.to_string().contains("mcap recover"));
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
