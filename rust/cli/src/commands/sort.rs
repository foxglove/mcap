//! `sort` rewrites an MCAP with its messages in log-time order.
//!
//! It is a thin preset over the shared `filter` engine: `sort` is `filter --order-by log_time`
//! keeping all records. The engine handles reading (indexed or summaryless), log-time ordering, and
//! the standardized record placement (metadata first, attachments last).
use anyhow::Result;

use crate::cli::{CompressionFormat, SortCommand};
use crate::commands::filter::{self, TranscodeCommandOptions};
use crate::context::CommandContext;
use crate::source;

pub fn run(ctx: &CommandContext, args: SortCommand) -> Result<()> {
    filter::run_transcode(
        build_transcode_options(args),
        source::SourceOptions::new(ctx.allow_remote_scan()),
    )
}

fn build_transcode_options(args: SortCommand) -> TranscodeCommandOptions {
    TranscodeCommandOptions::new(Some(args.file), Some(args.output_file), args.chunk_size)
        .compression(compression_name(args.compression))
        .use_chunks(!args.no_chunks)
        .include_crc(!args.no_crc)
        .include_metadata(true)
        .include_attachments(true)
        .order_by_log_time(true)
}

fn compression_name(value: CompressionFormat) -> &'static str {
    match value {
        CompressionFormat::Zstd => "zstd",
        CompressionFormat::Lz4 => "lz4",
        CompressionFormat::None => "none",
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::collections::BTreeMap;
    use std::io::Cursor;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use mcap::records::MessageHeader;

    use super::build_transcode_options;
    use crate::cli::{CompressionFormat, SortCommand};
    use crate::context::CommandContext;

    fn unique_temp_path(stem: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after epoch")
            .as_nanos();
        path.push(format!(
            "mcap_cli_sort_test_{stem}_{}_{}",
            std::process::id(),
            nonce
        ));
        path
    }

    /// Chunked (indexed) input with out-of-order messages plus a metadata record and an attachment.
    fn build_out_of_order_indexed_input() -> Vec<u8> {
        let mut output = Cursor::new(Vec::new());
        {
            let mut writer = mcap::WriteOptions::new()
                .chunk_size(Some(1024))
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
                    &MessageHeader {
                        channel_id,
                        sequence: 1,
                        log_time: 30,
                        publish_time: 30,
                    },
                    &[1],
                )
                .expect("message");
            writer.flush().expect("flush");
            writer
                .write_to_known_channel(
                    &MessageHeader {
                        channel_id,
                        sequence: 2,
                        log_time: 10,
                        publish_time: 10,
                    },
                    &[2],
                )
                .expect("message");
            writer
                .attach(&mcap::Attachment {
                    log_time: 100,
                    create_time: 100,
                    name: "demo.bin".to_string(),
                    media_type: "application/octet-stream".to_string(),
                    data: Cow::Borrowed(&[9, 8, 7]),
                })
                .expect("attachment");
            writer
                .write_metadata(&mcap::records::Metadata {
                    name: "demo".to_string(),
                    metadata: BTreeMap::from([("k".to_string(), "v".to_string())]),
                })
                .expect("metadata");
            writer.finish().expect("finish");
        }
        output.into_inner()
    }

    /// Unchunked, summaryless input with out-of-order messages.
    fn build_out_of_order_summaryless_input() -> Vec<u8> {
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
            for (sequence, log_time) in [(1u32, 30u64), (2, 10)] {
                writer
                    .write_to_known_channel(
                        &MessageHeader {
                            channel_id,
                            sequence,
                            log_time,
                            publish_time: log_time,
                        },
                        &[sequence as u8],
                    )
                    .expect("message");
            }
            writer.finish().expect("finish");
        }
        output.into_inner()
    }

    fn sort_command(file: PathBuf, output_file: PathBuf) -> SortCommand {
        SortCommand {
            file,
            output_file,
            compression: CompressionFormat::Zstd,
            chunk_size: mcap::WriteOptions::DEFAULT_CHUNK_SIZE,
            no_crc: false,
            no_chunks: false,
        }
    }

    fn run_sort_on(input: &[u8]) -> Vec<u8> {
        let input_path = unique_temp_path("input");
        let output_path = unique_temp_path("output");
        std::fs::write(&input_path, input).expect("write input fixture");

        super::run(
            &CommandContext::default(),
            sort_command(input_path.clone(), output_path.clone()),
        )
        .expect("sort should succeed");

        let output = std::fs::read(&output_path).expect("read output");
        let _ = std::fs::remove_file(input_path);
        let _ = std::fs::remove_file(output_path);
        output
    }

    fn log_times(output: &[u8]) -> Vec<u64> {
        mcap::MessageStream::new(output)
            .expect("message stream")
            .map(|message| message.expect("message").log_time)
            .collect()
    }

    #[test]
    fn build_transcode_options_maps_sort_flags() {
        let opts = build_transcode_options(SortCommand {
            file: "in.mcap".into(),
            output_file: "out.mcap".into(),
            compression: CompressionFormat::Lz4,
            chunk_size: 4096,
            no_crc: true,
            no_chunks: true,
        });

        assert_eq!(opts.file, Some(PathBuf::from("in.mcap")));
        assert_eq!(opts.output, Some(PathBuf::from("out.mcap")));
        assert_eq!(opts.output_compression, "lz4");
        assert_eq!(opts.chunk_size, 4096);
        assert!(!opts.use_chunks, "--no-chunks disables chunking");
        assert!(!opts.include_crc, "--no-crc disables CRCs");
        // sort keeps everything and always orders by log time.
        assert!(opts.include_metadata);
        assert!(opts.include_attachments);
        assert!(opts.order_by_log_time);
    }

    #[test]
    fn run_sorts_indexed_input_and_keeps_records() {
        let output = run_sort_on(&build_out_of_order_indexed_input());
        assert_eq!(log_times(&output), vec![10, 30]);

        let summary = mcap::Summary::read(&output)
            .expect("summary read")
            .expect("summary should exist");
        assert_eq!(summary.metadata_indexes.len(), 1);
        assert_eq!(summary.attachment_indexes.len(), 1);
    }

    #[test]
    fn run_sorts_summaryless_input() {
        let output = run_sort_on(&build_out_of_order_summaryless_input());
        assert_eq!(log_times(&output), vec![10, 30]);
    }

    #[test]
    fn run_rejects_same_input_and_output_file() {
        let input = build_out_of_order_indexed_input();
        let path = unique_temp_path("same-file");
        std::fs::write(&path, &input).expect("write input fixture");

        let err = super::run(
            &CommandContext::default(),
            sort_command(path.clone(), path.clone()),
        )
        .expect_err("same input/output path should fail");
        assert!(err.to_string().contains("input and output paths"));
        assert_eq!(std::fs::read(&path).expect("read input"), input);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn run_treats_cloud_input_as_remote() {
        let err = super::run(
            &CommandContext::default(),
            sort_command(
                PathBuf::from("s3://bucket/input.mcap?token=secret"),
                PathBuf::from("/tmp/mcap-cli-cloud-sort-output.mcap"),
            ),
        )
        .expect_err("cloud input should require scan opt-in before download");
        assert!(err.to_string().contains("--allow-remote-scan"));
        assert!(!err.to_string().contains("token=secret"));
    }
}
