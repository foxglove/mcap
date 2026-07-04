//! `sort` rewrites an MCAP with its messages in log-time order.
//!
//! It is a thin preset over the shared `filter` engine: `sort` is `filter --order-by log_time`. It
//! shares `filter`'s flag surface (topic/time selection, `--exclude-*`, compression, chunking, CRC)
//! via [`crate::cli::TranscodeArgs`], and only fixes the message ordering. The engine handles
//! reading (indexed or summaryless), ordering, and the standardized record placement.
use anyhow::Result;

use crate::cli::SortCommand;
use crate::commands::filter;
use crate::context::CommandContext;
use crate::source;

pub fn run(ctx: &CommandContext, args: SortCommand) -> Result<()> {
    args.transcode.warn_deprecations();
    filter::run_transcode(
        args.transcode
            .command_options(Some(args.file), Some(args.output_file), true),
        source::SourceOptions::new(ctx.allow_remote_scan()),
    )
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::collections::BTreeMap;
    use std::io::Cursor;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use mcap::records::MessageHeader;

    use crate::cli::{CompressionFormat, SortCommand, TranscodeArgs};
    use crate::context::CommandContext;

    fn transcode_args() -> TranscodeArgs {
        TranscodeArgs {
            include_topic_regex: Vec::new(),
            exclude_topic_regex: Vec::new(),
            last_per_channel_topic_regex: Vec::new(),
            start: None,
            start_secs: 0,
            start_nsecs: 0,
            end: None,
            end_secs: 0,
            end_nsecs: 0,
            exclude_metadata: false,
            exclude_attachments: false,
            include_metadata: false,
            include_attachments: false,
            compression: CompressionFormat::Zstd,
            output_compression: None,
            chunk_size: mcap::WriteOptions::DEFAULT_CHUNK_SIZE,
            no_crc: false,
            no_chunks: false,
        }
    }

    fn sort_command(file: PathBuf, output_file: PathBuf) -> SortCommand {
        SortCommand {
            file,
            output_file,
            transcode: transcode_args(),
        }
    }

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

    fn run_sort(command: SortCommand, input: &[u8]) -> Vec<u8> {
        std::fs::write(&command.file, input).expect("write input fixture");
        let output_path = command.output_file.clone();
        let input_path = command.file.clone();
        super::run(&CommandContext::default(), command).expect("sort should succeed");
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
    fn command_options_force_log_time_ordering_and_keep_records() {
        let command = SortCommand {
            transcode: TranscodeArgs {
                compression: CompressionFormat::Lz4,
                no_crc: true,
                no_chunks: true,
                ..transcode_args()
            },
            ..sort_command("in.mcap".into(), "out.mcap".into())
        };
        let opts = command.transcode.command_options(
            Some(command.file.clone()),
            Some(command.output_file.clone()),
            true,
        );

        assert_eq!(opts.output_compression, "lz4");
        assert!(!opts.use_chunks, "--no-chunks disables chunking");
        assert!(!opts.include_crc, "--no-crc disables CRCs");
        // sort keeps everything by default and always orders by log time.
        assert!(opts.include_metadata);
        assert!(opts.include_attachments);
        assert!(opts.order_by_log_time);
    }

    #[test]
    fn run_sorts_indexed_input_and_keeps_records() {
        let command = sort_command(unique_temp_path("input"), unique_temp_path("output"));
        let output = run_sort(command, &build_out_of_order_indexed_input());
        assert_eq!(log_times(&output), vec![10, 30]);

        let summary = mcap::Summary::read(&output)
            .expect("summary read")
            .expect("summary should exist");
        assert_eq!(summary.metadata_indexes.len(), 1);
        assert_eq!(summary.attachment_indexes.len(), 1);
    }

    #[test]
    fn run_sorts_summaryless_input() {
        let command = sort_command(unique_temp_path("input"), unique_temp_path("output"));
        let output = run_sort(command, &build_out_of_order_summaryless_input());
        assert_eq!(log_times(&output), vec![10, 30]);
    }

    #[test]
    fn run_honors_shared_topic_filter() {
        // sort now shares filter's selection flags: an exclude-topic filter is respected.
        let mut command = sort_command(unique_temp_path("input"), unique_temp_path("output"));
        command.transcode.exclude_topic_regex = vec!["/demo".to_string()];
        let output = run_sort(command, &build_out_of_order_indexed_input());
        assert!(
            log_times(&output).is_empty(),
            "excluded topic should be dropped"
        );
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
