//! `sort` rewrites an MCAP with its messages in log-time order.
//!
//! It is a thin preset over the shared [`crate::rewrite`] engine: `sort` is `filter` with
//! `--order` defaulting to `log_time` instead of `preserve`. The engine handles reading (indexed
//! or summaryless), ordering, and the standardized record placement (metadata first, attachments
//! last); `sort` only supplies the preset options. `--order` stays a real flag so future modes
//! (for example `publish_time`) apply to `sort` too.
use anyhow::Result;

use crate::cli::SortCommand;
use crate::context::CommandContext;
use crate::rewrite::{self, RewriteOptions};
use crate::source;

pub fn run(ctx: &CommandContext, args: SortCommand) -> Result<()> {
    args.common.warn_deprecations();
    rewrite::run(
        RewriteOptions::from(&args),
        source::SourceOptions::new(ctx.allow_remote_scan()),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::Cursor;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use mcap::records::{op, MessageHeader};

    use super::run;
    use crate::cli::{CommonRewriteArgs, CompressionFormat, MessageOrder, SortCommand};
    use crate::context::CommandContext;

    fn sort_command(file: PathBuf, output: PathBuf) -> SortCommand {
        SortCommand {
            common: CommonRewriteArgs {
                file: Some(file),
                output: Some(output),
                output_file: None,
                chunk_size: mcap::WriteOptions::DEFAULT_CHUNK_SIZE,
                no_crc: false,
            },
            compression: CompressionFormat::Zstd,
            no_chunks: false,
            order: MessageOrder::LogTime,
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

    /// Chunked (indexed) input whose messages are stored out of log-time order, with a metadata
    /// record and an attachment, so a successful sort is observable and the standardized placement
    /// can be asserted.
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
            for (sequence, log_time) in [(1u32, 30u64), (2, 10), (3, 20)] {
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
            writer.finish().expect("finish");
        }
        output.into_inner()
    }

    /// Summaryless, unchunked input with out-of-order messages, so the engine's linear (buffered)
    /// ordering path is exercised end to end through `sort`.
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
            for (sequence, log_time) in [(1u32, 30u64), (2, 10), (3, 20)] {
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

    fn output_log_times(output: &[u8]) -> Vec<u64> {
        mcap::MessageStream::new(output)
            .expect("message stream")
            .map(|message| message.expect("message").log_time)
            .collect()
    }

    fn run_sort(
        input: Vec<u8>,
        mut command: impl FnMut(&PathBuf, &PathBuf) -> SortCommand,
    ) -> Vec<u8> {
        let input_path = unique_temp_path("input");
        let output_path = unique_temp_path("output");
        std::fs::write(&input_path, input).expect("write input fixture");

        let result = run(
            &CommandContext::default(),
            command(&input_path, &output_path),
        );
        let output = result.and_then(|()| Ok(std::fs::read(&output_path)?));

        let _ = std::fs::remove_file(&input_path);
        let _ = std::fs::remove_file(&output_path);
        output.expect("sort should succeed")
    }

    #[test]
    fn sorts_indexed_input_and_keeps_metadata_and_attachments() {
        let output = run_sort(build_out_of_order_indexed_input(), |input, out| {
            sort_command(input.clone(), out.clone())
        });

        assert_eq!(
            output_log_times(&output),
            vec![10, 20, 30],
            "messages should be re-ordered by log time"
        );

        let summary = mcap::Summary::read(&output)
            .expect("summary read")
            .expect("summary present");
        assert_eq!(summary.metadata_indexes.len(), 1);
        assert_eq!(summary.attachment_indexes.len(), 1);

        // Standardized placement: metadata precedes the first chunk, the attachment follows it.
        let opcodes: Vec<u8> = mcap::read::LinearReader::new(&output)
            .expect("reader")
            .map(|record| record.expect("record").opcode())
            .collect();
        let position = |target: u8| opcodes.iter().position(|&opcode| opcode == target);
        assert!(position(op::METADATA) < position(op::CHUNK));
        assert!(position(op::CHUNK) < position(op::ATTACHMENT));
        assert!(position(op::ATTACHMENT) < position(op::DATA_END));
    }

    #[test]
    fn sorts_summaryless_input() {
        let output = run_sort(build_out_of_order_summaryless_input(), |input, out| {
            sort_command(input.clone(), out.clone())
        });
        assert_eq!(output_log_times(&output), vec![10, 20, 30]);
    }

    #[test]
    fn deprecated_output_file_alias_still_writes_output() {
        // Passing the deprecated `--output-file` (with `--output` absent) still produces sorted
        // output; the handler warns but does not fail.
        let output = run_sort(build_out_of_order_indexed_input(), |input, out| {
            let mut command = sort_command(input.clone(), out.clone());
            command.common.output = None;
            command.common.output_file = Some(out.clone());
            command
        });
        assert_eq!(output_log_times(&output), vec![10, 20, 30]);
    }

    #[test]
    fn order_preserve_keeps_stored_order() {
        // `--order` remains a real flag; `preserve` copies the input's stored order rather than
        // sorting, so future modes (for example `publish_time`) can slot in the same way.
        let output = run_sort(build_out_of_order_indexed_input(), |input, out| {
            let mut command = sort_command(input.clone(), out.clone());
            command.order = MessageOrder::Preserve;
            command
        });
        assert_eq!(
            output_log_times(&output),
            vec![30, 10, 20],
            "preserve should keep the input's stored order"
        );
    }

    #[test]
    fn stamps_cli_writer_library() {
        let output = run_sort(build_out_of_order_indexed_input(), |input, out| {
            sort_command(input.clone(), out.clone())
        });
        let library = crate::parse::read_header(&output)
            .expect("read header")
            .expect("header present")
            .library;
        assert_eq!(library, *crate::cli::LIBRARY_IDENTIFIER);
    }

    #[test]
    fn rejects_same_input_and_output_file() {
        let input = build_out_of_order_indexed_input();
        let file_path = unique_temp_path("same-file");
        std::fs::write(&file_path, &input).expect("write input fixture");

        let err = run(
            &CommandContext::default(),
            sort_command(file_path.clone(), file_path.clone()),
        )
        .expect_err("same input/output path should fail");
        assert!(err.to_string().contains("input and output paths"));
        assert_eq!(std::fs::read(&file_path).expect("read input"), input);

        let _ = std::fs::remove_file(file_path);
    }

    #[test]
    fn treats_cloud_input_as_remote() {
        let err = run(
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
