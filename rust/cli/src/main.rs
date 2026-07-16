mod cli;
mod commands;
mod context;
mod logsetup;
mod parse;
mod render;
mod rewrite;
mod source;

use std::process::ExitCode;

use anyhow::Result;
use clap::Parser;
use commands::CommandOutcome;
use context::CommandContext;

fn run() -> Result<CommandOutcome> {
    let args = cli::Args::parse();
    logsetup::init_logger(args.verbose, args.color)?;
    let ctx = CommandContext::new(
        args.verbose,
        args.color,
        args.allow_remote_scan,
        args.time_format,
    );

    commands::dispatch(&ctx, args.command)
}

fn main() -> ExitCode {
    // `main` is the single place that turns an outcome into a process exit code, so it runs only
    // after every command has returned and its output sinks have been dropped/flushed.
    match run() {
        Ok(outcome) => ExitCode::from(outcome.exit_code()),
        Err(e) => {
            eprintln!("Error: {e:#}");
            ExitCode::from(1)
        }
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;
    use clap_complete::Shell;

    use crate::cli::{
        AddAttachmentCommand, AddCommand, AddMetadataCommand, AddSubcommand, Args, CatCommand,
        CatFormat, CoalesceChannels, Command, CommonRewriteArgs, CompletionCommand,
        CompressCommand, CompressionFormat, ConvertCommand, DecompressCommand, DoctorCommand,
        DuCommand, FilterCommand, GetAttachmentCommand, GetCommand, GetMetadataCommand,
        GetSubcommand, InfoCommand, ListAttachmentsCommand, ListChannelsCommand, ListChunksCommand,
        ListCommand, ListMetadataCommand, ListSchemasCommand, ListSubcommand, MergeCommand,
        MessageOrder, RecoverCommand, SortCommand, TimeFormat,
    };

    #[test]
    fn parses_info_subcommand() {
        let args = Args::try_parse_from(["mcap", "info", "demo.mcap"]).expect("info should parse");
        assert_eq!(
            args.command,
            Command::Info(InfoCommand {
                file: "demo.mcap".into(),
            })
        );
    }

    #[test]
    fn parses_cat_subcommand_with_files() {
        let args =
            Args::try_parse_from(["mcap", "cat", "a.mcap", "b.mcap"]).expect("cat should parse");
        assert_eq!(
            args.command,
            Command::Cat(CatCommand {
                files: vec!["a.mcap".into(), "b.mcap".into()],
                topics: String::new(),
                start_secs: 0,
                start_nsecs: 0,
                end_secs: 0,
                end_nsecs: 0,
                format: CatFormat::Text,
                json: false,
            })
        );
    }

    #[test]
    fn parses_cat_without_files_for_stdin() {
        let args = Args::try_parse_from(["mcap", "cat"]).expect("cat should parse");
        assert_eq!(
            args.command,
            Command::Cat(CatCommand {
                files: Vec::new(),
                topics: String::new(),
                start_secs: 0,
                start_nsecs: 0,
                end_secs: 0,
                end_nsecs: 0,
                format: CatFormat::Text,
                json: false,
            })
        );
    }

    #[test]
    fn parses_cat_subcommand_with_flags() {
        let args = Args::try_parse_from([
            "mcap",
            "cat",
            "demo.mcap",
            "--topics",
            "/tf,/odom",
            "--start-secs",
            "10",
            "--end-nsecs",
            "20000000000",
            "--format=ndjson",
        ])
        .expect("cat should parse");
        assert_eq!(
            args.command,
            Command::Cat(CatCommand {
                files: vec!["demo.mcap".into()],
                topics: "/tf,/odom".to_string(),
                start_secs: 10,
                start_nsecs: 0,
                end_secs: 0,
                end_nsecs: 20_000_000_000,
                format: CatFormat::Ndjson,
                json: false,
            })
        );
        assert!(matches!(args.command, Command::Cat(ref c) if c.json_output()));
    }

    #[test]
    fn parses_cat_deprecated_json_alias() {
        // `--json` is retained as a hidden deprecated alias for `--format=ndjson`.
        let args = Args::try_parse_from(["mcap", "cat", "demo.mcap", "--json"])
            .expect("deprecated --json should still parse");
        assert_eq!(
            args.command,
            Command::Cat(CatCommand {
                files: vec!["demo.mcap".into()],
                topics: String::new(),
                start_secs: 0,
                start_nsecs: 0,
                end_secs: 0,
                end_nsecs: 0,
                format: CatFormat::Text,
                json: true,
            })
        );
        assert!(matches!(args.command, Command::Cat(ref c) if c.json_output()));
    }

    #[test]
    fn cat_rejects_format_with_json_alias() {
        let parse_err =
            Args::try_parse_from(["mcap", "cat", "demo.mcap", "--format=ndjson", "--json"])
                .expect_err("--format and --json should conflict");
        assert_eq!(parse_err.kind(), clap::error::ErrorKind::ArgumentConflict);
    }

    #[test]
    fn cat_rejects_conflicting_time_units() {
        let parse_err = Args::try_parse_from([
            "mcap",
            "cat",
            "demo.mcap",
            "--start-secs",
            "1",
            "--start-nsecs",
            "1",
        ])
        .expect_err("start seconds and nanoseconds should conflict");
        assert_eq!(parse_err.kind(), clap::error::ErrorKind::ArgumentConflict);

        let parse_err = Args::try_parse_from([
            "mcap",
            "cat",
            "demo.mcap",
            "--end-secs",
            "1",
            "--end-nsecs",
            "1",
        ])
        .expect_err("end seconds and nanoseconds should conflict");
        assert_eq!(parse_err.kind(), clap::error::ErrorKind::ArgumentConflict);
    }

    #[test]
    fn parses_cat_csv_with_topics() {
        let args = Args::try_parse_from([
            "mcap",
            "cat",
            "demo.mcap",
            "--format=csv",
            "--topics",
            "/tf",
        ])
        .expect("cat --format=csv --topics should parse");
        assert_eq!(
            args.command,
            Command::Cat(CatCommand {
                files: vec!["demo.mcap".into()],
                topics: "/tf".to_string(),
                start_secs: 0,
                start_nsecs: 0,
                end_secs: 0,
                end_nsecs: 0,
                format: CatFormat::Csv,
                json: false,
            })
        );
    }

    #[test]
    fn cat_topic_is_a_hidden_alias_for_topics() {
        let args = Args::try_parse_from(["mcap", "cat", "demo.mcap", "--topic", "/tf"])
            .expect("--topic should parse as an alias for --topics");
        assert!(
            matches!(args.command, Command::Cat(ref c) if c.topics == "/tf"),
            "--topic should populate the topics field"
        );
    }

    #[test]
    fn cat_rejects_csv_with_json() {
        let parse_err = Args::try_parse_from([
            "mcap",
            "cat",
            "demo.mcap",
            "--format=csv",
            "--json",
            "--topics",
            "/tf",
        ])
        .expect_err("--format=csv and --json should conflict");
        assert_eq!(parse_err.kind(), clap::error::ErrorKind::ArgumentConflict);
    }

    #[test]
    fn parses_completion_subcommand() {
        let args =
            Args::try_parse_from(["mcap", "completion", "bash"]).expect("completion should parse");
        assert_eq!(
            args.command,
            Command::Completion(CompletionCommand { shell: Shell::Bash })
        );
    }

    #[test]
    fn completion_requires_known_shell() {
        Args::try_parse_from(["mcap", "completion", "notashell"])
            .expect_err("completion should reject unknown shells");
    }

    #[test]
    fn requires_subcommand() {
        let parse_err = Args::try_parse_from(["mcap"]).expect_err("subcommand is required");
        assert_eq!(
            parse_err.kind(),
            clap::error::ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand
        );
    }

    #[test]
    fn parses_global_allow_remote_scan_flag() {
        let args = Args::try_parse_from(["mcap", "--allow-remote-scan", "info", "demo.mcap"])
            .expect("allow remote scan should parse before subcommand");
        assert!(args.allow_remote_scan);
        assert_eq!(
            args.command,
            Command::Info(InfoCommand {
                file: "demo.mcap".into(),
            })
        );

        let args = Args::try_parse_from(["mcap", "info", "--allow-remote-scan", "demo.mcap"])
            .expect("allow remote scan should parse after subcommand");
        assert!(args.allow_remote_scan);
    }

    #[test]
    fn parses_global_time_format_flag() {
        let args =
            Args::try_parse_from(["mcap", "--time-format", "nanoseconds", "info", "demo.mcap"])
                .expect("time format should parse before subcommand");
        assert_eq!(args.time_format, TimeFormat::Nanoseconds);

        let args = Args::try_parse_from(["mcap", "cat", "--time-format=iso8601", "demo.mcap"])
            .expect("time format should parse after subcommand");
        assert_eq!(args.time_format, TimeFormat::Rfc3339);

        let args = Args::try_parse_from(["mcap", "info", "demo.mcap"]).expect("default parse");
        assert_eq!(args.time_format, TimeFormat::Auto);

        for (value, expected) in [
            ("auto", TimeFormat::Auto),
            ("rfc3339", TimeFormat::Rfc3339),
            ("iso8601", TimeFormat::Rfc3339),
            ("seconds", TimeFormat::Seconds),
            ("s", TimeFormat::Seconds),
            ("sec", TimeFormat::Seconds),
            ("secs", TimeFormat::Seconds),
            ("nanoseconds", TimeFormat::Nanoseconds),
            ("ns", TimeFormat::Nanoseconds),
            ("nano", TimeFormat::Nanoseconds),
            ("nanos", TimeFormat::Nanoseconds),
            ("nsec", TimeFormat::Nanoseconds),
            ("nsecs", TimeFormat::Nanoseconds),
        ] {
            let args = Args::try_parse_from(["mcap", "--time-format", value, "info", "demo.mcap"])
                .unwrap_or_else(|_| panic!("time format {value} should parse"));
            assert_eq!(args.time_format, expected);
        }
    }

    #[test]
    fn parses_global_verbosity_flag() {
        let args = Args::try_parse_from(["mcap", "-vv", "info", "demo.mcap"])
            .expect("verbosity should parse");
        assert_eq!(args.verbose, 2);
        assert_eq!(
            args.command,
            Command::Info(InfoCommand {
                file: "demo.mcap".into(),
            })
        );
    }

    #[test]
    fn parses_nested_list_subcommands() {
        let args = Args::try_parse_from(["mcap", "list", "channels", "demo.mcap"])
            .expect("list channels should parse");
        assert_eq!(
            args.command,
            Command::List(ListCommand {
                command: ListSubcommand::Channels(ListChannelsCommand {
                    file: "demo.mcap".into(),
                }),
            })
        );
    }

    #[test]
    fn parses_list_attachments_subcommand() {
        let args = Args::try_parse_from(["mcap", "list", "attachments", "demo.mcap"])
            .expect("list attachments should parse");
        assert_eq!(
            args.command,
            Command::List(ListCommand {
                command: ListSubcommand::Attachments(ListAttachmentsCommand {
                    file: "demo.mcap".into(),
                }),
            })
        );
    }

    #[test]
    fn parses_list_chunks_subcommand() {
        let args = Args::try_parse_from(["mcap", "list", "chunks", "demo.mcap"])
            .expect("list chunks should parse");
        assert_eq!(
            args.command,
            Command::List(ListCommand {
                command: ListSubcommand::Chunks(ListChunksCommand {
                    file: "demo.mcap".into(),
                }),
            })
        );
    }

    #[test]
    fn parses_list_metadata_subcommand() {
        let args = Args::try_parse_from(["mcap", "list", "metadata", "demo.mcap"])
            .expect("list metadata should parse");
        assert_eq!(
            args.command,
            Command::List(ListCommand {
                command: ListSubcommand::Metadata(ListMetadataCommand {
                    file: "demo.mcap".into(),
                }),
            })
        );
    }

    #[test]
    fn parses_list_schemas_subcommand() {
        let args = Args::try_parse_from(["mcap", "list", "schemas", "demo.mcap"])
            .expect("list schemas should parse");
        assert_eq!(
            args.command,
            Command::List(ListCommand {
                command: ListSubcommand::Schemas(ListSchemasCommand {
                    file: "demo.mcap".into(),
                }),
            })
        );
    }

    #[test]
    fn parses_nested_get_subcommands() {
        let args = Args::try_parse_from(["mcap", "get", "attachment", "demo.mcap", "--name", "a"])
            .expect("get attachment should parse");
        assert_eq!(
            args.command,
            Command::Get(GetCommand {
                command: GetSubcommand::Attachment(GetAttachmentCommand {
                    file: "demo.mcap".into(),
                    name: "a".to_string(),
                    offset: None,
                    output: None,
                }),
            })
        );
    }

    #[test]
    fn parses_get_metadata_subcommand() {
        let args = Args::try_parse_from(["mcap", "get", "metadata", "demo.mcap", "--name", "cfg"])
            .expect("get metadata should parse");
        assert_eq!(
            args.command,
            Command::Get(GetCommand {
                command: GetSubcommand::Metadata(GetMetadataCommand {
                    file: "demo.mcap".into(),
                    name: "cfg".to_string(),
                }),
            })
        );
    }

    #[test]
    fn parses_nested_add_subcommands() {
        let args = Args::try_parse_from([
            "mcap",
            "add",
            "metadata",
            "demo.mcap",
            "--name",
            "robot",
            "-k",
            "foo=bar",
        ])
        .expect("add metadata should parse");
        assert_eq!(
            args.command,
            Command::Add(AddCommand {
                command: AddSubcommand::Metadata(AddMetadataCommand {
                    file: "demo.mcap".into(),
                    name: "robot".to_string(),
                    key_values: vec!["foo=bar".to_string()],
                }),
            })
        );
    }

    #[test]
    fn parses_add_attachment_subcommand() {
        let args = Args::try_parse_from([
            "mcap",
            "add",
            "attachment",
            "demo.mcap",
            "-f",
            "payload.bin",
            "-n",
            "payload",
            "--content-type",
            "application/octet-stream",
            "--log-time",
            "100",
            "--creation-time",
            "99",
        ])
        .expect("add attachment should parse");
        assert_eq!(
            args.command,
            Command::Add(AddCommand {
                command: AddSubcommand::Attachment(AddAttachmentCommand {
                    file: "demo.mcap".into(),
                    attachment_file: "payload.bin".into(),
                    name: Some("payload".to_string()),
                    content_type: "application/octet-stream".to_string(),
                    log_time: Some("100".to_string()),
                    creation_time: Some("99".to_string()),
                }),
            })
        );
    }

    #[test]
    fn parses_du_subcommand() {
        let args = Args::try_parse_from(["mcap", "du", "demo.mcap"]).expect("du should parse");
        assert_eq!(
            args.command,
            Command::Du(DuCommand {
                approximate: false,
                file: "demo.mcap".into(),
            })
        );
    }

    #[test]
    fn parses_du_approximate_subcommand() {
        let args = Args::try_parse_from(["mcap", "du", "--approximate", "demo.mcap"])
            .expect("du --approximate should parse");
        assert_eq!(
            args.command,
            Command::Du(DuCommand {
                approximate: true,
                file: "demo.mcap".into(),
            })
        );
    }

    #[test]
    fn parses_convert_with_defaults() {
        let args = Args::try_parse_from(["mcap", "convert", "input.bag", "output.mcap"])
            .expect("convert should parse");
        assert_eq!(
            args.command,
            Command::Convert(ConvertCommand {
                input: "input.bag".into(),
                output: "output.mcap".into(),
                compression: CompressionFormat::Zstd,
                chunk_size: mcap::WriteOptions::DEFAULT_CHUNK_SIZE,
                no_crc: false,
                no_chunks: false,
            })
        );
    }

    #[test]
    fn parses_convert_with_all_flags() {
        let args = Args::try_parse_from([
            "mcap",
            "convert",
            "input.bag",
            "output.mcap",
            "--compression",
            "none",
            "--chunk-size",
            "1024",
            "--no-crc",
            "--no-chunks",
        ])
        .expect("convert with flags should parse");
        assert_eq!(
            args.command,
            Command::Convert(ConvertCommand {
                input: "input.bag".into(),
                output: "output.mcap".into(),
                compression: CompressionFormat::None,
                chunk_size: 1024,
                no_crc: true,
                no_chunks: true,
            })
        );
    }

    #[test]
    fn parses_doctor_subcommand() {
        let args =
            Args::try_parse_from(["mcap", "doctor", "demo.mcap"]).expect("doctor should parse");
        assert_eq!(
            args.command,
            Command::Doctor(DoctorCommand {
                strict_message_order: false,
                file: "demo.mcap".into(),
            })
        );
    }

    #[test]
    fn parses_doctor_with_strict_message_order_flag() {
        let args = Args::try_parse_from(["mcap", "doctor", "--strict-message-order", "demo.mcap"])
            .expect("doctor --strict-message-order should parse");
        assert_eq!(
            args.command,
            Command::Doctor(DoctorCommand {
                strict_message_order: true,
                file: "demo.mcap".into(),
            })
        );
    }

    #[test]
    fn parses_compress_with_defaults() {
        let args =
            Args::try_parse_from(["mcap", "compress", "in.mcap"]).expect("compress should parse");
        assert_eq!(
            args.command,
            Command::Compress(CompressCommand {
                common: CommonRewriteArgs {
                    file: Some("in.mcap".into()),
                    output: None,
                    output_file: None,
                    chunk_size: mcap::WriteOptions::DEFAULT_CHUNK_SIZE,
                    no_crc: false,
                },
                compression: CompressionFormat::Zstd,
                order: MessageOrder::Preserve,
            })
        );
    }

    #[test]
    fn parses_compress_with_all_flags() {
        let args = Args::try_parse_from([
            "mcap",
            "compress",
            "in.mcap",
            "--output",
            "out.mcap",
            "--chunk-size",
            "1024",
            "--compression",
            "lz4",
            "--no-crc",
            "--order",
            "log-time",
        ])
        .expect("compress with flags should parse");
        assert_eq!(
            args.command,
            Command::Compress(CompressCommand {
                common: CommonRewriteArgs {
                    file: Some("in.mcap".into()),
                    output: Some("out.mcap".into()),
                    output_file: None,
                    chunk_size: 1024,
                    no_crc: true,
                },
                compression: CompressionFormat::Lz4,
                order: MessageOrder::LogTime,
            })
        );
    }

    #[test]
    fn rejects_compress_invalid_compression() {
        let err = Args::try_parse_from(["mcap", "compress", "in.mcap", "--compression", "invalid"])
            .expect_err("invalid compression should be rejected at parse time");
        assert_eq!(err.kind(), clap::error::ErrorKind::InvalidValue);
    }

    #[test]
    fn parses_decompress_with_defaults() {
        let args = Args::try_parse_from(["mcap", "decompress", "in.mcap"])
            .expect("decompress should parse");
        assert_eq!(
            args.command,
            Command::Decompress(DecompressCommand {
                common: CommonRewriteArgs {
                    file: Some("in.mcap".into()),
                    output: None,
                    output_file: None,
                    chunk_size: mcap::WriteOptions::DEFAULT_CHUNK_SIZE,
                    no_crc: false,
                },
                order: MessageOrder::Preserve,
            })
        );
    }

    #[test]
    fn parses_decompress_with_flags() {
        let args = Args::try_parse_from([
            "mcap",
            "decompress",
            "in.mcap",
            "--output",
            "out.mcap",
            "--chunk-size",
            "2048",
            "--no-crc",
            "--order",
            "log-time",
        ])
        .expect("decompress with flags should parse");
        assert_eq!(
            args.command,
            Command::Decompress(DecompressCommand {
                common: CommonRewriteArgs {
                    file: Some("in.mcap".into()),
                    output: Some("out.mcap".into()),
                    output_file: None,
                    chunk_size: 2048,
                    no_crc: true,
                },
                order: MessageOrder::LogTime,
            })
        );
    }

    #[test]
    fn parses_filter_subcommand_with_flags() {
        let args = Args::try_parse_from([
            "mcap",
            "filter",
            "in.mcap",
            "-o",
            "out.mcap",
            "-y",
            "camera.*",
            "-l",
            "camera_.*",
            "-S",
            "100",
            "-E",
            "200",
            "--include-metadata",
            "--include-attachments",
            "--compression",
            "lz4",
            "--chunk-size",
            "2048",
            "--no-crc",
            "--no-chunks",
        ])
        .expect("filter should parse");
        assert_eq!(
            args.command,
            Command::Filter(FilterCommand {
                common: CommonRewriteArgs {
                    file: Some("in.mcap".into()),
                    output: Some("out.mcap".into()),
                    output_file: None,
                    chunk_size: 2048,
                    no_crc: true,
                },
                include_topic_regex: vec!["camera.*".to_string()],
                exclude_topic_regex: vec![],
                last_per_channel_topic_regex: vec!["camera_.*".to_string()],
                start: Some("100".to_string()),
                start_secs: 0,
                start_nsecs: 0,
                end: Some("200".to_string()),
                end_secs: 0,
                end_nsecs: 0,
                exclude_metadata: false,
                exclude_attachments: false,
                include_metadata: true,
                include_attachments: true,
                compression: Some(CompressionFormat::Lz4),
                output_compression: None,
                no_chunks: true,
                order: MessageOrder::Preserve,
            })
        );
    }

    #[test]
    fn parses_filter_deprecated_output_compression_alias() {
        let args =
            Args::try_parse_from(["mcap", "filter", "in.mcap", "--output-compression", "none"])
                .expect("filter should parse");
        match args.command {
            Command::Filter(filter) => {
                // The deprecated alias is captured separately and leaves --compression unset.
                assert_eq!(filter.compression, None);
                assert_eq!(filter.output_compression, Some(CompressionFormat::None));
            }
            other => panic!("expected filter command, got {other:?}"),
        }
    }

    #[test]
    fn parses_filter_exclude_metadata_and_attachments() {
        let args = Args::try_parse_from([
            "mcap",
            "filter",
            "in.mcap",
            "--exclude-metadata",
            "--exclude-attachments",
        ])
        .expect("filter should parse");
        match args.command {
            Command::Filter(filter) => {
                assert!(filter.exclude_metadata);
                assert!(filter.exclude_attachments);
                // Deprecated include flags default off and are no-ops.
                assert!(!filter.include_metadata);
                assert!(!filter.include_attachments);
            }
            other => panic!("expected filter command, got {other:?}"),
        }
    }

    #[test]
    fn filter_order_defaults_to_preserve_and_parses_log_time() {
        // Each rewrite command owns its `--order` (so its default shows in --help): filter defaults
        // to preserve; sort defaults to log_time. The flag name and values are identical across
        // commands.
        let default = Args::try_parse_from(["mcap", "filter", "in.mcap"])
            .expect("filter should parse without --order");
        match default.command {
            Command::Filter(filter) => assert_eq!(filter.order, MessageOrder::Preserve),
            other => panic!("expected filter command, got {other:?}"),
        }

        for (value, expected) in [
            ("log_time", MessageOrder::LogTime),
            ("log-time", MessageOrder::LogTime),
            ("topic", MessageOrder::Topic),
        ] {
            let args = Args::try_parse_from(["mcap", "filter", "in.mcap", "--order", value])
                .unwrap_or_else(|_| panic!("filter should parse --order {value}"));
            match args.command {
                Command::Filter(filter) => assert_eq!(filter.order, expected),
                other => panic!("expected filter command, got {other:?}"),
            }
        }

        assert!(
            Args::try_parse_from(["mcap", "filter", "in.mcap", "--order", "bogus"]).is_err(),
            "an unknown --order value should be rejected"
        );
    }

    #[test]
    fn parses_recover_with_defaults() {
        let args =
            Args::try_parse_from(["mcap", "recover", "input.mcap"]).expect("recover should parse");
        assert_eq!(
            args.command,
            Command::Recover(RecoverCommand {
                file: Some("input.mcap".into()),
                output: None,
                chunk_size: mcap::WriteOptions::DEFAULT_CHUNK_SIZE,
                compression: "preserve".to_string(),
            })
        );
    }

    #[test]
    fn parses_sort_with_defaults() {
        let args = Args::try_parse_from(["mcap", "sort", "in.mcap", "-o", "out.mcap"])
            .expect("sort should parse");
        assert_eq!(
            args.command,
            Command::Sort(SortCommand {
                common: CommonRewriteArgs {
                    file: Some("in.mcap".into()),
                    output: Some("out.mcap".into()),
                    output_file: None,
                    chunk_size: mcap::WriteOptions::DEFAULT_CHUNK_SIZE,
                    no_crc: false,
                },
                compression: CompressionFormat::Zstd,
                no_chunks: false,
                // `sort` defaults --order to log_time; the copy commands default to preserve.
                order: MessageOrder::LogTime,
            })
        );
    }

    #[test]
    fn parses_sort_output_file_deprecated_alias() {
        // `--output-file` is retained as a hidden deprecated alias for `--output` on every rewrite
        // command; it parses into the separate field so the handler can warn.
        let args = Args::try_parse_from(["mcap", "sort", "in.mcap", "--output-file", "out.mcap"])
            .expect("deprecated --output-file should still parse");
        assert_eq!(
            args.command,
            Command::Sort(SortCommand {
                common: CommonRewriteArgs {
                    file: Some("in.mcap".into()),
                    output: None,
                    output_file: Some("out.mcap".into()),
                    chunk_size: mcap::WriteOptions::DEFAULT_CHUNK_SIZE,
                    no_crc: false,
                },
                compression: CompressionFormat::Zstd,
                no_chunks: false,
                order: MessageOrder::LogTime,
            })
        );
    }

    #[test]
    fn parses_recover_with_all_flags() {
        let args = Args::try_parse_from([
            "mcap",
            "recover",
            "input.mcap",
            "-o",
            "out.mcap",
            "--chunk-size",
            "2048",
            "--compression",
            "none",
        ])
        .expect("recover with flags should parse");
        assert_eq!(
            args.command,
            Command::Recover(RecoverCommand {
                file: Some("input.mcap".into()),
                output: Some("out.mcap".into()),
                chunk_size: 2048,
                compression: "none".to_string(),
            })
        );
    }

    #[test]
    fn parses_sort_with_all_flags() {
        let args = Args::try_parse_from([
            "mcap",
            "sort",
            "in.mcap",
            "-o",
            "out.mcap",
            "--compression",
            "none",
            "--chunk-size",
            "1024",
            "--no-crc",
            "--no-chunks",
            "--order",
            "preserve",
        ])
        .expect("sort with flags should parse");
        assert_eq!(
            args.command,
            Command::Sort(SortCommand {
                common: CommonRewriteArgs {
                    file: Some("in.mcap".into()),
                    output: Some("out.mcap".into()),
                    output_file: None,
                    chunk_size: 1024,
                    no_crc: true,
                },
                compression: CompressionFormat::None,
                no_chunks: true,
                // `--order` is a real flag on `sort`, so it can be overridden rather than being
                // locked to its log_time default.
                order: MessageOrder::Preserve,
            })
        );
    }

    #[test]
    fn parses_merge_with_defaults() {
        let args = Args::try_parse_from(["mcap", "merge", "a.mcap", "b.mcap"])
            .expect("merge should parse");
        assert_eq!(
            args.command,
            Command::Merge(MergeCommand {
                files: vec!["a.mcap".into(), "b.mcap".into()],
                output: None,
                output_file: None,
                compression: CompressionFormat::Zstd,
                chunk_size: mcap::WriteOptions::DEFAULT_CHUNK_SIZE,
                no_crc: false,
                no_chunks: false,
                allow_duplicate_metadata: false,
                coalesce_channels: CoalesceChannels::Auto,
            })
        );
    }

    #[test]
    fn parses_merge_with_all_flags() {
        let args = Args::try_parse_from([
            "mcap",
            "merge",
            "a.mcap",
            "b.mcap",
            "-o",
            "out.mcap",
            "--compression",
            "none",
            "--chunk-size",
            "2048",
            "--no-crc",
            "--no-chunks",
            "--allow-duplicate-metadata",
            "--coalesce-channels",
            "force",
        ])
        .expect("merge with flags should parse");
        assert_eq!(
            args.command,
            Command::Merge(MergeCommand {
                files: vec!["a.mcap".into(), "b.mcap".into()],
                // `-o` is the canonical `--output`; `--output-file` is the deprecated alias.
                output: Some("out.mcap".into()),
                output_file: None,
                compression: CompressionFormat::None,
                chunk_size: 2048,
                no_crc: true,
                no_chunks: true,
                allow_duplicate_metadata: true,
                coalesce_channels: CoalesceChannels::Force,
            })
        );
    }

    #[test]
    fn parses_merge_deprecated_output_file_alias() {
        // The deprecated `--output-file` parses into its own field; the handler resolves it and
        // warns.
        let args = Args::try_parse_from(["mcap", "merge", "a.mcap", "--output-file", "out.mcap"])
            .expect("merge with --output-file should parse");
        let Command::Merge(merge) = args.command else {
            panic!("expected a merge command");
        };
        assert_eq!(merge.output, None);
        assert_eq!(merge.output_file, Some("out.mcap".into()));
    }

    #[test]
    fn merge_requires_at_least_one_file() {
        Args::try_parse_from(["mcap", "merge"]).expect_err("merge requires at least one file");
    }

    #[test]
    fn merge_requires_file_even_when_flags_are_present() {
        Args::try_parse_from(["mcap", "merge", "--compression", "zstd"])
            .expect_err("merge should require at least one file");
    }

    #[test]
    fn parses_sort_without_output_streams_to_stdout() {
        // `sort` now shares the rewrite args, so (like filter/compress/decompress) omitting the
        // output is allowed and writes to stdout rather than being a required flag.
        let args = Args::try_parse_from(["mcap", "sort", "in.mcap"]).expect("sort should parse");
        let Command::Sort(sort) = args.command else {
            panic!("expected a sort command");
        };
        assert_eq!(sort.common.file, Some("in.mcap".into()));
        assert_eq!(sort.common.output, None);
        assert_eq!(sort.common.output_file, None);
    }
}
