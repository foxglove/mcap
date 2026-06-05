mod cli;
mod commands;
mod context;
mod logsetup;
mod parse;
mod render;
mod source;

use std::process::ExitCode;

use anyhow::Result;
use clap::Parser;
use commands::CommandOutcome;
use context::CommandContext;

fn run() -> Result<CommandOutcome> {
    let args = cli::Args::parse();
    logsetup::init_logger(args.verbose, args.color)?;
    let ctx = CommandContext::new(args.verbose, args.color, args.allow_remote_scan);

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
        CoalesceChannels, Command, CompletionCommand, CompressCommand, CompressionFormat,
        ConvertCommand, DecompressCommand, DoctorCommand, DuCommand, FilterCommand,
        GetAttachmentCommand, GetCommand, GetMetadataCommand, GetSubcommand, InfoCommand,
        ListAttachmentsCommand, ListChannelsCommand, ListChunksCommand, ListCommand,
        ListMetadataCommand, ListSchemasCommand, ListSubcommand, MergeCommand, RecoverCommand,
        SortCommand,
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
            "--json",
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
                json: true,
            })
        );
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
    fn convert_rejects_removed_writer_bool_flags() {
        Args::try_parse_from([
            "mcap",
            "convert",
            "input.bag",
            "output.mcap",
            "--include-crc",
        ])
        .expect_err("convert should reject removed --include-crc flag");

        Args::try_parse_from([
            "mcap",
            "convert",
            "input.bag",
            "output.mcap",
            "--chunked=false",
        ])
        .expect_err("convert should reject removed --chunked flag");
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
                file: Some("in.mcap".into()),
                output: None,
                chunk_size: mcap::WriteOptions::DEFAULT_CHUNK_SIZE,
                compression: "zstd".to_string(),
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
        ])
        .expect("compress with flags should parse");
        assert_eq!(
            args.command,
            Command::Compress(CompressCommand {
                file: Some("in.mcap".into()),
                output: Some("out.mcap".into()),
                chunk_size: 1024,
                compression: "lz4".to_string(),
            })
        );
    }

    #[test]
    fn compress_rejects_unchunked_flag() {
        Args::try_parse_from(["mcap", "compress", "in.mcap", "--unchunked"])
            .expect_err("compress should reject removed --unchunked flag");
    }

    #[test]
    fn parses_decompress_with_defaults() {
        let args = Args::try_parse_from(["mcap", "decompress", "in.mcap"])
            .expect("decompress should parse");
        assert_eq!(
            args.command,
            Command::Decompress(DecompressCommand {
                file: Some("in.mcap".into()),
                output: None,
                chunk_size: mcap::WriteOptions::DEFAULT_CHUNK_SIZE,
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
        ])
        .expect("decompress with flags should parse");
        assert_eq!(
            args.command,
            Command::Decompress(DecompressCommand {
                file: Some("in.mcap".into()),
                output: Some("out.mcap".into()),
                chunk_size: 2048,
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
            "--output-compression",
            "lz4",
            "--chunk-size",
            "2048",
        ])
        .expect("filter should parse");
        assert_eq!(
            args.command,
            Command::Filter(FilterCommand {
                file: Some("in.mcap".into()),
                output: Some("out.mcap".into()),
                include_topic_regex: vec!["camera.*".to_string()],
                exclude_topic_regex: vec![],
                last_per_channel_topic_regex: vec!["camera_.*".to_string()],
                start: Some("100".to_string()),
                start_secs: 0,
                start_nsecs: 0,
                end: Some("200".to_string()),
                end_secs: 0,
                end_nsecs: 0,
                include_metadata: true,
                include_attachments: true,
                output_compression: "lz4".to_string(),
                chunk_size: 2048,
            })
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
                file: "in.mcap".into(),
                output_file: "out.mcap".into(),
                chunk_size: mcap::WriteOptions::DEFAULT_CHUNK_SIZE,
                compression: CompressionFormat::Zstd,
                no_crc: false,
                no_chunks: false,
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
        ])
        .expect("sort with flags should parse");
        assert_eq!(
            args.command,
            Command::Sort(SortCommand {
                file: "in.mcap".into(),
                output_file: "out.mcap".into(),
                chunk_size: 1024,
                compression: CompressionFormat::None,
                no_crc: true,
                no_chunks: true,
            })
        );
    }

    #[test]
    fn sort_rejects_removed_writer_bool_flags() {
        Args::try_parse_from(["mcap", "sort", "in.mcap", "-o", "out.mcap", "--include-crc"])
            .expect_err("sort should reject removed --include-crc flag");

        Args::try_parse_from([
            "mcap",
            "sort",
            "in.mcap",
            "-o",
            "out.mcap",
            "--chunked=false",
        ])
        .expect_err("sort should reject removed --chunked flag");
    }

    #[test]
    fn parses_merge_with_defaults() {
        let args = Args::try_parse_from(["mcap", "merge", "a.mcap", "b.mcap"])
            .expect("merge should parse");
        assert_eq!(
            args.command,
            Command::Merge(MergeCommand {
                files: vec!["a.mcap".into(), "b.mcap".into()],
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
                output_file: Some("out.mcap".into()),
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
    fn merge_rejects_removed_writer_bool_flags() {
        Args::try_parse_from(["mcap", "merge", "a.mcap", "b.mcap", "--include-crc"])
            .expect_err("merge should reject removed --include-crc flag");

        Args::try_parse_from(["mcap", "merge", "a.mcap", "b.mcap", "--chunked=false"])
            .expect_err("merge should reject removed --chunked flag");
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
    fn sort_requires_output_file() {
        Args::try_parse_from(["mcap", "sort", "in.mcap"]).expect_err("sort requires --output-file");
    }
}
