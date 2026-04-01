mod cli;
mod commands;
mod context;
mod logsetup;

use std::process;

use anyhow::Result;
use clap::Parser;
use context::CommandContext;

fn run() -> Result<()> {
    let args = cli::Args::parse();
    logsetup::init_logger(args.verbose, args.color)?;
    if args.config.is_some() {
        anyhow::bail!("'--config' is not implemented yet");
    }
    if args.pprof_profile {
        anyhow::bail!("'--pprof-profile' is not implemented yet");
    }
    let ctx = CommandContext::new(args.verbose, args.color, args.config, args.pprof_profile);

    commands::dispatch(&ctx, args.command)
}

fn main() {
    run().unwrap_or_else(|e| {
        eprintln!("Error: {e:#}");
        process::exit(1);
    });
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use crate::cli::{
        AddAttachmentCommand, AddCommand, AddMetadataCommand, AddSubcommand, Args, CatCommand,
        Command, ConvertCommand, ConvertCompression, DoctorCommand, DuCommand, FilterCommand,
        GetAttachmentCommand, GetCommand, GetMetadataCommand, GetSubcommand, InfoCommand,
        ListAttachmentsCommand, ListChannelsCommand, ListChunksCommand, ListCommand,
        ListMetadataCommand, ListSchemasCommand, ListSubcommand, VersionCommand,
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
    fn parses_version_subcommand() {
        let args = Args::try_parse_from(["mcap", "version"]).expect("version should parse");
        assert_eq!(
            args.command,
            Command::Version(VersionCommand { library: false })
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
            })
        );
    }

    #[test]
    fn cat_requires_at_least_one_file() {
        Args::try_parse_from(["mcap", "cat"]).expect_err("cat requires at least one file");
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
                compression: ConvertCompression::Zstd,
                chunk_size: 8 * 1024 * 1024,
                include_crc: true,
                chunked: true,
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
            "--include-crc=false",
            "--chunked=false",
        ])
        .expect("convert with flags should parse");
        assert_eq!(
            args.command,
            Command::Convert(ConvertCommand {
                input: "input.bag".into(),
                output: "output.mcap".into(),
                compression: ConvertCompression::None,
                chunk_size: 1024,
                include_crc: false,
                chunked: false,
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
}
