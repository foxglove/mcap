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
        AddCommand, AddSubcommand, Args, Command, ConvertCommand, ConvertCompression, GetCommand,
        GetSubcommand, InfoCommand, ListAttachmentsCommand, ListChannelsCommand,
        ListChunksCommand, ListCommand, ListMetadataCommand, ListSchemasCommand, ListSubcommand,
        VersionCommand,
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
        let args = Args::try_parse_from(["mcap", "get", "attachment"])
            .expect("get attachment should parse");
        assert_eq!(
            args.command,
            Command::Get(GetCommand {
                command: GetSubcommand::Attachment,
            })
        );
    }

    #[test]
    fn parses_nested_add_subcommands() {
        let args =
            Args::try_parse_from(["mcap", "add", "metadata"]).expect("add metadata should parse");
        assert_eq!(
            args.command,
            Command::Add(AddCommand {
                command: AddSubcommand::Metadata,
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
}
