mod cli;
mod cli_io;
mod commands;
mod logsetup;
mod output;
mod time;

use std::process;

use anyhow::Result;
use clap::Parser;

fn run() -> Result<()> {
    let args = cli::Args::parse();
    logsetup::init_logger(args.verbose, args.color);

    commands::dispatch(args.command)
}

fn main() {
    run().unwrap_or_else(|e| {
        eprintln!("Error: {e:#}");
        process::exit(1);
    });
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use clap::Parser;

    use crate::cli::{
        AddCommand, AddMetadataArgs, AddSubcommand, Args, Command, GetAttachmentArgs, GetCommand,
        GetSubcommand, ListCommand, ListSubcommand, VersionCommand,
    };

    #[test]
    fn parses_info_subcommand() {
        let args = Args::try_parse_from(["mcap", "info", "demo.mcap"]).expect("info should parse");
        assert_eq!(
            args.command,
            Command::Info(crate::cli::InputFile {
                file: PathBuf::from("demo.mcap"),
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
            Command::Info(crate::cli::InputFile {
                file: PathBuf::from("demo.mcap"),
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
                command: ListSubcommand::Channels(crate::cli::InputFile {
                    file: PathBuf::from("demo.mcap"),
                }),
            })
        );
    }

    #[test]
    fn parses_nested_get_subcommands() {
        let args =
            Args::try_parse_from(["mcap", "get", "attachment", "demo.mcap", "--name", "foo"])
                .expect("get attachment should parse");
        assert_eq!(
            args.command,
            Command::Get(GetCommand {
                command: GetSubcommand::Attachment(GetAttachmentArgs {
                    file: PathBuf::from("demo.mcap"),
                    name: "foo".to_string(),
                    offset: None,
                    output: None,
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
            "meta",
            "--key",
            "foo=bar",
        ])
        .expect("add metadata should parse");
        assert_eq!(
            args.command,
            Command::Add(AddCommand {
                command: AddSubcommand::Metadata(AddMetadataArgs {
                    file: PathBuf::from("demo.mcap"),
                    name: "meta".to_string(),
                    key_values: vec!["foo=bar".to_string()],
                }),
            })
        );
    }
}
