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
        AddCommand, AddSubcommand, Args, Command, GetCommand, GetSubcommand, InfoCommand,
        ListChannelsCommand, ListCommand, ListSubcommand, VersionCommand,
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
}
