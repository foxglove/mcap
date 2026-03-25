mod commands;
mod logsetup;

use std::process;

use anyhow::Result;
use clap::{Parser, Subcommand};
use log::error;

#[derive(Parser, Debug)]
#[clap(name = "mcap")]
struct Args {
    /// Verbosity (-v, -vv, -vvv, etc.)
    #[clap(short, long, parse(from_occurrences))]
    verbose: u8,

    #[clap(short, long, arg_enum, default_value = "auto")]
    color: logsetup::Color,

    #[clap(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug, PartialEq, Eq)]
enum Command {
    /// Show basic information about an MCAP file (stub)
    Info,
    /// Show CLI version information (stub)
    Version,
}

fn run() -> Result<()> {
    let args = Args::parse();
    logsetup::init_logger(args.verbose, args.color);

    match args.command {
        Command::Info => commands::run_info(),
        Command::Version => commands::run_version(),
    }
}

fn main() {
    run().unwrap_or_else(|e| {
        error!("{e:?}");
        process::exit(1);
    });
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::{Args, Command};

    #[test]
    fn parses_info_subcommand() {
        let args = Args::try_parse_from(["mcap", "info"]).expect("info should parse");
        assert_eq!(args.command, Command::Info);
    }

    #[test]
    fn parses_version_subcommand() {
        let args = Args::try_parse_from(["mcap", "version"]).expect("version should parse");
        assert_eq!(args.command, Command::Version);
    }

    #[test]
    fn requires_subcommand() {
        let parse_err = Args::try_parse_from(["mcap"]).expect_err("subcommand is required");
        assert_eq!(
            parse_err.kind(),
            clap::ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand
        );
    }

    #[test]
    fn parses_global_verbosity_flag() {
        let args = Args::try_parse_from(["mcap", "-vv", "info"]).expect("verbosity should parse");
        assert_eq!(args.verbose, 2);
        assert_eq!(args.command, Command::Info);
    }
}
