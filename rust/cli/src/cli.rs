use clap::{Parser, Subcommand};

use crate::{error::CliResult, info::print_info};

#[derive(Subcommand)]
enum Command {
    /// Report statistics about an MCAP file
    Info {
        /// The path to the MCAP file to report statistics on
        path: String,
    },
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    cmd: Command,
}

pub async fn run() -> CliResult<()> {
    let Args { cmd } = Args::parse();

    match cmd {
        Command::Info { path } => print_info(path).await,
    }
}
