use clap::{Parser, Subcommand};

use crate::info::print_info;

#[derive(Subcommand)]
enum Command {
    /// Report statistics about an MCAP file
    Info {
        /// The path to the MCAP file to report statistics on.
        ///
        /// This can either be a local file, or a file in Google Cloud Storage if prefixed with
        /// `gs://`.
        path: String,
    },
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    cmd: Command,
}

/// Parse the CLI arguments and run the CLI.
pub async fn run() -> Result<(), String> {
    let Args { cmd } = Args::parse();

    match cmd {
        Command::Info { path } => print_info(path.clone())
            .await
            .map_err(|e| format!("Failed to read MCAP file '{path}': {e}")),
    }
}
