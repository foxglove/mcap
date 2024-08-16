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

macro_rules! err_msg {
    ($($t:tt)*) => {
        |e| e.into_human_message(format!($($t)*))
    };
}

/// Parse the CLI arguments and run the CLI.
pub async fn run() -> Result<(), String> {
    let Args { cmd } = Args::parse();

    match cmd {
        Command::Info { path } => print_info(path.clone())
            .await
            .map_err(err_msg!("Failed to get info for MCAP file '{path}'")),
    }
}
