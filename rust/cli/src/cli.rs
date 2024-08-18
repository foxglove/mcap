use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};
use tracing::instrument;

use crate::{filter::filter_mcap, info::print_info};

#[derive(Debug, Clone, ValueEnum)]
pub enum OutputCompression {
    /// Compression using the Zstandard algorithm
    Zstd,
    /// Compression using the LZ4 algorithm
    Lz4,
    /// No compression
    None,
}

impl From<OutputCompression> for Option<mcap::Compression> {
    fn from(value: OutputCompression) -> Self {
        match value {
            OutputCompression::Zstd => Some(mcap::Compression::Zstd),
            OutputCompression::Lz4 => Some(mcap::Compression::Lz4),
            OutputCompression::None => None,
        }
    }
}

#[derive(Subcommand)]
enum Command {
    /// Report statistics about an MCAP file
    Info {
        /// Path to the MCAP file to report statistics on
        ///
        /// This can either be a local file, a URL, or a file in Google Cloud Storage prefixed with `gs://`.
        path: String,
    },

    /// Filter an MCAP file by topic and time range to a new file
    Filter(FilterArgs),
}

#[derive(Args)]
pub struct FilterArgs {
    /// Path to the MCAP file to filter
    ///
    /// This can either be a local file, a URL, or a file in Google Cloud Storage prefixed with `gs://`.
    pub path: String,

    /// Chunk size of the output file
    #[arg(long, default_value_t = 4194304)]
    pub chunk_size: u64,

    /// Messages with log times before nanosecond-precision timestamp will be included
    #[arg(short = 'E', long)]
    pub end_nsecs: Option<u64>,

    /// Messages with log times before timestamp will be included
    #[arg(short = 'e', long)]
    pub end_secs: Option<u64>,

    /// Messages with topic names matching this regex will be excluded, can be specified
    /// multiple times
    #[arg(short = 'n', long)]
    pub exclude_topic_regex: Vec<String>,

    /// Whether to include attachments in the output mcap
    #[arg(long)]
    pub include_attachments: bool,

    /// Whether to include metadata in the output mcap
    #[arg(long)]
    pub include_metadata: bool,

    /// Messages with topic names matching this regex will be included, can be supplied
    /// multiple times
    #[arg(short = 'y', long)]
    pub include_topic_regex: Vec<String>,

    /// Output filename
    #[arg(short = 'o', long)]
    pub output: PathBuf,

    /// Compression algorithm to use on output file
    #[arg(long, value_enum, default_value_t = OutputCompression::Zstd)]
    pub output_compression: OutputCompression,

    /// Messages with log times after or equal to this nanosecond-precision timestamp will be
    /// included
    #[arg(short = 'S', long)]
    pub start_nsecs: Option<u64>,

    /// Messages with log times after or equal to this timestamp will be included
    #[arg(short = 's', long)]
    pub start_secs: Option<u64>,
}

/// 🔪 Officially the top-rated CLI tool for slicing and dicing MCAP files.
///
#[derive(Parser)]
#[command(bin_name="mcap", author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    cmd: Command,
}

macro_rules! msg {
    ($($t:tt)*) => {
        |e| e.into_human_message(format!($($t)*))
    };
}

/// Parse the CLI arguments and run the CLI.
#[instrument(name = "mcap_cli::run")]
pub async fn run() -> Result<(), String> {
    let Cli { cmd } = Cli::parse();

    match cmd {
        Command::Info { path } => print_info(path.clone())
            .await
            .map_err(msg!("Failed to get info for MCAP file '{path}'")),
        Command::Filter(filter) => filter_mcap(filter)
            .await
            .map_err(msg!("Failed to filter MCAP file")),
    }
}
