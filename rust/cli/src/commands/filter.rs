use anyhow::Result;
use clap::Args;
use std::path::PathBuf;

#[derive(Args)]
pub struct FilterArgs {
    /// Input MCAP file
    pub input: PathBuf,

    /// Output MCAP file
    #[arg(short, long)]
    pub output: PathBuf,

    /// Include topics matching regex
    #[arg(short = 'y', long = "include-topic-regex")]
    pub include_topics: Vec<String>,

    /// Exclude topics matching regex
    #[arg(short = 'n', long = "exclude-topic-regex")]
    pub exclude_topics: Vec<String>,

    /// Start time (RFC3339 or nanoseconds)
    #[arg(long)]
    pub start: Option<String>,

    /// End time (RFC3339 or nanoseconds)
    #[arg(long)]
    pub end: Option<String>,
}

pub async fn run(args: FilterArgs) -> Result<()> {
    anyhow::bail!(
        "filter command not yet implemented for input: {:?}",
        args.input
    );
}
