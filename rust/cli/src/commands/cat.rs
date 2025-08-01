use anyhow::Result;
use clap::Args;
use std::path::PathBuf;

#[derive(Args)]
pub struct CatArgs {
    /// MCAP files to read
    pub files: Vec<PathBuf>,

    /// Output file (default: stdout)
    #[arg(short, long)]
    pub output: Option<PathBuf>,

    /// Topics to include (regex)
    #[arg(long)]
    pub topics: Option<String>,

    /// Start time (nanoseconds or RFC3339)
    #[arg(long)]
    pub start: Option<String>,

    /// End time (nanoseconds or RFC3339)
    #[arg(long)]
    pub end: Option<String>,

    /// Output as JSON
    #[arg(long)]
    pub json: bool,
}

pub async fn run(args: CatArgs) -> Result<()> {
    anyhow::bail!(
        "cat command not yet implemented for files: {:?}",
        args.files
    );
}
