use anyhow::Result;
use clap::Args;
use std::path::PathBuf;

#[derive(Args)]
pub struct ChunksArgs {
    /// MCAP file to analyze
    pub file: PathBuf,

    /// Output as JSON
    #[arg(long)]
    pub json: bool,
}

pub async fn run(args: ChunksArgs) -> Result<()> {
    anyhow::bail!(
        "list chunks command not yet implemented for file: {:?}",
        args.file
    );
}
