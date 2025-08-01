use anyhow::Result;
use clap::Args;
use std::path::PathBuf;

#[derive(Args)]
pub struct DuArgs {
    /// MCAP file to analyze
    pub file: PathBuf,

    /// Show detailed breakdown
    #[arg(short, long)]
    pub detailed: bool,
}

pub async fn run(args: DuArgs) -> Result<()> {
    anyhow::bail!("du command not yet implemented for file: {:?}", args.file);
}
