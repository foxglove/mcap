use anyhow::Result;
use clap::Args;
use std::path::PathBuf;

#[derive(Args)]
pub struct InfoArgs {
    /// MCAP file to analyze
    pub file: PathBuf,
}

pub async fn run(args: InfoArgs) -> Result<()> {
    anyhow::bail!("info command not yet implemented for file: {:?}", args.file);
}
