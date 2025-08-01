use anyhow::Result;
use clap::Args;
use std::path::PathBuf;

#[derive(Args)]
pub struct ChannelsArgs {
    /// MCAP file to analyze
    pub file: PathBuf,

    /// Output as JSON
    #[arg(long)]
    pub json: bool,
}

pub async fn run(args: ChannelsArgs) -> Result<()> {
    anyhow::bail!(
        "list channels command not yet implemented for file: {:?}",
        args.file
    );
}
