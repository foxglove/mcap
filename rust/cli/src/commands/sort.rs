use anyhow::Result;
use clap::Args;
use std::path::PathBuf;

#[derive(Args)]
pub struct SortArgs {
    /// Input MCAP file
    pub input: PathBuf,

    /// Output MCAP file
    #[arg(short, long)]
    pub output: PathBuf,
}

pub async fn run(args: SortArgs) -> Result<()> {
    anyhow::bail!(
        "sort command not yet implemented for input: {:?}",
        args.input
    );
}
