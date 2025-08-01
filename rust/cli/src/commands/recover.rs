use anyhow::Result;
use clap::Args;
use std::path::PathBuf;

#[derive(Args)]
pub struct RecoverArgs {
    /// Input MCAP file (possibly corrupted)
    pub input: PathBuf,

    /// Output MCAP file
    #[arg(short, long)]
    pub output: PathBuf,
}

pub async fn run(args: RecoverArgs) -> Result<()> {
    anyhow::bail!(
        "recover command not yet implemented for input: {:?}",
        args.input
    );
}
