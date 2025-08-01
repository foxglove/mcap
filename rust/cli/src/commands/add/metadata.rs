use anyhow::Result;
use clap::Args;
use std::path::PathBuf;

#[derive(Args)]
pub struct MetadataArgs {
    /// MCAP file to modify
    pub file: PathBuf,

    /// Metadata name
    #[arg(long)]
    pub name: String,

    /// Metadata value
    #[arg(long)]
    pub value: String,

    /// Output MCAP file
    #[arg(short, long)]
    pub output: PathBuf,
}

pub async fn run(args: MetadataArgs) -> Result<()> {
    anyhow::bail!(
        "add metadata command not yet implemented for file: {:?}",
        args.file
    );
}
