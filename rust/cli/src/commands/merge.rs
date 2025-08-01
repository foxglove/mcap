use anyhow::Result;
use clap::Args;
use std::path::PathBuf;

#[derive(Args)]
pub struct MergeArgs {
    /// Input MCAP files to merge
    pub inputs: Vec<PathBuf>,

    /// Output MCAP file
    #[arg(short, long)]
    pub output: PathBuf,

    /// Compression algorithm
    #[arg(long)]
    pub compression: Option<String>,

    /// Chunk size in bytes
    #[arg(long)]
    pub chunk_size: Option<u64>,
}

pub async fn run(args: MergeArgs) -> Result<()> {
    anyhow::bail!(
        "merge command not yet implemented for inputs: {:?}",
        args.inputs
    );
}
