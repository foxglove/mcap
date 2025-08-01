use anyhow::Result;
use clap::Args;
use std::path::PathBuf;

#[derive(Args)]
pub struct CompressArgs {
    /// Input MCAP file
    pub input: PathBuf,

    /// Output MCAP file
    #[arg(short, long)]
    pub output: PathBuf,

    /// Compression algorithm (zstd, lz4, none)
    #[arg(long, default_value = "zstd")]
    pub compression: String,
}

#[derive(Args)]
pub struct DecompressArgs {
    /// Input MCAP file
    pub input: PathBuf,

    /// Output MCAP file
    #[arg(short, long)]
    pub output: PathBuf,
}

pub async fn compress(args: CompressArgs) -> Result<()> {
    anyhow::bail!(
        "compress command not yet implemented for input: {:?}",
        args.input
    );
}

pub async fn decompress(args: DecompressArgs) -> Result<()> {
    anyhow::bail!(
        "decompress command not yet implemented for input: {:?}",
        args.input
    );
}
