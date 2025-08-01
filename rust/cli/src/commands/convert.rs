use anyhow::Result;
use clap::Args;
use std::path::PathBuf;

#[derive(Args)]
pub struct ConvertArgs {
    /// Input file
    pub input: PathBuf,

    /// Output file
    #[arg(short, long)]
    pub output: PathBuf,

    /// Output format
    #[arg(long)]
    pub format: Option<String>,
}

pub async fn run(args: ConvertArgs) -> Result<()> {
    anyhow::bail!(
        "convert command not yet implemented for input: {:?}",
        args.input
    );
}
