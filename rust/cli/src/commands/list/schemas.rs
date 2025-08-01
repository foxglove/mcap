use anyhow::Result;
use clap::Args;
use std::path::PathBuf;

#[derive(Args)]
pub struct SchemasArgs {
    /// MCAP file to analyze
    pub file: PathBuf,

    /// Output as JSON
    #[arg(long)]
    pub json: bool,
}

pub async fn run(args: SchemasArgs) -> Result<()> {
    anyhow::bail!(
        "list schemas command not yet implemented for file: {:?}",
        args.file
    );
}
