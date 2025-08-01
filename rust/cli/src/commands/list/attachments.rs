use anyhow::Result;
use clap::Args;
use std::path::PathBuf;

#[derive(Args)]
pub struct AttachmentsArgs {
    /// MCAP file to analyze
    pub file: PathBuf,

    /// Output as JSON
    #[arg(long)]
    pub json: bool,
}

pub async fn run(args: AttachmentsArgs) -> Result<()> {
    anyhow::bail!(
        "list attachments command not yet implemented for file: {:?}",
        args.file
    );
}
