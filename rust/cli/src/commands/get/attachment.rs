use anyhow::Result;
use clap::Args;
use std::path::PathBuf;

#[derive(Args)]
pub struct AttachmentArgs {
    /// MCAP file to read from
    pub file: PathBuf,

    /// Attachment name or index
    pub attachment: String,

    /// Output file (default: stdout)
    #[arg(short, long)]
    pub output: Option<PathBuf>,
}

pub async fn run(args: AttachmentArgs) -> Result<()> {
    anyhow::bail!(
        "get attachment command not yet implemented for file: {:?}",
        args.file
    );
}
