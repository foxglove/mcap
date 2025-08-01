use anyhow::Result;
use clap::Args;
use std::path::PathBuf;

#[derive(Args)]
pub struct AttachmentArgs {
    /// MCAP file to modify
    pub file: PathBuf,

    /// Attachment file to add
    pub attachment_file: PathBuf,

    /// Attachment name
    #[arg(long)]
    pub name: String,

    /// Attachment content type
    #[arg(long)]
    pub content_type: Option<String>,

    /// Output MCAP file
    #[arg(short, long)]
    pub output: PathBuf,
}

pub async fn run(args: AttachmentArgs) -> Result<()> {
    anyhow::bail!(
        "add attachment command not yet implemented for file: {:?}",
        args.file
    );
}
