use crate::error::CliResult;
use clap::Subcommand;

pub mod attachment;
pub mod metadata;

#[derive(Subcommand)]
pub enum GetCommands {
    /// Extract an attachment from an MCAP file
    Attachment(attachment::GetAttachmentArgs),

    /// Extract metadata from an MCAP file
    Metadata(metadata::GetMetadataArgs),
}

/// Execute the given get subcommand
pub async fn execute(command: GetCommands) -> CliResult<()> {
    match command {
        GetCommands::Attachment(args) => attachment::execute(args).await,
        GetCommands::Metadata(args) => metadata::execute(args).await,
    }
}
