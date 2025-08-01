use crate::error::CliResult;
use clap::Subcommand;

pub mod attachment;
pub mod metadata;

#[derive(Subcommand)]
pub enum AddCommands {
    /// Add an attachment to an MCAP file
    Attachment(attachment::AddAttachmentArgs),

    /// Add metadata to an MCAP file
    Metadata(metadata::AddMetadataArgs),
}

/// Execute the given add subcommand
pub async fn execute(command: AddCommands) -> CliResult<()> {
    match command {
        AddCommands::Attachment(args) => attachment::execute(args).await,
        AddCommands::Metadata(args) => metadata::execute(args).await,
    }
}
