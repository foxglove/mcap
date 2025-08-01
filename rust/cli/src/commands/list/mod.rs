use crate::error::CliResult;
use clap::Subcommand;

pub mod attachments;
pub mod channels;
pub mod chunks;
pub mod metadata;
pub mod schemas;

#[derive(Subcommand)]
pub enum ListCommands {
    /// List channels in an MCAP file
    Channels(channels::ChannelsArgs),

    /// List schemas in an MCAP file
    Schemas(schemas::SchemasArgs),

    /// List attachments in an MCAP file
    Attachments(attachments::AttachmentsArgs),

    /// List metadata in an MCAP file
    Metadata(metadata::MetadataArgs),

    /// List chunks in an MCAP file
    Chunks(chunks::ChunksArgs),
}

pub async fn execute(command: ListCommands) -> CliResult<()> {
    match command {
        ListCommands::Channels(args) => channels::execute(args).await,
        ListCommands::Schemas(args) => schemas::execute(args).await,
        ListCommands::Attachments(args) => attachments::execute(args).await,
        ListCommands::Metadata(args) => metadata::execute(args).await,
        ListCommands::Chunks(args) => chunks::execute(args).await,
    }
}
