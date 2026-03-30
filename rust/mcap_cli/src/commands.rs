mod add_attachment;
mod add_metadata;
mod cat;
mod common;
mod compress;
mod convert;
mod decompress;
mod doctor;
mod du;
mod filter;
mod get_attachment;
mod get_metadata;
mod info;
mod list_attachments;
mod list_channels;
mod list_chunks;
mod list_metadata;
mod list_schemas;
mod merge;
mod recover;
mod sort;
mod version;

use anyhow::Result;

use crate::cli::{AddSubcommand, Command, GetSubcommand, ListSubcommand};
use crate::context::CommandContext;

pub fn not_implemented(command_name: &str) -> anyhow::Error {
    anyhow::anyhow!("'{command_name}' is not implemented yet")
}

pub fn dispatch(ctx: &CommandContext, command: Command) -> Result<()> {
    match command {
        Command::Info(args) => info::run(ctx, args),
        Command::Version(args) => version::run(ctx, args),

        Command::Add(args) => match args.command {
            AddSubcommand::Attachment => add_attachment::run(ctx),
            AddSubcommand::Metadata => add_metadata::run(ctx),
        },
        Command::Get(args) => match args.command {
            GetSubcommand::Attachment(args) => get_attachment::run(ctx, args),
            GetSubcommand::Metadata(args) => get_metadata::run(ctx, args),
        },
        Command::List(args) => match args.command {
            ListSubcommand::Attachments(args) => list_attachments::run(ctx, args),
            ListSubcommand::Channels(args) => list_channels::run(ctx, args),
            ListSubcommand::Chunks(args) => list_chunks::run(ctx, args),
            ListSubcommand::Metadata(args) => list_metadata::run(ctx, args),
            ListSubcommand::Schemas(args) => list_schemas::run(ctx, args),
        },

        Command::Cat => cat::run(ctx),
        Command::Compress => compress::run(ctx),
        Command::Convert => convert::run(ctx),
        Command::Decompress => decompress::run(ctx),
        Command::Doctor => doctor::run(ctx),
        Command::Du => du::run(ctx),
        Command::Filter => filter::run(ctx),
        Command::Merge => merge::run(ctx),
        Command::Recover => recover::run(ctx),
        Command::Sort => sort::run(ctx),
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::dispatch;
    use crate::cli::{
        AddCommand, AddSubcommand, Command, GetAttachmentCommand, InfoCommand,
        ListAttachmentsCommand,
        ListChannelsCommand, ListChunksCommand, ListCommand, ListMetadataCommand,
        ListSchemasCommand, ListSubcommand,
    };
    use crate::context::CommandContext;

    #[test]
    fn info_requires_existing_file() {
        let err = dispatch(
            &CommandContext::default(),
            Command::Info(InfoCommand {
                file: PathBuf::from("does-not-exist.mcap"),
            }),
        )
        .expect_err("info should fail on missing file");
        assert!(err.to_string().contains("couldn't open"));
    }

    #[test]
    fn list_subcommands_require_existing_file() {
        let err = dispatch(
            &CommandContext::default(),
            Command::List(ListCommand {
                command: ListSubcommand::Channels(ListChannelsCommand {
                    file: PathBuf::from("does-not-exist.mcap"),
                }),
            }),
        )
        .expect_err("list channels should fail on missing file");
        assert!(err.to_string().contains("couldn't open"));
    }

    #[test]
    fn add_subcommands_stub_with_specific_names() {
        let err = dispatch(
            &CommandContext::default(),
            Command::Add(AddCommand {
                command: AddSubcommand::Attachment,
            }),
        )
        .expect_err("add attachment should be a stub");
        assert_eq!(err.to_string(), "'add attachment' is not implemented yet");
    }

    #[test]
    fn get_subcommands_require_existing_file() {
        let err = dispatch(
            &CommandContext::default(),
            Command::Get(crate::cli::GetCommand {
                command: crate::cli::GetSubcommand::Attachment(GetAttachmentCommand {
                    file: PathBuf::from("does-not-exist.mcap"),
                    name: "attachment.bin".to_string(),
                    offset: None,
                    output: None,
                }),
            }),
        )
        .expect_err("get attachment should fail on missing file");
        assert!(err.to_string().contains("couldn't open"));
    }

    #[test]
    fn list_all_subcommands_are_wired() {
        let ctx = CommandContext::default();
        for command in [
            ListSubcommand::Attachments(ListAttachmentsCommand {
                file: PathBuf::from("does-not-exist.mcap"),
            }),
            ListSubcommand::Channels(ListChannelsCommand {
                file: PathBuf::from("does-not-exist.mcap"),
            }),
            ListSubcommand::Chunks(ListChunksCommand {
                file: PathBuf::from("does-not-exist.mcap"),
            }),
            ListSubcommand::Metadata(ListMetadataCommand {
                file: PathBuf::from("does-not-exist.mcap"),
            }),
            ListSubcommand::Schemas(ListSchemasCommand {
                file: PathBuf::from("does-not-exist.mcap"),
            }),
        ] {
            let err = dispatch(&ctx, Command::List(ListCommand { command }))
                .expect_err("list command should fail on missing file");
            assert!(err.to_string().contains("couldn't open"));
        }
    }
}
