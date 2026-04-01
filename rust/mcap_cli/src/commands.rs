mod add_attachment;
mod add_common;
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
            AddSubcommand::Attachment(args) => add_attachment::run(ctx, args),
            AddSubcommand::Metadata(args) => add_metadata::run(ctx, args),
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

        Command::Cat(args) => cat::run(ctx, args),
        Command::Compress(args) => compress::run(ctx, args),
        Command::Convert(args) => convert::run(ctx, args),
        Command::Decompress(args) => decompress::run(ctx, args),
        Command::Doctor(args) => doctor::run(ctx, args),
        Command::Du(args) => du::run(ctx, args),
        Command::Filter(args) => filter::run(ctx, args),
        Command::Merge(args) => merge::run(ctx, args),
        Command::Recover => recover::run(ctx),
        Command::Sort(args) => sort::run(ctx, args),
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::dispatch;
    use crate::cli::{
        AddAttachmentCommand, AddCommand, AddMetadataCommand, AddSubcommand, Command,
        CompressCommand, DoctorCommand, DuCommand, GetAttachmentCommand, GetMetadataCommand,
        InfoCommand, ListAttachmentsCommand, ListChannelsCommand, ListChunksCommand, ListCommand,
        ListMetadataCommand, ListSchemasCommand, ListSubcommand, SortCommand,
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
    fn du_requires_existing_file() {
        let err = dispatch(
            &CommandContext::default(),
            Command::Du(DuCommand {
                approximate: false,
                file: PathBuf::from("does-not-exist.mcap"),
            }),
        )
        .expect_err("du should fail on missing file");
        assert!(err.to_string().contains("couldn't open"));
    }

    #[test]
    fn doctor_requires_existing_file() {
        let err = dispatch(
            &CommandContext::default(),
            Command::Doctor(DoctorCommand {
                strict_message_order: false,
                file: PathBuf::from("does-not-exist.mcap"),
            }),
        )
        .expect_err("doctor should fail on missing file");
        assert!(err.to_string().contains("couldn't open"));
    }

    #[test]
    fn add_attachment_requires_existing_attachment_source() {
        let err = dispatch(
            &CommandContext::default(),
            Command::Add(AddCommand {
                command: AddSubcommand::Attachment(AddAttachmentCommand {
                    file: PathBuf::from("does-not-exist.mcap"),
                    attachment_file: PathBuf::from("attachment.bin"),
                    name: None,
                    content_type: "application/octet-stream".to_string(),
                    log_time: None,
                    creation_time: None,
                }),
            }),
        )
        .expect_err("add attachment should fail on missing file");
        assert!(err.to_string().contains("failed to read attachment source"));
    }

    #[test]
    fn add_metadata_requires_existing_mcap_file() {
        let err = dispatch(
            &CommandContext::default(),
            Command::Add(AddCommand {
                command: AddSubcommand::Metadata(AddMetadataCommand {
                    file: PathBuf::from("does-not-exist.mcap"),
                    name: "demo".to_string(),
                    key_values: vec!["k=v".to_string()],
                }),
            }),
        )
        .expect_err("add metadata should fail on missing file");
        assert!(err.to_string().contains("failed to add metadata"));
    }

    #[test]
    fn get_subcommands_require_existing_file() {
        let attachment_err = dispatch(
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
        assert!(attachment_err.to_string().contains("couldn't open"));

        let metadata_err = dispatch(
            &CommandContext::default(),
            Command::Get(crate::cli::GetCommand {
                command: crate::cli::GetSubcommand::Metadata(GetMetadataCommand {
                    file: PathBuf::from("does-not-exist.mcap"),
                    name: "demo".to_string(),
                }),
            }),
        )
        .expect_err("get metadata should fail on missing file");
        assert!(metadata_err.to_string().contains("couldn't open"));
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

    #[test]
    fn compress_rejects_invalid_compression() {
        let err = dispatch(
            &CommandContext::default(),
            Command::Compress(CompressCommand {
                file: None,
                output: None,
                chunk_size: 4 * 1024 * 1024,
                compression: "invalid".to_string(),
                unchunked: false,
            }),
        )
        .expect_err("compress should reject invalid compression");
        assert!(err.to_string().contains("unrecognized compression format"));
    }

    #[test]
    fn sort_requires_existing_file() {
        let err = dispatch(
            &CommandContext::default(),
            Command::Sort(SortCommand {
                file: PathBuf::from("does-not-exist.mcap"),
                output_file: PathBuf::from("sorted.mcap"),
                chunk_size: 4 * 1024 * 1024,
                compression: crate::cli::ConvertCompression::Zstd,
                include_crc: true,
                chunked: true,
            }),
        )
        .expect_err("sort should fail on missing input file");
        assert!(
            err.to_string().contains("couldn't open")
                || err.to_string().contains("failed to canonicalize input")
        );
    }
}
