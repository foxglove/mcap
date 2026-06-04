mod add;
mod cat;
mod completion;
mod compress;
mod convert;
mod decompress;
mod doctor;
mod du;
mod filter;
mod get;
mod info;
mod list;
mod merge;
mod recover;
mod sort;

use anyhow::Result;

use crate::cli::{AddSubcommand, Command, GetSubcommand, ListSubcommand};
use crate::context::CommandContext;

/// How a command finished, for commands that complete successfully but still want to influence the
/// process exit code. Hard failures are reported via `Err` and handled by `main` (exit 1); clap
/// owns exit 2 for argument-parsing errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandOutcome {
    /// Completed cleanly. Exit 0.
    Success,
    /// Completed, but with warning-level data loss (for example, `recover` discarding records or
    /// stopping early on a truncated input). Exit 3.
    Warnings,
}

impl CommandOutcome {
    pub fn exit_code(self) -> u8 {
        match self {
            CommandOutcome::Success => 0,
            CommandOutcome::Warnings => 3,
        }
    }
}

pub fn dispatch(ctx: &CommandContext, command: Command) -> Result<CommandOutcome> {
    match command {
        Command::Info(args) => info::run(ctx, args).map(|()| CommandOutcome::Success),

        Command::Add(args) => match args.command {
            AddSubcommand::Attachment(args) => add::attachment::run(ctx, args),
            AddSubcommand::Metadata(args) => add::metadata::run(ctx, args),
        }
        .map(|()| CommandOutcome::Success),
        Command::Get(args) => match args.command {
            GetSubcommand::Attachment(args) => get::attachment::run(ctx, args),
            GetSubcommand::Metadata(args) => get::metadata::run(ctx, args),
        }
        .map(|()| CommandOutcome::Success),
        Command::List(args) => match args.command {
            ListSubcommand::Attachments(args) => list::attachments::run(ctx, args),
            ListSubcommand::Channels(args) => list::channels::run(ctx, args),
            ListSubcommand::Chunks(args) => list::chunks::run(ctx, args),
            ListSubcommand::Metadata(args) => list::metadata::run(ctx, args),
            ListSubcommand::Schemas(args) => list::schemas::run(ctx, args),
        }
        .map(|()| CommandOutcome::Success),

        Command::Cat(args) => cat::run(ctx, args).map(|()| CommandOutcome::Success),
        Command::Completion(args) => completion::run(args).map(|()| CommandOutcome::Success),
        Command::Compress(args) => compress::run(ctx, args).map(|()| CommandOutcome::Success),
        Command::Convert(args) => convert::run(ctx, args).map(|()| CommandOutcome::Success),
        Command::Decompress(args) => decompress::run(ctx, args).map(|()| CommandOutcome::Success),
        Command::Doctor(args) => doctor::run(ctx, args).map(|()| CommandOutcome::Success),
        Command::Du(args) => du::run(ctx, args).map(|()| CommandOutcome::Success),
        Command::Filter(args) => filter::run(ctx, args).map(|()| CommandOutcome::Success),
        Command::Merge(args) => merge::run(ctx, args).map(|()| CommandOutcome::Success),
        Command::Recover(args) => recover::run(ctx, args),
        Command::Sort(args) => sort::run(ctx, args).map(|()| CommandOutcome::Success),
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
        ListMetadataCommand, ListSchemasCommand, ListSubcommand, RecoverCommand, SortCommand,
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
    fn recover_requires_existing_file_when_input_is_provided() {
        let err = dispatch(
            &CommandContext::default(),
            Command::Recover(RecoverCommand {
                file: Some(PathBuf::from("does-not-exist.mcap")),
                output: Some(PathBuf::from("recovered.mcap")),
                chunk_size: mcap::WriteOptions::DEFAULT_CHUNK_SIZE,
                compression: "preserve".to_string(),
            }),
        )
        .expect_err("recover should fail on missing file");
        assert!(err.to_string().contains("couldn't open"));
    }

    #[test]
    fn compress_rejects_invalid_compression() {
        let err = dispatch(
            &CommandContext::default(),
            Command::Compress(CompressCommand {
                file: None,
                output: None,
                chunk_size: mcap::WriteOptions::DEFAULT_CHUNK_SIZE,
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
                chunk_size: mcap::WriteOptions::DEFAULT_CHUNK_SIZE,
                compression: crate::cli::CompressionFormat::Zstd,
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
