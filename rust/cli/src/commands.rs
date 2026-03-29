mod add_attachment;
mod add_metadata;
mod cat;
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
mod rewrite;
mod sort;
mod transform_common;
mod version;

use anyhow::Result;

use crate::cli::{AddSubcommand, Command, GetSubcommand, ListSubcommand};

pub fn dispatch(command: Command) -> Result<()> {
    match command {
        Command::Info(args) => info::run(args),
        Command::Version(args) => version::run(args),

        Command::Add(args) => match args.command {
            AddSubcommand::Attachment(args) => add_attachment::run(args),
            AddSubcommand::Metadata(args) => add_metadata::run(args),
        },
        Command::Get(args) => match args.command {
            GetSubcommand::Attachment(args) => get_attachment::run(args),
            GetSubcommand::Metadata(args) => get_metadata::run(args),
        },
        Command::List(args) => match args.command {
            ListSubcommand::Attachments(input) => list_attachments::run(input),
            ListSubcommand::Channels(input) => list_channels::run(input),
            ListSubcommand::Chunks(input) => list_chunks::run(input),
            ListSubcommand::Metadata(input) => list_metadata::run(input),
            ListSubcommand::Schemas(input) => list_schemas::run(input),
        },

        Command::Cat(args) => cat::run(args),
        Command::Compress(args) => compress::run(args),
        Command::Convert(args) => convert::run(args),
        Command::Decompress(args) => decompress::run(args),
        Command::Doctor(args) => doctor::run(args),
        Command::Du(args) => du::run(args),
        Command::Filter(args) => filter::run(args),
        Command::Merge(args) => merge::run(args),
        Command::Recover(args) => recover::run(args),
        Command::Sort(args) => sort::run(args),
    }
}

#[cfg(test)]
mod tests {
    use super::dispatch;
    use crate::cli::{
        AddAttachmentArgs, AddCommand, AddSubcommand, Command, InputFile, ListCommand,
        ListSubcommand,
    };
    use std::path::PathBuf;

    #[test]
    fn info_command_executes() {
        dispatch(Command::Info(InputFile {
            file: PathBuf::from("non-existent.mcap"),
        }))
        .expect_err("info should fail for missing file");
    }

    #[test]
    fn list_subcommands_execute() {
        let err = dispatch(Command::List(ListCommand {
            command: ListSubcommand::Channels(InputFile {
                file: PathBuf::from("non-existent.mcap"),
            }),
        }))
        .expect_err("list channels should fail for missing file");
        assert!(err.to_string().contains("failed to read file"));
    }

    #[test]
    fn add_subcommands_stub_with_specific_names() {
        let err = dispatch(Command::Add(AddCommand {
            command: AddSubcommand::Attachment(AddAttachmentArgs {
                file: PathBuf::from("non-existent.mcap"),
                attachment_file: PathBuf::from("file.bin"),
                name: None,
                content_type: "application/octet-stream".to_string(),
                log_time: None,
                creation_time: None,
            }),
        }))
        .expect_err("add attachment should be a stub");
        assert!(err.to_string().contains("failed to read attachment file"));
    }
}
