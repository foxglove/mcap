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
mod sort;
mod version;

use anyhow::Result;

use crate::cli::{AddSubcommand, Command, GetSubcommand, ListSubcommand};

pub fn dispatch(command: Command) -> Result<()> {
    match command {
        Command::Info(args) => info::run(args),
        Command::Version(args) => version::run(args),

        Command::Add(args) => match args.command {
            AddSubcommand::Attachment => add_attachment::run(),
            AddSubcommand::Metadata => add_metadata::run(),
        },
        Command::Get(args) => match args.command {
            GetSubcommand::Attachment => get_attachment::run(),
            GetSubcommand::Metadata => get_metadata::run(),
        },
        Command::List(args) => match args.command {
            ListSubcommand::Attachments(input) => list_attachments::run(input),
            ListSubcommand::Channels(input) => list_channels::run(input),
            ListSubcommand::Chunks(input) => list_chunks::run(input),
            ListSubcommand::Metadata(input) => list_metadata::run(input),
            ListSubcommand::Schemas(input) => list_schemas::run(input),
        },

        Command::Cat => cat::run(),
        Command::Compress => compress::run(),
        Command::Convert => convert::run(),
        Command::Decompress => decompress::run(),
        Command::Doctor => doctor::run(),
        Command::Du => du::run(),
        Command::Filter => filter::run(),
        Command::Merge => merge::run(),
        Command::Recover => recover::run(),
        Command::Sort => sort::run(),
    }
}

#[cfg(test)]
mod tests {
    use super::dispatch;
    use crate::cli::{AddCommand, AddSubcommand, Command, InputFile, ListCommand, ListSubcommand};
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
            command: AddSubcommand::Attachment,
        }))
        .expect_err("add attachment should be a stub");
        assert_eq!(err.to_string(), "'add attachment' is not implemented yet");
    }
}
