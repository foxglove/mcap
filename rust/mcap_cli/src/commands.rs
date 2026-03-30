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
use crate::context::CommandContext;

pub fn not_implemented(command_name: &str) -> anyhow::Error {
    anyhow::anyhow!("'{command_name}' is not implemented yet")
}

pub fn dispatch(ctx: &CommandContext, command: Command) -> Result<()> {
    match command {
        Command::Info => info::run(ctx),
        Command::Version(args) => version::run(ctx, args),

        Command::Add(args) => match args.command {
            AddSubcommand::Attachment => add_attachment::run(ctx),
            AddSubcommand::Metadata => add_metadata::run(ctx),
        },
        Command::Get(args) => match args.command {
            GetSubcommand::Attachment => get_attachment::run(ctx),
            GetSubcommand::Metadata => get_metadata::run(ctx),
        },
        Command::List(args) => match args.command {
            ListSubcommand::Attachments => list_attachments::run(ctx),
            ListSubcommand::Channels => list_channels::run(ctx),
            ListSubcommand::Chunks => list_chunks::run(ctx),
            ListSubcommand::Metadata => list_metadata::run(ctx),
            ListSubcommand::Schemas => list_schemas::run(ctx),
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
    use super::dispatch;
    use crate::cli::{AddCommand, AddSubcommand, Command, ListCommand, ListSubcommand};
    use crate::context::CommandContext;

    #[test]
    fn info_returns_not_implemented() {
        let err =
            dispatch(&CommandContext::default(), Command::Info).expect_err("info should be a stub");
        assert_eq!(err.to_string(), "'info' is not implemented yet");
    }

    #[test]
    fn list_subcommands_stub_with_specific_names() {
        let err = dispatch(
            &CommandContext::default(),
            Command::List(ListCommand {
                command: ListSubcommand::Channels,
            }),
        )
        .expect_err("list channels should be a stub");
        assert_eq!(err.to_string(), "'list channels' is not implemented yet");
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
}
