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

pub fn dispatch(ctx: &CommandContext, command: Command) -> Result<()> {
    let _ = (ctx.verbose, ctx.color, &ctx.config, ctx.pprof_profile);
    match command {
        Command::Info => info::run(),
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
            ListSubcommand::Attachments => list_attachments::run(),
            ListSubcommand::Channels => list_channels::run(),
            ListSubcommand::Chunks => list_chunks::run(),
            ListSubcommand::Metadata => list_metadata::run(),
            ListSubcommand::Schemas => list_schemas::run(),
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
    use crate::cli::{AddCommand, AddSubcommand, Command, ListCommand, ListSubcommand};
    use crate::context::CommandContext;

    #[test]
    fn info_returns_not_implemented() {
        let err = dispatch(&CommandContext::default(), Command::Info)
            .expect_err("info should be a stub");
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
