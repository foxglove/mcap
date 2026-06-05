use anyhow::Result;

use crate::cli::CompressCommand;
use crate::commands::filter::{self, TranscodeCommandOptions};
use crate::context::CommandContext;

pub fn run(ctx: &CommandContext, args: CompressCommand) -> Result<()> {
    let options = TranscodeCommandOptions::new(args.file, args.output, args.chunk_size)
        .compression(args.compression)
        .use_chunks(true)
        .include_metadata(true)
        .include_attachments(true);
    filter::run_transcode(
        options,
        crate::source::SourceOptions::new(ctx.allow_remote_scan()),
    )
}
