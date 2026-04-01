use anyhow::Result;

use crate::cli::DecompressCommand;
use crate::commands::filter::{self, TranscodeCommandOptions};
use crate::context::CommandContext;

pub fn run(_ctx: &CommandContext, args: DecompressCommand) -> Result<()> {
    let mut options = TranscodeCommandOptions::new(args.file, args.output, args.chunk_size)
        .compression("none")
        .use_chunks(true);
    options.include_metadata = true;
    options.include_attachments = true;
    filter::run_transcode(options)
}
