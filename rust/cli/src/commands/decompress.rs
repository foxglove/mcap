use anyhow::Result;

use crate::cli::DecompressCommand;
use crate::commands::filter::{self, TranscodeCommandOptions};
use crate::context::CommandContext;

pub fn run(_ctx: &CommandContext, args: DecompressCommand) -> Result<()> {
    let options = TranscodeCommandOptions::new(args.file, args.output, args.chunk_size)
        .compression("none")
        .include_metadata(true)
        .include_attachments(true)
        .use_chunks(true);
    filter::run_transcode(options)
}
