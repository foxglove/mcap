use anyhow::Result;

use crate::cli::CompressCommand;
use crate::commands::filter::{self, TranscodeCommandOptions};
use crate::context::CommandContext;

pub fn run(_ctx: &CommandContext, args: CompressCommand) -> Result<()> {
    let mut options = TranscodeCommandOptions::new(args.file, args.output, args.chunk_size)
        .compression(args.compression)
        .use_chunks(!args.unchunked);
    options.include_metadata = true;
    options.include_attachments = true;
    filter::run_transcode(options)
}
