use anyhow::Result;

use crate::cli::CompressCommand;
use crate::commands::filter::{self, TranscodeCommandOptions};
use crate::context::CommandContext;

pub fn run(_ctx: &CommandContext, args: CompressCommand) -> Result<()> {
    // Intentionally keep accepting --chunk-size/--compression with --unchunked to
    // match Go CLI flag behavior. With unchunked output there are no chunk records,
    // so these settings are effectively ignored by the writer.
    let options = TranscodeCommandOptions::new(args.file, args.output, args.chunk_size)
        .compression(args.compression)
        .use_chunks(!args.unchunked)
        .include_metadata(true)
        .include_attachments(true);
    filter::run_transcode(options)
}
