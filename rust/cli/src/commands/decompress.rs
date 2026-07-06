use anyhow::Result;

use crate::cli::{DecompressCommand, MessageOrder};
use crate::context::CommandContext;
use crate::rewrite::{self, RewriteOptions};

pub fn run(ctx: &CommandContext, args: DecompressCommand) -> Result<()> {
    let options = RewriteOptions::new(args.file, args.output, args.chunk_size)
        .compression("none")
        .include_metadata(true)
        .include_attachments(true)
        .use_chunks(true)
        // An uncompressed copy preserves the input's message order; use `mcap sort` to reorder.
        .order(MessageOrder::Preserve);
    rewrite::run(
        options,
        crate::source::SourceOptions::new(ctx.allow_remote_scan()),
    )
}
