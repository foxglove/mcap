use anyhow::Result;

use crate::cli::{CompressCommand, MessageOrder};
use crate::context::CommandContext;
use crate::rewrite::{self, RewriteOptions};

pub fn run(ctx: &CommandContext, args: CompressCommand) -> Result<()> {
    let options = RewriteOptions::new(args.file, args.output, args.chunk_size)
        .compression(args.compression)
        .use_chunks(true)
        .include_metadata(true)
        .include_attachments(true)
        // A compressed copy preserves the input's message order; use `mcap sort` to reorder.
        .order(MessageOrder::Preserve);
    rewrite::run(
        options,
        crate::source::SourceOptions::new(ctx.allow_remote_scan()),
    )
}
