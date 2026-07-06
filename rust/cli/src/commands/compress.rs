use anyhow::Result;

use crate::cli::CompressCommand;
use crate::context::CommandContext;
use crate::rewrite::{self, RewriteOptions};

pub fn run(ctx: &CommandContext, args: CompressCommand) -> Result<()> {
    let options = RewriteOptions::new(args.file, args.output, args.chunk_size)
        .compression(args.compression)
        .use_chunks(true)
        .include_metadata(true)
        .include_attachments(true)
        // A compressed copy preserves the input's message order; use `mcap sort` to reorder.
        .order_by_log_time(false);
    rewrite::run(
        options,
        crate::source::SourceOptions::new(ctx.allow_remote_scan()),
    )
}
