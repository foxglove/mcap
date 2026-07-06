use anyhow::Result;

use crate::cli::CompressCommand;
use crate::context::CommandContext;
use crate::rewrite::{self, RewriteOptions};

pub fn run(ctx: &CommandContext, args: CompressCommand) -> Result<()> {
    let options = RewriteOptions::new(args.common.file, args.common.output, args.common.chunk_size)
        .compression(args.compression)
        .use_chunks(true)
        .include_crc(!args.common.no_crc)
        .include_metadata(true)
        .include_attachments(true)
        .order(args.common.order);
    rewrite::run(
        options,
        crate::source::SourceOptions::new(ctx.allow_remote_scan()),
    )
}
