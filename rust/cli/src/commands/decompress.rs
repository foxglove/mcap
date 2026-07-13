use anyhow::Result;

use crate::cli::DecompressCommand;
use crate::context::CommandContext;
use crate::rewrite::{self, RewriteOptions};

pub fn run(ctx: &CommandContext, args: DecompressCommand) -> Result<()> {
    args.common.warn_deprecations();
    // `filter`-style rewrite with a preset: rechunk uncompressed, keeping metadata and attachments
    // and preserving the input's message order (`decompress` doesn't reorder). Paths, chunk size,
    // and `--no-crc` come from the shared args.
    let options = RewriteOptions::from(&args.common).compression(None);
    rewrite::run(
        options,
        crate::source::SourceOptions::new(ctx.allow_remote_scan()),
    )
}
