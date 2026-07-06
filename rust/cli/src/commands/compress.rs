use anyhow::Result;

use crate::cli::CompressCommand;
use crate::context::CommandContext;
use crate::rewrite::{self, RewriteOptions};

pub fn run(ctx: &CommandContext, args: CompressCommand) -> Result<()> {
    args.common.warn_deprecations();
    // `filter`-style rewrite with a preset: chunk with the chosen compression, keeping metadata
    // and attachments. Paths, chunk size, `--no-crc`, and order come from the shared args.
    let options = RewriteOptions::from(&args.common).compression(args.compression);
    rewrite::run(
        options,
        crate::source::SourceOptions::new(ctx.allow_remote_scan()),
    )
}
