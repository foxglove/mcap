use anyhow::Result;

use crate::cli::DecompressCommand;
use crate::context::CommandContext;
use crate::rewrite::{self, RewriteOptions};

pub fn run(ctx: &CommandContext, args: DecompressCommand) -> Result<()> {
    // `filter`-style rewrite with a preset: rechunk uncompressed, keeping metadata and
    // attachments. Paths, chunk size, `--no-crc`, and order come from the shared args.
    let options = RewriteOptions::from(&args.common).compression("none");
    rewrite::run(
        options,
        crate::source::SourceOptions::new(ctx.allow_remote_scan()),
    )
}
