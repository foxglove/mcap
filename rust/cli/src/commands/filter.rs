//! The `filter` command: rewrite an MCAP, keeping a selected subset of records. This is a thin
//! adapter over the shared [`crate::rewrite`] engine, which owns the rewrite pipeline and the
//! standardized record placement.
use anyhow::Result;
use log::warn;

use crate::cli::FilterCommand;
use crate::context::CommandContext;
use crate::rewrite::{self, RewriteOptions};
use crate::source;

pub fn run(ctx: &CommandContext, args: FilterCommand) -> Result<()> {
    if args.include_metadata {
        warn!("--include-metadata is deprecated and has no effect; metadata is included by default (use --exclude-metadata to drop it)");
    }
    if args.include_attachments {
        warn!("--include-attachments is deprecated and has no effect; attachments are included by default (use --exclude-attachments to drop them)");
    }
    if args.output_compression.is_some() {
        warn!("--output-compression is deprecated and takes precedence over --compression; use --compression instead");
    }
    rewrite::run(
        RewriteOptions::from(&args),
        source::SourceOptions::new(ctx.allow_remote_scan()),
    )
}
