//! The `filter` command: rewrite an MCAP, keeping a selected subset of records.
//!
//! This is a thin adapter over the shared [`crate::rewrite`] engine. Selection is opt-out: by
//! default everything is kept and flags narrow the output; see the engine docs for details.
use anyhow::Result;

use crate::cli::{FilterCommand, MessageOrder};
use crate::context::CommandContext;
use crate::{rewrite, source};

pub fn run(ctx: &CommandContext, args: FilterCommand) -> Result<()> {
    args.transcode.warn_deprecations();
    let order_by_log_time = matches!(args.order_by, MessageOrder::LogTime);
    rewrite::run(
        args.transcode
            .command_options(args.file, args.output, order_by_log_time),
        source::SourceOptions::new(ctx.allow_remote_scan()),
    )
}
