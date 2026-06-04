use std::io;

use anyhow::Result;
use clap::CommandFactory;
use clap_complete::generate;

use crate::cli::{Args, CompletionCommand};

/// Generate a shell completion script for the requested shell and write it to stdout.
pub fn run(args: CompletionCommand) -> Result<()> {
    let mut cmd = Args::command();
    let bin_name = cmd.get_name().to_string();
    generate(args.shell, &mut cmd, bin_name, &mut io::stdout());
    Ok(())
}
