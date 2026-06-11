use std::io::{self, ErrorKind, Write};

use anyhow::{Context, Result};
use clap::CommandFactory;
use clap_complete::generate;

use crate::cli::{Args, CompletionCommand};

/// Generate a shell completion script for the requested shell and write it to stdout.
pub fn run(args: CompletionCommand) -> Result<()> {
    let mut buffer = Vec::new();
    let mut cmd = Args::command();
    let bin_name = cmd.get_name().to_string();
    generate(args.shell, &mut cmd, bin_name, &mut buffer);

    let stdout = io::stdout();
    write_completion(stdout.lock(), &buffer)
}

fn write_completion(mut writer: impl Write, buffer: &[u8]) -> Result<()> {
    match writer.write_all(buffer) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == ErrorKind::BrokenPipe => Ok(()),
        Err(err) => Err(err).context("failed to write completion script"),
    }
}
