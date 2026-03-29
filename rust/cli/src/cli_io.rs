#![allow(dead_code)]

use std::{
    fs::File,
    io::{self, IsTerminal, Read},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use mcap::MAGIC;

pub fn open_local_mcap(path: &Path) -> Result<Vec<u8>> {
    // Scaffold helper for commands that need quick access to file bytes;
    // command implementations should prefer streaming readers for large files.
    std::fs::read(path).with_context(|| format!("failed to read file {}", path.display()))
}

pub fn open_local_file(path: &Path) -> Result<File> {
    File::open(path).with_context(|| format!("failed to open file {}", path.display()))
}

pub fn create_local_file(path: &Path) -> Result<File> {
    File::create(path).with_context(|| format!("failed to create file {}", path.display()))
}

pub fn reading_stdin() -> Result<bool> {
    Ok(!std::io::stdin().is_terminal())
}

pub fn stdout_redirected() -> Result<bool> {
    Ok(!std::io::stdout().is_terminal())
}

pub fn ensure_stdout_redirected_for_binary_output() -> Result<()> {
    if !stdout_redirected()? {
        anyhow::bail!(
            "Binary output can screw up your terminal. Supply -o or redirect to a file or pipe"
        );
    }
    Ok(())
}

pub fn read_paths_from_stdin() -> Result<Vec<PathBuf>> {
    if !reading_stdin()? {
        return Ok(Vec::new());
    }
    let mut input = String::new();
    io::stdin()
        .read_to_string(&mut input)
        .context("failed to read stdin")?;
    Ok(input
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(PathBuf::from)
        .collect())
}

pub fn has_mcap_magic(bytes: &[u8]) -> bool {
    bytes.starts_with(MAGIC) && bytes.ends_with(MAGIC)
}
