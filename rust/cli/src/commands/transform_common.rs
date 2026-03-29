#![allow(dead_code)]

use std::{
    io::{Read, Write},
    path::PathBuf,
};

use anyhow::{Context, Result};

use crate::{cli_io, time::parse_date_or_nanos};

pub fn input_bytes_from_optional_file(file: Option<&PathBuf>) -> Result<Vec<u8>> {
    match file {
        Some(path) => cli_io::open_local_mcap(path),
        None => {
            if !cli_io::reading_stdin()? {
                anyhow::bail!("please supply a file. see --help for usage details.");
            }
            let mut buf = Vec::new();
            std::io::stdin()
                .lock()
                .read_to_end(&mut buf)
                .context("failed to read MCAP data from stdin")?;
            Ok(buf)
        }
    }
}

pub fn write_output_bytes(output: Option<&PathBuf>, bytes: &[u8]) -> Result<()> {
    if let Some(path) = output {
        std::fs::write(path, bytes)
            .with_context(|| format!("failed to write output file {}", path.display()))?;
        return Ok(());
    }
    cli_io::ensure_stdout_redirected_for_binary_output()?;
    let mut stdout = std::io::stdout().lock();
    stdout
        .write_all(bytes)
        .context("failed to write output bytes to stdout")?;
    Ok(())
}

pub fn parse_optional_time_bound(value: Option<&str>) -> Result<Option<u64>> {
    value.map(parse_date_or_nanos).transpose()
}

pub fn compression_from_str(name: &str) -> Result<Option<mcap::Compression>> {
    match name {
        "zstd" => Ok(Some(mcap::Compression::Zstd)),
        "lz4" => Ok(Some(mcap::Compression::Lz4)),
        "none" | "" => Ok(None),
        other => anyhow::bail!(
            "unrecognized compression format '{other}': valid options are 'lz4', 'zstd', or 'none'"
        ),
    }
}

pub fn split_topics(topics: Option<&str>) -> Option<Vec<String>> {
    topics.map(|raw| {
        raw.split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToString::to_string)
            .collect::<Vec<_>>()
    })
}
