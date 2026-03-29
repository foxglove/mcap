use std::io::Write;

use anyhow::{Context, Result};
use mcap::{read, Summary};

use crate::{
    cli::GetAttachmentArgs,
    cli_io::{ensure_stdout_redirected_for_binary_output, open_local_mcap},
};

pub fn run(args: GetAttachmentArgs) -> Result<()> {
    let bytes = open_local_mcap(&args.file)?;
    let summary = Summary::read(&bytes)?
        .ok_or_else(|| anyhow::anyhow!("failed to read summary from {}", args.file.display()))?;

    let matches = summary
        .attachment_indexes
        .iter()
        .filter(|idx| idx.name == args.name)
        .collect::<Vec<_>>();

    if matches.is_empty() {
        anyhow::bail!("attachment '{}' not found", args.name);
    }

    let index = match (args.offset, matches.len()) {
        (Some(offset), _) => matches
            .into_iter()
            .find(|idx| idx.offset == offset)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "failed to find attachment '{}' at offset {offset}",
                    args.name
                )
            })?,
        (None, 1) => matches[0],
        (None, _) => anyhow::bail!(
            "multiple attachments named '{}' exist (specify an offset)",
            args.name
        ),
    };

    let attachment = read::attachment(&bytes, index)
        .with_context(|| format!("failed to read attachment '{}'", args.name))?;

    if let Some(output_path) = args.output {
        std::fs::write(&output_path, attachment.data.as_ref())
            .with_context(|| format!("failed to write {}", output_path.display()))?;
    } else {
        ensure_stdout_redirected_for_binary_output()?;
        let mut stdout = std::io::stdout().lock();
        stdout
            .write_all(attachment.data.as_ref())
            .context("failed to write attachment bytes to stdout")?;
    }

    Ok(())
}
