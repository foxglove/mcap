use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};

use crate::cli::AddAttachmentCommand;
use crate::commands::add::shared::{self, AttachmentToAdd};
use crate::context::CommandContext;
use crate::render::parse_timestamp_or_nanos;

pub fn run(_ctx: &CommandContext, args: AddAttachmentCommand) -> Result<()> {
    let attachment_data = fs::read(&args.attachment_file).with_context(|| {
        format!(
            "failed to read attachment source '{}'",
            args.attachment_file.display()
        )
    })?;

    let create_time = match args.creation_time.as_deref() {
        Some(value) => parse_timestamp_or_nanos(value)?,
        None => {
            let metadata = fs::metadata(&args.attachment_file).with_context(|| {
                format!(
                    "failed to read metadata for attachment source '{}'",
                    args.attachment_file.display()
                )
            })?;
            let modified = metadata.modified().with_context(|| {
                format!(
                    "failed to read modified time for attachment source '{}'",
                    args.attachment_file.display()
                )
            })?;
            system_time_to_nanos(modified)?
        }
    };

    let log_time = match args.log_time.as_deref() {
        Some(value) => parse_timestamp_or_nanos(value)?,
        None => system_time_to_nanos(SystemTime::now())?,
    };

    let attachment = AttachmentToAdd {
        log_time,
        create_time,
        name: args
            .name
            .unwrap_or_else(|| args.attachment_file.display().to_string()),
        media_type: args.content_type,
        data: attachment_data,
    };

    shared::amend_mcap_file(&args.file, &[attachment], &[])
        .with_context(|| format!("failed to add attachment to '{}'", args.file.display()))?;
    Ok(())
}

fn system_time_to_nanos(time: SystemTime) -> Result<u64> {
    let duration = time
        .duration_since(UNIX_EPOCH)
        .context("timestamp is before unix epoch")?;
    duration
        .as_secs()
        .checked_mul(1_000_000_000)
        .and_then(|v| v.checked_add(duration.subsec_nanos() as u64))
        .context("timestamp is out of range")
}
