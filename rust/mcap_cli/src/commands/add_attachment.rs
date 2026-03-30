use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};

use crate::cli::AddAttachmentCommand;
use crate::commands::add_common::{self, AttachmentToAdd};
use crate::context::CommandContext;

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

    add_common::amend_mcap_file(&args.file, &[attachment], &[]).with_context(|| {
        format!(
            "failed to add attachment to '{}'. You may need to run `mcap recover` to repair the file.",
            args.file.display()
        )
    })?;
    Ok(())
}

pub(crate) fn parse_timestamp_or_nanos(value: &str) -> Result<u64> {
    if let Ok(nanos) = value.parse::<u64>() {
        return Ok(nanos);
    }

    let parsed = chrono::DateTime::parse_from_rfc3339(value)
        .with_context(|| format!("failed to parse timestamp '{value}'"))?;
    let seconds = parsed.timestamp();
    anyhow::ensure!(seconds >= 0, "timestamp is before unix epoch: '{value}'");
    let seconds = seconds as u64;
    let nanos = parsed.timestamp_subsec_nanos() as u64;
    seconds
        .checked_mul(1_000_000_000)
        .and_then(|v| v.checked_add(nanos))
        .with_context(|| format!("timestamp is out of range: '{value}'"))
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

#[cfg(test)]
mod tests {
    use super::parse_timestamp_or_nanos;

    #[test]
    fn parses_nanos_or_rfc3339() {
        assert_eq!(parse_timestamp_or_nanos("123").expect("nanos"), 123);
        let ts = parse_timestamp_or_nanos("2023-07-25T15:27:30.132545471Z").expect("rfc3339");
        assert_eq!(ts, 1_690_298_850_132_545_471);
    }

    #[test]
    fn rejects_invalid_timestamp() {
        let err = parse_timestamp_or_nanos("not-a-time").expect_err("invalid time should fail");
        assert!(err.to_string().contains("failed to parse timestamp"));
    }
}
