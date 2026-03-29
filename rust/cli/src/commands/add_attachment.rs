use std::{
    borrow::Cow,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};

use crate::{
    cli::AddAttachmentArgs, commands::rewrite::rewrite_mcap_with_appends, time::parse_date_or_nanos,
};

pub fn run(args: AddAttachmentArgs) -> Result<()> {
    let attachment_bytes = std::fs::read(&args.attachment_file).with_context(|| {
        format!(
            "failed to read attachment file {}",
            args.attachment_file.display()
        )
    })?;
    let now_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock before UNIX_EPOCH")?
        .as_nanos();
    let now_nanos = u64::try_from(now_nanos).context("system time too large for u64 nanos")?;

    let file_name_fallback = Path::new(&args.attachment_file)
        .file_name()
        .map(|v| v.to_string_lossy().into_owned())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "attachment file path has no filename component: {}",
                args.attachment_file.display()
            )
        })?;

    let attachment = mcap::Attachment {
        log_time: match &args.log_time {
            Some(value) => parse_date_or_nanos(value)?,
            None => now_nanos,
        },
        create_time: match &args.creation_time {
            Some(value) => parse_date_or_nanos(value)?,
            None => now_nanos,
        },
        name: args.name.unwrap_or(file_name_fallback),
        media_type: args.content_type,
        data: Cow::Owned(attachment_bytes),
    };

    rewrite_mcap_with_appends(&args.file, Some(attachment), None)
        .with_context(|| format!("failed to add attachment to {}", args.file.display()))
}
