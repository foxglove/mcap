use std::io::IsTerminal as _;
use std::io::Write as _;

use anyhow::{Context, Result};

use crate::cli::GetAttachmentCommand;
use crate::commands::common;
use crate::context::CommandContext;

const PLEASE_REDIRECT: &str =
    "Binary output can screw up your terminal. Supply -o or redirect to a file or pipe";

pub fn run(ctx: &CommandContext, args: GetAttachmentCommand) -> Result<()> {
    let attachment = if let Some(remote) = common::try_open_remote_mcap(&args.file)? {
        let index = select_attachment_index(&remote.summary().attachment_indexes, &args.name, args.offset)?;
        let bytes = remote.read_range(index.offset, usize::try_from(index.length).context("attachment record is too large to read on this platform")?)?;
        parse_attachment_record(&bytes).with_context(|| {
            format!(
                "failed to read attachment {} at offset {}",
                args.name, index.offset
            )
        })?
    } else {
        let mcap = common::load_path(
            &args.file,
            common::SourceOptions::new(ctx.allow_remote_scan()),
        )?;
        let parsed = common::parse_mcap(&mcap)?;
        let index = select_attachment_index(&parsed.attachment_indexes, &args.name, args.offset)?;
        let attachment = mcap::read::attachment(&mcap, index).with_context(|| {
            format!(
                "failed to read attachment {} at offset {}",
                args.name, index.offset
            )
        })?;
        own_attachment(attachment)
    };

    if let Some(output) = args.output {
        std::fs::write(&output, &attachment.data)
            .with_context(|| format!("failed to write attachment to '{}'", output.display()))?;
    } else if std::io::stdout().is_terminal() {
        anyhow::bail!("{PLEASE_REDIRECT}");
    } else {
        std::io::stdout()
            .write_all(&attachment.data)
            .context("failed to write attachment to stdout")?;
    }

    Ok(())
}



fn own_attachment(attachment: mcap::Attachment<'_>) -> mcap::Attachment<'static> {
    mcap::Attachment {
        log_time: attachment.log_time,
        create_time: attachment.create_time,
        name: attachment.name,
        media_type: attachment.media_type,
        data: std::borrow::Cow::Owned(attachment.data.into_owned()),
    }
}

fn parse_attachment_record(bytes: &[u8]) -> Result<mcap::Attachment<'static>> {
    let mut reader = mcap::read::LinearReader::sans_magic(bytes);
    let attachment = match reader.next().ok_or(mcap::McapError::BadIndex)?? {
        mcap::records::Record::Attachment { header, data, .. } => mcap::Attachment {
            log_time: header.log_time,
            create_time: header.create_time,
            name: header.name,
            media_type: header.media_type,
            data: std::borrow::Cow::Owned(data.into_owned()),
        },
        _ => return Err(mcap::McapError::BadIndex.into()),
    };
    if reader.next().is_some() {
        return Err(mcap::McapError::BadIndex.into());
    }
    Ok(attachment)
}

fn select_attachment_index<'a>(
    indexes: &'a [mcap::records::AttachmentIndex],
    name: &str,
    offset: Option<u64>,
) -> Result<&'a mcap::records::AttachmentIndex> {
    let matches: Vec<&mcap::records::AttachmentIndex> =
        indexes.iter().filter(|index| index.name == name).collect();

    match matches.len() {
        0 => anyhow::bail!("attachment {name} not found"),
        1 => {
            let first_match = matches[0];
            if let Some(offset) = offset {
                if first_match.offset != offset {
                    anyhow::bail!("failed to find attachment {name} at offset {offset}");
                }
            }
            Ok(first_match)
        }
        _ => {
            let offset = offset.ok_or_else(|| {
                anyhow::anyhow!("multiple attachments named {name} exist (specify an offset)")
            })?;

            matches
                .into_iter()
                .find(|index| index.offset == offset)
                .ok_or_else(|| {
                    anyhow::anyhow!("failed to find attachment {name} at offset {offset}")
                })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::select_attachment_index;
    use mcap::records::AttachmentIndex;

    fn attachment(name: &str, offset: u64) -> AttachmentIndex {
        AttachmentIndex {
            offset,
            length: 1,
            log_time: 0,
            create_time: 0,
            data_size: 1,
            name: name.to_string(),
            media_type: "application/octet-stream".to_string(),
        }
    }

    #[test]
    fn selects_single_match_without_offset() {
        let indexes = vec![attachment("a", 10)];
        let selected =
            select_attachment_index(&indexes, "a", None).expect("attachment should resolve");
        assert_eq!(selected.offset, 10);
    }

    #[test]
    fn errors_when_name_not_found() {
        let indexes = vec![attachment("a", 10)];
        let err = select_attachment_index(&indexes, "b", None)
            .expect_err("missing attachment should error");
        assert_eq!(err.to_string(), "attachment b not found");
    }

    #[test]
    fn errors_when_duplicate_without_offset() {
        let indexes = vec![attachment("a", 10), attachment("a", 20)];
        let err = select_attachment_index(&indexes, "a", None)
            .expect_err("duplicate attachments need offset");
        assert_eq!(
            err.to_string(),
            "multiple attachments named a exist (specify an offset)"
        );
    }

    #[test]
    fn resolves_duplicate_with_matching_offset() {
        let indexes = vec![attachment("a", 10), attachment("a", 20)];
        let selected =
            select_attachment_index(&indexes, "a", Some(20)).expect("offset should disambiguate");
        assert_eq!(selected.offset, 20);
    }

    #[test]
    fn errors_when_duplicate_offset_missing() {
        let indexes = vec![attachment("a", 10), attachment("a", 20)];
        let err = select_attachment_index(&indexes, "a", Some(999))
            .expect_err("unknown offset should error");
        assert_eq!(err.to_string(), "failed to find attachment a at offset 999");
    }

    #[test]
    fn errors_when_single_match_has_different_offset() {
        let indexes = vec![attachment("a", 10)];
        let err = select_attachment_index(&indexes, "a", Some(999))
            .expect_err("single record should enforce provided offset");
        assert_eq!(err.to_string(), "failed to find attachment a at offset 999");
    }
}
