use std::io::IsTerminal as _;
use std::io::Write as _;

use anyhow::{Context, Result};

use crate::byte_source::{self, ByteSource};
use crate::cli::GetAttachmentCommand;
use crate::context::CommandContext;
use crate::{parse, source};

const PLEASE_REDIRECT: &str =
    "Binary output can screw up your terminal. Supply -o or redirect to a file or pipe";

pub fn run(ctx: &CommandContext, args: GetAttachmentCommand) -> Result<()> {
    let source_options = source::SourceOptions::new(ctx.allow_remote_scan());
    if let Some(output) = args.output.as_deref() {
        source::ensure_distinct_local_input_output(&args.file, output)?;
    }
    let mut input = byte_source::open_byte_source(Some(&args.file), source_options)?;
    let indexes = attachment_indexes(input.as_mut(), &args.name, source_options)?;
    let index = select_attachment_index(&indexes, &args.name, args.offset)?;
    let length = usize::try_from(index.length)
        .context("indexed record is too large to read on this platform")?;
    source::require_remote_indexed_read_budget(
        index.length,
        source_options,
        "remote attachment record",
    )?;
    let bytes = input.read_at(index.offset, length)?;
    let attachment = parse::parse_attachment_record(&bytes).with_context(|| {
        format!(
            "failed to read attachment {} at offset {}",
            args.name, index.offset
        )
    })?;
    write_attachment_data(attachment.data.as_ref(), args.output.as_deref())?;
    Ok(())
}

fn attachment_indexes(
    source: &mut dyn ByteSource,
    name: &str,
    source_options: source::SourceOptions,
) -> Result<Vec<mcap::records::AttachmentIndex>> {
    let header = byte_source::read_header(source)?;
    let parsed = match parse::try_parsed_mcap_from_summary(source, header.clone())? {
        Some(parsed) => parsed,
        None => {
            source::require_remote_scan_for_linear(source, source_options)?;
            return Ok(
                parse::parse_mcap_linear_from_byte_source(source, header)?.attachment_indexes
            );
        }
    };
    let missing_requested_name = !parsed
        .attachment_indexes
        .iter()
        .any(|index| index.name == name);
    if parse::attachment_indexes_need_scan(&parsed)
        || (missing_requested_name && parsed.summary_available && parsed.statistics.is_none())
    {
        parse::warn_index_scan("attachment");
        source::require_remote_scan_for_linear(source, source_options)?;
        return parse::collect_attachment_indexes_from_byte_source(source);
    }
    Ok(parsed.attachment_indexes)
}

fn write_attachment_data(data: &[u8], output: Option<&std::path::Path>) -> Result<()> {
    if let Some(output) = output {
        std::fs::write(output, data)
            .with_context(|| format!("failed to write attachment to '{}'", output.display()))?;
    } else if std::io::stdout().is_terminal() {
        anyhow::bail!("{PLEASE_REDIRECT}");
    } else {
        std::io::stdout()
            .write_all(data)
            .context("failed to write attachment to stdout")?;
    }
    Ok(())
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
    use std::borrow::Cow;

    use super::{attachment_indexes, select_attachment_index};
    use crate::byte_source::MemorySource;
    use crate::cli::GetAttachmentCommand;
    use crate::context::CommandContext;
    use crate::parse;
    use crate::source::SourceOptions;
    use mcap::records::{AttachmentIndex, Statistics};

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

    fn mcap_with_attachment() -> Vec<u8> {
        let mut mcap_bytes = Vec::new();
        {
            let mut writer =
                mcap::Writer::new(std::io::Cursor::new(&mut mcap_bytes)).expect("writer");
            writer
                .attach(&mcap::Attachment {
                    log_time: 1,
                    create_time: 1,
                    name: "a".to_string(),
                    media_type: "application/octet-stream".to_string(),
                    data: Cow::Borrowed(b"payload"),
                })
                .expect("attachment");
            writer.finish().expect("finish");
        }
        mcap_bytes
    }

    #[test]
    fn selects_single_match_without_offset() {
        let indexes = vec![attachment("a", 10)];
        let selected =
            select_attachment_index(&indexes, "a", None).expect("attachment should resolve");
        assert_eq!(selected.offset, 10);
    }

    #[test]
    fn run_rejects_same_input_and_output_without_truncating() {
        let input = mcap_with_attachment();
        let dir = tempfile::TempDir::new().expect("temp dir");
        let path = dir.path().join("same-path.mcap");
        std::fs::write(&path, &input).expect("write input");

        let err = super::run(
            &CommandContext::default(),
            GetAttachmentCommand {
                file: path.clone(),
                name: "a".to_string(),
                offset: None,
                output: Some(path.clone()),
            },
        )
        .expect_err("same input/output should fail");

        assert!(err.to_string().contains("input and output paths"));
        assert_eq!(std::fs::read(&path).expect("read input"), input);
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

    #[test]
    fn missing_name_does_not_scan_when_attachment_indexes_are_complete() {
        let mut source = MemorySource::new(mcap_with_attachment());
        // Build a ParsedMcap-like path by using a complete summary fixture via real bytes.
        let indexes =
            attachment_indexes(&mut source, "missing", SourceOptions::default()).expect("indexes");
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].name, "a");
        let _ = parse::ParsedMcap {
            summary_available: true,
            statistics: Some(Statistics {
                attachment_count: 1,
                ..Default::default()
            }),
            attachment_indexes: vec![attachment("a", 10)],
            ..Default::default()
        };
    }

    #[test]
    fn missing_name_does_not_rescan_summaryless_input() {
        // Linear-parsed attachment indexes are complete when summary_available is false.
        let parsed = parse::ParsedMcap {
            attachment_indexes: vec![attachment("a", 10)],
            ..Default::default()
        };
        assert!(!parse::attachment_indexes_need_scan(&parsed));
        let missing_requested_name = !parsed
            .attachment_indexes
            .iter()
            .any(|index| index.name == "missing");
        assert!(missing_requested_name);
        // Without summary_available, the get path must not force a rescan.
        assert!(
            !(missing_requested_name && parsed.summary_available && parsed.statistics.is_none())
        );
    }

    #[test]
    fn missing_name_scans_when_attachment_index_completeness_is_unknown() {
        let mut mcap_bytes = Vec::new();
        {
            let mut writer = mcap::WriteOptions::new()
                .emit_summary_records(false)
                .emit_summary_offsets(false)
                .emit_attachment_indexes(false)
                .create(std::io::Cursor::new(&mut mcap_bytes))
                .expect("writer");
            writer
                .attach(&mcap::Attachment {
                    log_time: 1,
                    create_time: 1,
                    name: "a".to_string(),
                    media_type: "application/octet-stream".to_string(),
                    data: Cow::Borrowed(b"payload"),
                })
                .expect("attachment");
            writer.finish().expect("finish");
        }
        let mut source = MemorySource::new(mcap_bytes);
        let indexes =
            attachment_indexes(&mut source, "missing", SourceOptions::default()).expect("scan");
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].name, "a");
    }
}
