use anyhow::Result;

use crate::cli::{ListAttachmentsCommand, TimeFormat};
use crate::context::CommandContext;
use crate::{parse, render, source};

pub fn run(ctx: &CommandContext, args: ListAttachmentsCommand) -> Result<()> {
    let source_options = source::SourceOptions::new(ctx.allow_remote_scan());
    let mut indexes =
        if let Some(remote) = source::try_open_remote_mcap(&args.file, source_options)? {
            remote.summary().attachment_indexes.clone()
        } else {
            let mcap = source::load_path(&args.file, source_options)?;
            let parsed = parse::parse_mcap(&mcap)?;
            if parse::attachment_indexes_need_scan(&parsed) {
                parse::warn_index_scan("attachment");
                parse::collect_attachment_indexes_linear(&mcap)?
            } else {
                parsed.attachment_indexes
            }
        };
    indexes.sort_by_key(|index| index.offset);
    render::print_table(&render_attachment_rows(&indexes, ctx.time_format()));
    Ok(())
}

fn render_attachment_rows(
    indexes: &[mcap::records::AttachmentIndex],
    time_format: TimeFormat,
) -> Vec<Vec<String>> {
    let times = render::TimeRenderer::new(time_format);
    if let Some(first) = indexes.first() {
        times.prime(first.log_time);
    }

    let mut rows = vec![vec![
        "name".to_string(),
        "media type".to_string(),
        "log time".to_string(),
        "creation time".to_string(),
        "content length".to_string(),
        "offset".to_string(),
    ]];

    for index in indexes {
        rows.push(vec![
            index.name.clone(),
            index.media_type.clone(),
            times.format(index.log_time),
            times.format(index.create_time),
            index.data_size.to_string(),
            index.offset.to_string(),
        ]);
    }
    rows
}

#[cfg(test)]
mod tests {
    use super::render_attachment_rows;
    use crate::cli::TimeFormat;
    use mcap::records::AttachmentIndex;

    #[test]
    fn render_rows_includes_attachment_data() {
        let rows = render_attachment_rows(
            &[AttachmentIndex {
                offset: 22,
                length: 10,
                log_time: 2,
                create_time: 3,
                data_size: 44,
                name: "demo.bin".to_string(),
                media_type: "application/octet-stream".to_string(),
            }],
            TimeFormat::Auto,
        );

        assert_eq!(
            rows[0],
            [
                "name",
                "media type",
                "log time",
                "creation time",
                "content length",
                "offset",
            ]
        );
        assert_eq!(rows[1][0], "demo.bin");
        assert_eq!(rows[1][2], "0.000000002");
        assert_eq!(rows[1][3], "0.000000003");
        assert_eq!(rows[1][5], "22");
    }

    #[test]
    fn render_rows_primes_auto_from_first_attachment_log_time() {
        let rows = render_attachment_rows(
            &[
                AttachmentIndex {
                    offset: 1,
                    length: 1,
                    log_time: 1_490_149_580_103_843_113,
                    create_time: 1_490_149_580_103_843_113,
                    data_size: 1,
                    name: "a.bin".to_string(),
                    media_type: "application/octet-stream".to_string(),
                },
                AttachmentIndex {
                    offset: 2,
                    length: 1,
                    log_time: 1_000_000_000,
                    create_time: 2_000_000_000,
                    data_size: 1,
                    name: "b.bin".to_string(),
                    media_type: "application/octet-stream".to_string(),
                },
            ],
            TimeFormat::Auto,
        );
        assert_eq!(rows[1][2], "2017-03-22T02:26:20.103843113Z");
        assert_eq!(rows[2][2], "1970-01-01T00:00:01Z");
        assert_eq!(rows[2][3], "1970-01-01T00:00:02Z");
    }

    #[test]
    fn render_rows_honors_seconds_format() {
        let rows = render_attachment_rows(
            &[AttachmentIndex {
                offset: 1,
                length: 1,
                log_time: 1_490_149_580_103_843_113,
                create_time: 1_490_149_580_103_843_113,
                data_size: 1,
                name: "a.bin".to_string(),
                media_type: "application/octet-stream".to_string(),
            }],
            TimeFormat::Seconds,
        );
        assert_eq!(rows[1][2], "1490149580.103843113");
    }
}
