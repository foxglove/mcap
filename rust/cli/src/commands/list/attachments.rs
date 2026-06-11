use anyhow::Result;

use crate::cli::ListAttachmentsCommand;
use crate::context::CommandContext;
use crate::{parse, render, source};

pub fn run(ctx: &CommandContext, args: ListAttachmentsCommand) -> Result<()> {
    let source_options = source::SourceOptions::new(ctx.allow_remote_scan());
    let mut indexes = if source::is_remote_url(&args.file) {
        source::parse_mcap_from_path(&args.file, source_options)?.attachment_indexes
    } else {
        let mcap = source::load_path(&args.file, source_options)?;
        let parsed = parse::parse_mcap(&mcap)?;
        if attachment_indexes_need_scan(&parsed) {
            parse::collect_attachment_indexes_linear(&mcap)?
        } else {
            parsed.attachment_indexes
        }
    };
    indexes.sort_by_key(|index| index.offset);
    render::print_table(&render_attachment_rows(&indexes));
    Ok(())
}

fn attachment_indexes_need_scan(parsed: &parse::ParsedMcap) -> bool {
    match &parsed.statistics {
        Some(statistics) => statistics.attachment_count as usize > parsed.attachment_indexes.len(),
        None => parsed.attachment_indexes.is_empty(),
    }
}

fn render_attachment_rows(indexes: &[mcap::records::AttachmentIndex]) -> Vec<Vec<String>> {
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
            render::raw_time(index.log_time),
            render::raw_time(index.create_time),
            index.data_size.to_string(),
            index.offset.to_string(),
        ]);
    }
    rows
}

#[cfg(test)]
mod tests {
    use super::render_attachment_rows;
    use mcap::records::AttachmentIndex;

    #[test]
    fn render_rows_includes_attachment_data() {
        let rows = render_attachment_rows(&[AttachmentIndex {
            offset: 22,
            length: 10,
            log_time: 2,
            create_time: 3,
            data_size: 44,
            name: "demo.bin".to_string(),
            media_type: "application/octet-stream".to_string(),
        }]);

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
        assert_eq!(rows[1][5], "22");
    }
}
