use anyhow::Result;

use crate::cli::ListAttachmentsCommand;
use crate::commands::common;
use crate::context::CommandContext;

pub fn run(_ctx: &CommandContext, args: ListAttachmentsCommand) -> Result<()> {
    let mcap = common::map_file(&args.file)?;
    let parsed = common::parse_mcap(&mcap)?;
    let mut indexes = parsed.attachment_indexes;
    indexes.sort_by_key(|index| index.offset);
    common::print_table(&render_attachment_rows(&indexes));
    Ok(())
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
            common::formatted_time(index.log_time),
            common::formatted_time(index.create_time),
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
