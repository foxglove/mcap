use anyhow::Result;

use crate::cli::ListChannelsCommand;
use crate::commands::common;
use crate::context::CommandContext;

pub fn run(_ctx: &CommandContext, args: ListChannelsCommand) -> Result<()> {
    let mcap = common::read_file(&args.file)?;
    let parsed = common::parse_mcap(&mcap)?;
    common::print_table(&render_channel_rows(&parsed.channels)?);
    Ok(())
}

fn render_channel_rows(
    channels: &std::collections::BTreeMap<u16, mcap::records::Channel>,
) -> Result<Vec<Vec<String>>> {
    let mut rows = vec![vec![
        "id".to_string(),
        "schemaId".to_string(),
        "topic".to_string(),
        "messageEncoding".to_string(),
        "metadata".to_string(),
    ]];

    for channel in channels.values() {
        rows.push(vec![
            channel.id.to_string(),
            channel.schema_id.to_string(),
            channel.topic.clone(),
            channel.message_encoding.clone(),
            serde_json::to_string(&channel.metadata)?,
        ]);
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::render_channel_rows;
    use mcap::records;

    #[test]
    fn render_rows_includes_header_and_metadata_json() {
        let mut channels = BTreeMap::new();
        channels.insert(
            5,
            records::Channel {
                id: 5,
                schema_id: 2,
                topic: "/demo".to_string(),
                message_encoding: "cdr".to_string(),
                metadata: BTreeMap::from([("k".to_string(), "v".to_string())]),
            },
        );

        let rows = render_channel_rows(&channels).expect("rows should render");
        assert_eq!(
            rows[0],
            ["id", "schemaId", "topic", "messageEncoding", "metadata"]
        );
        assert_eq!(rows[1][0], "5");
        assert_eq!(rows[1][2], "/demo");
        assert_eq!(rows[1][4], r#"{"k":"v"}"#);
    }
}
