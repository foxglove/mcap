use crate::cli::ListSchemasCommand;
use crate::commands::common;
use crate::context::CommandContext;
use anyhow::Result;

pub fn run(_ctx: &CommandContext, args: ListSchemasCommand) -> Result<()> {
    let mcap = common::read_file(&args.file)?;
    let parsed = common::parse_mcap(&mcap)?;
    common::print_table(&render_schema_rows(&parsed.schemas));
    Ok(())
}

fn render_schema_rows(
    schemas: &std::collections::BTreeMap<u16, crate::commands::common::ParsedSchema>,
) -> Vec<Vec<String>> {
    let mut rows = vec![vec![
        "id".to_string(),
        "name".to_string(),
        "encoding".to_string(),
        "data".to_string(),
    ]];

    for schema in schemas.values() {
        rows.push(vec![
            schema.header.id.to_string(),
            schema.header.name.clone(),
            schema.header.encoding.clone(),
            String::from_utf8_lossy(schema.data.as_slice()).to_string(),
        ]);
    }
    rows
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::render_schema_rows;
    use crate::commands::common::ParsedSchema;
    use mcap::records::SchemaHeader;

    #[test]
    fn render_rows_includes_header_and_schema_data() {
        let mut schemas = BTreeMap::new();
        schemas.insert(
            2,
            ParsedSchema {
                header: SchemaHeader {
                    id: 2,
                    name: "demo".to_string(),
                    encoding: "jsonschema".to_string(),
                },
                data: br#"{"type":"object"}"#.to_vec(),
            },
        );

        let rows = render_schema_rows(&schemas);
        assert_eq!(rows[0], ["id", "name", "encoding", "data"]);
        assert_eq!(rows[1][0], "2");
        assert_eq!(rows[1][1], "demo");
        assert_eq!(rows[1][3], r#"{"type":"object"}"#);
    }
}
