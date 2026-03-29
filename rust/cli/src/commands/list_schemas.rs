use std::{collections::BTreeMap, sync::Arc};

use anyhow::{Context, Result};
use mcap::Schema;

use crate::{cli::InputFile, cli_io::open_local_mcap, output::print_rows};

pub fn run(input: InputFile) -> Result<()> {
    let bytes = open_local_mcap(&input.file)?;
    let summary = mcap::Summary::read(&bytes)
        .with_context(|| format!("failed to parse summary from {}", input.file.display()))?;
    let Some(summary) = summary else {
        anyhow::bail!(
            "file does not contain a summary section: {}",
            input.file.display()
        );
    };

    print_schemas_to_stdout(summary.schemas.values())?;
    Ok(())
}

fn print_schemas_to_stdout<'a>(
    schemas: impl Iterator<Item = &'a Arc<Schema<'static>>>,
) -> Result<()> {
    let mut ordered = BTreeMap::new();
    for schema in schemas {
        ordered.insert(schema.id, schema.clone());
    }

    let mut rows = vec![vec![
        "id".to_string(),
        "name".to_string(),
        "encoding".to_string(),
        "data_len".to_string(),
    ]];
    for (_id, schema) in ordered {
        rows.push(vec![
            schema.id.to_string(),
            schema.name.clone(),
            schema.encoding.clone(),
            schema.data.len().to_string(),
        ]);
    }

    print_rows(&rows)
}

#[cfg(test)]
fn collect_schema_rows<'a>(
    schemas: impl Iterator<Item = &'a Arc<Schema<'static>>>,
) -> Vec<Vec<String>> {
    let mut ordered = BTreeMap::new();
    for schema in schemas {
        ordered.insert(schema.id, schema.clone());
    }

    let mut rows = vec![vec![
        "id".to_string(),
        "name".to_string(),
        "encoding".to_string(),
        "data_len".to_string(),
    ]];
    for (_id, schema) in ordered {
        rows.push(vec![
            schema.id.to_string(),
            schema.name.clone(),
            schema.encoding.clone(),
            schema.data.len().to_string(),
        ]);
    }
    rows
}

#[cfg(test)]
mod tests {
    use super::collect_schema_rows;
    use mcap::Schema;
    use std::{borrow::Cow, collections::BTreeMap, sync::Arc};

    #[test]
    fn schema_rows_are_sorted_by_id() {
        let schemas = vec![
            Arc::new(Schema {
                id: 2,
                name: "pkg/B".to_string(),
                encoding: "jsonschema".to_string(),
                data: Cow::Owned(vec![1, 2, 3]),
            }),
            Arc::new(Schema {
                id: 1,
                name: "pkg/A".to_string(),
                encoding: "protobuf".to_string(),
                data: Cow::Owned(vec![9]),
            }),
        ];
        let ordered: BTreeMap<_, _> = schemas.into_iter().map(|s| (s.id, s)).collect();
        let rows = collect_schema_rows(ordered.values());
        assert_eq!(rows[1][0], "1");
        assert_eq!(rows[2][0], "2");
    }
}
