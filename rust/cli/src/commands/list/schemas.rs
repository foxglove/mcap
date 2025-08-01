use crate::error::CliResult;
use crate::utils::{format_table, validation::validate_input_file};
use clap::Args;
use mcap::Summary;
use memmap2::Mmap;
use std::fs::File;
use std::io::Write;

#[derive(Args)]
pub struct SchemasArgs {
    /// Path to MCAP file
    file: String,
}

pub async fn execute(args: SchemasArgs) -> CliResult<()> {
    // Validate input file
    validate_input_file(&args.file)?;

    // Memory map the file
    let file = File::open(&args.file)?;
    let mapped = unsafe { Mmap::map(&file)? };

    // Read the summary from the file
    let summary = Summary::read(&mapped)?;

    // Print the schemas
    print_schemas(&mut std::io::stdout(), &summary)?;

    Ok(())
}

fn print_schemas<W: Write>(writer: &mut W, summary: &Option<Summary>) -> std::io::Result<()> {
    if let Some(info) = summary {
        let headers = vec!["id", "name", "encoding"];
        let mut rows = Vec::new();

        // Convert HashMap to sorted Vec for consistent output
        let mut schemas: Vec<_> = info.schemas.iter().collect();
        schemas.sort_by_key(|(id, _)| *id);

        for (&schema_id, schema) in schemas {
            rows.push(vec![
                schema_id.to_string(),
                schema.name.clone(),
                schema.encoding.clone(),
            ]);
        }

        format_table(writer, headers, rows)?;
    } else {
        writeln!(writer, "No schema information available")?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schemas_args() {
        let args = SchemasArgs {
            file: "test.mcap".to_string(),
        };
        assert_eq!(args.file, "test.mcap");
    }
}
