use crate::error::CliResult;
use crate::utils::{format_table, validation::validate_input_file};
use clap::Args;
use mcap::Summary;
use memmap2::Mmap;
use std::fs::File;
use std::io::Write;

#[derive(Args)]
pub struct MetadataArgs {
    /// Path to MCAP file
    file: String,
}

pub async fn execute(args: MetadataArgs) -> CliResult<()> {
    // Validate input file
    validate_input_file(&args.file)?;

    // Memory map the file
    let file = File::open(&args.file)?;
    let mapped = unsafe { Mmap::map(&file)? };

    // Read the summary from the file
    let summary = Summary::read(&mapped)?;

    // Print the metadata
    print_metadata(&mut std::io::stdout(), &summary)?;

    Ok(())
}

fn print_metadata<W: Write>(writer: &mut W, summary: &Option<Summary>) -> std::io::Result<()> {
    if let Some(info) = summary {
        if info.metadata_indexes.is_empty() {
            writeln!(writer, "No metadata found")?;
            return Ok(());
        }

        let headers = vec!["offset", "length", "name"];
        let mut rows = Vec::new();

        for metadata in &info.metadata_indexes {
            rows.push(vec![
                metadata.offset.to_string(),
                metadata.length.to_string(),
                metadata.name.clone(),
            ]);
        }

        format_table(writer, headers, rows)?;
    } else {
        writeln!(writer, "No metadata information available")?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metadata_args() {
        let args = MetadataArgs {
            file: "test.mcap".to_string(),
        };
        assert_eq!(args.file, "test.mcap");
    }
}
