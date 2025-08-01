use crate::error::CliResult;
use crate::utils::{format_bytes, format_table, validation::validate_input_file};
use clap::Args;
use mcap::Summary;
use memmap2::Mmap;
use std::fs::File;
use std::io::Write;

#[derive(Args)]
pub struct ChunksArgs {
    /// Path to MCAP file
    file: String,
}

pub async fn execute(args: ChunksArgs) -> CliResult<()> {
    // Validate input file
    validate_input_file(&args.file)?;

    // Memory map the file
    let file = File::open(&args.file)?;
    let mapped = unsafe { Mmap::map(&file)? };

    // Read the summary from the file
    let summary = Summary::read(&mapped)?;

    // Print the chunks
    print_chunks(&mut std::io::stdout(), &summary)?;

    Ok(())
}

fn print_chunks<W: Write>(writer: &mut W, summary: &Option<Summary>) -> std::io::Result<()> {
    if let Some(info) = summary {
        if info.chunk_indexes.is_empty() {
            writeln!(writer, "No chunks found")?;
            return Ok(());
        }

        let headers = vec![
            "offset",
            "length",
            "start_time",
            "end_time",
            "uncompressed_size",
            "compressed_size",
            "compression",
        ];
        let mut rows = Vec::new();

        for chunk in &info.chunk_indexes {
            rows.push(vec![
                chunk.chunk_start_offset.to_string(),
                chunk.chunk_length.to_string(),
                chunk.message_start_time.to_string(),
                chunk.message_end_time.to_string(),
                format_bytes(chunk.uncompressed_size),
                format_bytes(chunk.compressed_size),
                chunk.compression.clone(),
            ]);
        }

        format_table(writer, headers, rows)?;
    } else {
        writeln!(writer, "No chunk information available")?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunks_args() {
        let args = ChunksArgs {
            file: "test.mcap".to_string(),
        };
        assert_eq!(args.file, "test.mcap");
    }
}
