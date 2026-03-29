use anyhow::Result;
use mcap::Summary;

use crate::{cli::InputFile, cli_io, output};

pub fn run(args: InputFile) -> Result<()> {
    let mcap = cli_io::open_local_mcap(&args.file)?;
    let summary = Summary::read(&mcap)?.unwrap_or_default();

    let mut rows = Vec::with_capacity(summary.chunk_indexes.len() + 1);
    rows.push(vec![
        "offset".to_string(),
        "length".to_string(),
        "start".to_string(),
        "end".to_string(),
        "compression".to_string(),
        "compressed_size".to_string(),
        "uncompressed_size".to_string(),
        "compression_ratio".to_string(),
        "message_index_length".to_string(),
    ]);

    for chunk in summary.chunk_indexes {
        let ratio = if chunk.uncompressed_size == 0 {
            0.0
        } else {
            chunk.compressed_size as f64 / chunk.uncompressed_size as f64
        };
        rows.push(vec![
            chunk.chunk_start_offset.to_string(),
            chunk.chunk_length.to_string(),
            chunk.message_start_time.to_string(),
            chunk.message_end_time.to_string(),
            chunk.compression,
            chunk.compressed_size.to_string(),
            chunk.uncompressed_size.to_string(),
            format!("{ratio:.6}"),
            chunk.message_index_length.to_string(),
        ]);
    }

    output::print_rows(&rows)?;
    Ok(())
}
