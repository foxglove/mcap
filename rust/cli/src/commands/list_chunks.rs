use anyhow::Result;

use crate::cli::ListChunksCommand;
use crate::commands::common;
use crate::context::CommandContext;

pub fn run(_ctx: &CommandContext, args: ListChunksCommand) -> Result<()> {
    let mcap = common::map_file(&args.file)?;
    let parsed = common::parse_mcap(&mcap)?;
    common::print_table(&render_chunk_rows(&parsed.chunk_indexes));
    Ok(())
}

fn render_chunk_rows(chunk_indexes: &[mcap::records::ChunkIndex]) -> Vec<Vec<String>> {
    let mut rows = vec![vec![
        "offset".to_string(),
        "length".to_string(),
        "start".to_string(),
        "end".to_string(),
        "compression".to_string(),
        "compressed size".to_string(),
        "uncompressed size".to_string(),
        "compression ratio".to_string(),
        "message index length".to_string(),
    ]];

    for chunk in chunk_indexes {
        let ratio = if chunk.uncompressed_size == 0 {
            0.0
        } else {
            chunk.compressed_size as f64 / chunk.uncompressed_size as f64
        };
        rows.push(vec![
            chunk.chunk_start_offset.to_string(),
            chunk.chunk_length.to_string(),
            common::raw_time(chunk.message_start_time),
            common::raw_time(chunk.message_end_time),
            chunk.compression.clone(),
            chunk.compressed_size.to_string(),
            chunk.uncompressed_size.to_string(),
            format!("{ratio:.6}"),
            chunk.message_index_length.to_string(),
        ]);
    }
    rows
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::render_chunk_rows;
    use mcap::records::ChunkIndex;

    #[test]
    fn render_chunk_rows_includes_ratio() {
        let rows = render_chunk_rows(&[ChunkIndex {
            message_start_time: 10,
            message_end_time: 20,
            chunk_start_offset: 30,
            chunk_length: 40,
            message_index_offsets: BTreeMap::new(),
            message_index_length: 50,
            compression: "zstd".to_string(),
            compressed_size: 60,
            uncompressed_size: 120,
        }]);
        assert_eq!(
            rows[0],
            [
                "offset",
                "length",
                "start",
                "end",
                "compression",
                "compressed size",
                "uncompressed size",
                "compression ratio",
                "message index length",
            ]
        );
        assert_eq!(rows[1][0], "30");
        assert_eq!(rows[1][2], "10");
        assert_eq!(rows[1][7], "0.500000");
    }
}
