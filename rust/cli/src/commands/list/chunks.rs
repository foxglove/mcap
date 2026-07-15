use anyhow::Result;

use crate::cli::{ListChunksCommand, TimeFormat};
use crate::context::CommandContext;
use crate::{render, source};

pub fn run(ctx: &CommandContext, args: ListChunksCommand) -> Result<()> {
    let parsed = source::parse_mcap_from_path(
        &args.file,
        source::SourceOptions::new(ctx.allow_remote_scan()),
    )?;
    render::print_table(&render_chunk_rows(&parsed.chunk_indexes, ctx.time_format()));
    Ok(())
}

fn render_chunk_rows(
    chunk_indexes: &[mcap::records::ChunkIndex],
    time_format: TimeFormat,
) -> Vec<Vec<String>> {
    let times = render::TimeRenderer::new(time_format);
    if let Some(first) = chunk_indexes.first() {
        times.prime(first.message_start_time);
    }

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
            times.format(chunk.message_start_time),
            times.format(chunk.message_end_time),
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
    use crate::cli::TimeFormat;
    use mcap::records::ChunkIndex;

    #[test]
    fn render_chunk_rows_includes_ratio() {
        let rows = render_chunk_rows(
            &[ChunkIndex {
                message_start_time: 10,
                message_end_time: 20,
                chunk_start_offset: 30,
                chunk_length: 40,
                message_index_offsets: BTreeMap::new(),
                message_index_length: 50,
                compression: "zstd".to_string(),
                compressed_size: 60,
                uncompressed_size: 120,
            }],
            TimeFormat::Auto,
        );
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
        assert_eq!(rows[1][2], "0.000000010");
        assert_eq!(rows[1][3], "0.000000020");
        assert_eq!(rows[1][7], "0.500000");
    }

    #[test]
    fn render_chunk_rows_primes_auto_from_first_chunk_start() {
        let rows = render_chunk_rows(
            &[
                ChunkIndex {
                    message_start_time: 1_490_149_580_103_843_113,
                    message_end_time: 1_490_149_580_203_843_113,
                    chunk_start_offset: 0,
                    chunk_length: 10,
                    message_index_offsets: BTreeMap::new(),
                    message_index_length: 0,
                    compression: "zstd".to_string(),
                    compressed_size: 1,
                    uncompressed_size: 2,
                },
                ChunkIndex {
                    // Pre-cutoff end time must stay in the latched RFC3339 mode.
                    message_start_time: 1_000_000_000,
                    message_end_time: 2_000_000_000,
                    chunk_start_offset: 10,
                    chunk_length: 10,
                    message_index_offsets: BTreeMap::new(),
                    message_index_length: 0,
                    compression: "zstd".to_string(),
                    compressed_size: 1,
                    uncompressed_size: 2,
                },
            ],
            TimeFormat::Auto,
        );
        assert_eq!(rows[1][2], "2017-03-22T02:26:20.103843113Z");
        assert_eq!(rows[2][2], "1970-01-01T00:00:01Z");
        assert_eq!(rows[2][3], "1970-01-01T00:00:02Z");
    }

    #[test]
    fn render_chunk_rows_honors_nanoseconds_format() {
        let rows = render_chunk_rows(
            &[ChunkIndex {
                message_start_time: 10,
                message_end_time: 20,
                chunk_start_offset: 0,
                chunk_length: 0,
                message_index_offsets: BTreeMap::new(),
                message_index_length: 0,
                compression: String::new(),
                compressed_size: 0,
                uncompressed_size: 0,
            }],
            TimeFormat::Nanoseconds,
        );
        assert_eq!(rows[1][2], "10");
        assert_eq!(rows[1][3], "20");
    }
}
