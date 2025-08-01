use crate::error::CliResult;
use crate::utils::{
    format_bytes, format_decimal_time, format_duration, format_human_time,
    validation::validate_input_file,
};
use clap::Args;
use mcap::records::ChunkIndex;
use mcap::Summary;
use memmap2::Mmap;
use std::fs::File;
use std::io::Write;

#[derive(Args)]
pub struct InfoArgs {
    /// Path to MCAP file
    file: String,
}

pub async fn execute(args: InfoArgs) -> CliResult<()> {
    // Validate input file
    validate_input_file(&args.file)?;

    // Memory map the file
    let file = File::open(&args.file)?;
    let mapped = unsafe { Mmap::map(&file)? };

    // Read the summary from the file
    let summary = Summary::read(&mapped)?;

    // Print the information
    print_info(&mut std::io::stdout(), &summary)?;

    Ok(())
}

fn print_info<W: Write>(writer: &mut W, summary: &Option<Summary>) -> std::io::Result<()> {
    // For now, we'll use placeholder values for library and profile
    // TODO: Read actual header information from the file
    writeln!(writer, "library: ")?;
    writeln!(writer, "profile: ")?;

    if let Some(info) = summary {
        if let Some(stats) = &info.stats {
            writeln!(writer, "messages: {}", stats.message_count)?;

            if stats.message_count > 0 {
                let duration_ns = stats.message_end_time - stats.message_start_time;
                let duration_seconds = duration_ns as f64 / 1_000_000_000.0;

                writeln!(writer, "duration: {}", format_duration(duration_ns))?;
                writeln!(
                    writer,
                    "start: {} ({})",
                    format_human_time(stats.message_start_time),
                    format_decimal_time(stats.message_start_time)
                )?;
                writeln!(
                    writer,
                    "end: {} ({})",
                    format_human_time(stats.message_end_time),
                    format_decimal_time(stats.message_end_time)
                )?;

                if duration_seconds > 0.0 {
                    let message_rate = stats.message_count as f64 / duration_seconds;
                    writeln!(writer, "message rate: {:.2} Hz", message_rate)?;
                }
            }

            writeln!(writer, "topics: {}", stats.channel_count)?;
            writeln!(writer, "attachments: {}", info.attachment_indexes.len())?;
            writeln!(writer, "metadata: {}", info.metadata_indexes.len())?;
            writeln!(writer, "chunks: {}", info.chunk_indexes.len())?;

            if !info.chunk_indexes.is_empty() {
                let total_chunk_size: u64 = info
                    .chunk_indexes
                    .iter()
                    .map(|chunk| chunk.uncompressed_size)
                    .sum();
                writeln!(
                    writer,
                    "uncompressed size: {}",
                    format_bytes(total_chunk_size)
                )?;
            }

            // Print compression information if available
            if !info.chunk_indexes.is_empty() {
                let compressed_size: u64 = info
                    .chunk_indexes
                    .iter()
                    .map(|chunk| chunk.compressed_size)
                    .sum();
                let uncompressed_size: u64 = info
                    .chunk_indexes
                    .iter()
                    .map(|chunk| chunk.uncompressed_size)
                    .sum();

                if compressed_size > 0 && uncompressed_size > 0 {
                    let compression_ratio = compressed_size as f64 / uncompressed_size as f64;
                    writeln!(writer, "compressed size: {}", format_bytes(compressed_size))?;
                    writeln!(
                        writer,
                        "compression ratio: {:.2}%",
                        compression_ratio * 100.0
                    )?;
                }
            }

            // Check for chunk overlaps
            if info.chunk_indexes.len() > 1 {
                let (has_overlaps, max_chunks, max_size) =
                    count_chunk_overlaps(&info.chunk_indexes);
                if has_overlaps {
                    writeln!(writer, "chunk time overlaps: true")?;
                    writeln!(writer, "max overlapping chunks: {}", max_chunks)?;
                    writeln!(writer, "max overlapping size: {}", format_bytes(max_size))?;
                } else {
                    writeln!(writer, "chunk time overlaps: false")?;
                }
            }
        }
    }

    Ok(())
}

struct ChunkEvent {
    time: u64,
    is_start: bool,
    uncompressed_size: u64,
}

fn count_chunk_overlaps(chunks: &[ChunkIndex]) -> (bool, usize, u64) {
    if chunks.len() < 2 {
        return match chunks.len() {
            1 => (false, 1, chunks[0].uncompressed_size),
            _ => (false, 0, 0),
        };
    }

    // Create start and end events for each chunk
    let mut events = Vec::with_capacity(chunks.len() * 2);
    for chunk in chunks {
        events.push(ChunkEvent {
            time: chunk.message_start_time,
            is_start: true,
            uncompressed_size: chunk.uncompressed_size,
        });
        events.push(ChunkEvent {
            time: chunk.message_end_time,
            is_start: false,
            uncompressed_size: chunk.uncompressed_size,
        });
    }

    // Sort events by time, with starts before ends at the same time
    events.sort_by(|a, b| {
        match a.time.cmp(&b.time) {
            std::cmp::Ordering::Equal => {
                // If times are equal, process starts before ends
                if a.is_start && !b.is_start {
                    std::cmp::Ordering::Less
                } else if !a.is_start && b.is_start {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Equal
                }
            }
            other => other,
        }
    });

    // Sweep through events tracking max active chunks and total size
    let mut active_chunks = 0;
    let mut max_active_chunks = 0;
    let mut current_total_size = 0u64;
    let mut max_total_uncompressed_size = 0u64;

    for event in &events {
        if event.is_start {
            active_chunks += 1;
            current_total_size += event.uncompressed_size;
            if active_chunks > max_active_chunks
                || (active_chunks == max_active_chunks
                    && current_total_size > max_total_uncompressed_size)
            {
                max_active_chunks = active_chunks;
                max_total_uncompressed_size = current_total_size;
            }
        } else {
            active_chunks -= 1;
            current_total_size -= event.uncompressed_size;
        }
    }

    (
        max_active_chunks > 1,
        max_active_chunks,
        max_total_uncompressed_size,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn test_count_chunk_overlaps_no_chunks() {
        let chunks = vec![];
        let (has_overlaps, max_chunks, max_size) = count_chunk_overlaps(&chunks);
        assert!(!has_overlaps);
        assert_eq!(max_chunks, 0);
        assert_eq!(max_size, 0);
    }

    #[test]
    fn test_count_chunk_overlaps_single_chunk() {
        let chunks = vec![ChunkIndex {
            message_start_time: 100,
            message_end_time: 200,
            chunk_start_offset: 0,
            chunk_length: 1000,
            uncompressed_size: 1000,
            compressed_size: 800,
            message_index_offsets: BTreeMap::new(),
            message_index_length: 0,
            compression: "".to_string(),
        }];
        let (has_overlaps, max_chunks, max_size) = count_chunk_overlaps(&chunks);
        assert!(!has_overlaps);
        assert_eq!(max_chunks, 1);
        assert_eq!(max_size, 1000);
    }
}
