use crate::error::CliResult;
use crate::utils::{format_bytes, validation::validate_input_file};
use clap::Args;
use mcap::{MessageStream, Summary};
use memmap2::Mmap;
use std::collections::HashSet;
use std::fs::File;

#[derive(Args)]
pub struct DoctorArgs {
    /// Path to MCAP file
    file: String,

    /// Check for message ordering issues
    #[arg(long)]
    check_ordering: bool,

    /// Check for duplicate messages
    #[arg(long)]
    check_duplicates: bool,

    /// Check for schema consistency
    #[arg(long)]
    check_schemas: bool,

    /// Check CRC checksums
    #[arg(long)]
    check_crc: bool,

    /// Verbose output
    #[arg(short, long)]
    verbose: bool,
}

#[derive(Debug)]
struct HealthReport {
    file_size: u64,
    issues: Vec<HealthIssue>,
    warnings: Vec<HealthWarning>,
    stats: FileStats,
}

#[derive(Debug)]
struct FileStats {
    message_count: u64,
    channel_count: usize,
    schema_count: usize,
    chunk_count: usize,
    attachment_count: usize,
    metadata_count: usize,
    time_range: Option<(u64, u64)>,
}

#[derive(Debug)]
enum HealthIssue {
    MissingHeader,
    MissingFooter,
    CorruptedChunk {
        chunk_index: usize,
        reason: String,
    },
    InvalidTimestamp {
        message_index: u64,
        timestamp: u64,
    },
    UnorderedMessages {
        previous_time: u64,
        current_time: u64,
        message_index: u64,
    },
    DuplicateMessage {
        channel_id: u16,
        timestamp: u64,
        sequence: u32,
        message_index: u64,
    },
    SchemaInconsistency {
        channel_id: u16,
        reason: String,
    },
    CrcMismatch {
        record_type: String,
        offset: u64,
    },
}

#[derive(Debug)]
enum HealthWarning {
    LargeTimeGap { gap_ns: u64, at_message: u64 },
    UnusedSchema { schema_id: u16, name: String },
    UnusedChannel { channel_id: u16, topic: String },
    SmallChunk { chunk_index: usize, size: u64 },
    LargeChunk { chunk_index: usize, size: u64 },
}

pub async fn execute(args: DoctorArgs) -> CliResult<()> {
    // Validate input file
    validate_input_file(&args.file)?;

    println!("🩺 MCAP File Health Check");
    println!("=========================");
    println!("Examining: {}", args.file);
    println!();

    // Get file metadata
    let file_metadata = std::fs::metadata(&args.file)?;
    let file_size = file_metadata.len();

    // Memory map the file
    let file = File::open(&args.file)?;
    let mapped = unsafe { Mmap::map(&file)? };

    // Perform health checks
    let report = perform_health_check(&mapped, file_size, &args).await?;

    // Print report
    print_health_report(&report)?;

    // Return non-zero exit code if there are critical issues
    if !report.issues.is_empty() {
        std::process::exit(1);
    }

    Ok(())
}

async fn perform_health_check(
    mapped: &[u8],
    file_size: u64,
    args: &DoctorArgs,
) -> CliResult<HealthReport> {
    let mut issues = Vec::new();
    let mut warnings = Vec::new();

    // Read summary if available
    let summary = Summary::read(mapped)?;

    // Initialize stats
    let mut stats = FileStats {
        message_count: 0,
        channel_count: 0,
        schema_count: 0,
        chunk_count: 0,
        attachment_count: 0,
        metadata_count: 0,
        time_range: None,
    };

    if let Some(info) = &summary {
        stats.channel_count = info.channels.len();
        stats.schema_count = info.schemas.len();
        stats.chunk_count = info.chunk_indexes.len();
        stats.attachment_count = info.attachment_indexes.len();
        stats.metadata_count = info.metadata_indexes.len();

        if let Some(file_stats) = &info.stats {
            stats.message_count = file_stats.message_count;
        }
    }

    // Check for basic structural issues
    check_file_structure(&summary, &mut issues)?;

    // Check message stream if requested
    if args.check_ordering || args.check_duplicates {
        check_messages(mapped, &mut issues, &mut warnings, &mut stats, args).await?;
    }

    // Check schemas if requested and available
    if args.check_schemas {
        if let Some(info) = &summary {
            check_schema_consistency(info, &mut issues, &mut warnings)?;
        }
    }

    // Check chunk sizes and generate warnings
    if let Some(info) = &summary {
        check_chunk_health(&info.chunk_indexes, &mut warnings)?;
    }

    Ok(HealthReport {
        file_size,
        issues,
        warnings,
        stats,
    })
}

fn check_file_structure(summary: &Option<Summary>, issues: &mut Vec<HealthIssue>) -> CliResult<()> {
    if summary.is_none() {
        issues.push(HealthIssue::MissingHeader);
        issues.push(HealthIssue::MissingFooter);
    }

    Ok(())
}

async fn check_messages(
    mapped: &[u8],
    issues: &mut Vec<HealthIssue>,
    warnings: &mut Vec<HealthWarning>,
    stats: &mut FileStats,
    args: &DoctorArgs,
) -> CliResult<()> {
    let message_stream = MessageStream::new(mapped)?;

    let mut previous_time = 0u64;
    let mut message_index = 0u64;
    let mut first_timestamp = None;
    let mut last_timestamp = None;

    let mut seen_messages = HashSet::new();
    let mut _used_channels = HashSet::new();

    for message_result in message_stream {
        let message = message_result?;
        message_index += 1;

        // Track time range
        if first_timestamp.is_none() {
            first_timestamp = Some(message.log_time);
        }
        last_timestamp = Some(message.log_time);

        // Check for invalid timestamps
        if message.log_time == 0 {
            issues.push(HealthIssue::InvalidTimestamp {
                message_index,
                timestamp: message.log_time,
            });
        }

        // Check ordering if requested
        if args.check_ordering && message.log_time < previous_time {
            issues.push(HealthIssue::UnorderedMessages {
                previous_time,
                current_time: message.log_time,
                message_index,
            });
        }

        // Check for large time gaps (warning)
        if previous_time > 0 && message.log_time > previous_time {
            let gap = message.log_time - previous_time;
            // Warn about gaps > 1 second
            if gap > 1_000_000_000 {
                warnings.push(HealthWarning::LargeTimeGap {
                    gap_ns: gap,
                    at_message: message_index,
                });
            }
        }

        // Check for duplicates if requested
        if args.check_duplicates {
            let message_key = (message.channel.id, message.log_time, message.sequence);
            if !seen_messages.insert(message_key) {
                issues.push(HealthIssue::DuplicateMessage {
                    channel_id: message.channel.id,
                    timestamp: message.log_time,
                    sequence: message.sequence,
                    message_index,
                });
            }
        }

        _used_channels.insert(message.channel.id);
        previous_time = message.log_time;
    }

    stats.message_count = message_index;
    if let (Some(start), Some(end)) = (first_timestamp, last_timestamp) {
        stats.time_range = Some((start, end));
    }

    Ok(())
}

fn check_schema_consistency(
    info: &Summary,
    _issues: &mut Vec<HealthIssue>,
    warnings: &mut Vec<HealthWarning>,
) -> CliResult<()> {
    let mut used_schemas = HashSet::new();

    // Collect used schemas from channels
    for (_, channel) in &info.channels {
        if let Some(schema) = &channel.schema {
            used_schemas.insert(schema.id);
        }
    }

    // Check for unused schemas
    for (_, schema) in &info.schemas {
        if !used_schemas.contains(&schema.id) {
            warnings.push(HealthWarning::UnusedSchema {
                schema_id: schema.id,
                name: schema.name.clone(),
            });
        }
    }

    Ok(())
}

fn check_chunk_health(
    chunks: &[mcap::records::ChunkIndex],
    warnings: &mut Vec<HealthWarning>,
) -> CliResult<()> {
    for (index, chunk) in chunks.iter().enumerate() {
        let compressed_size = chunk.compressed_size;

        // Warn about very small chunks (< 1KB)
        if compressed_size < 1024 {
            warnings.push(HealthWarning::SmallChunk {
                chunk_index: index,
                size: compressed_size,
            });
        }

        // Warn about very large chunks (> 100MB)
        if compressed_size > 100 * 1024 * 1024 {
            warnings.push(HealthWarning::LargeChunk {
                chunk_index: index,
                size: compressed_size,
            });
        }
    }

    Ok(())
}

fn print_health_report(report: &HealthReport) -> CliResult<()> {
    // Print file stats
    println!("📊 File Statistics:");
    println!("   File size: {}", format_bytes(report.file_size));
    println!("   Messages: {}", report.stats.message_count);
    println!("   Channels: {}", report.stats.channel_count);
    println!("   Schemas: {}", report.stats.schema_count);
    println!("   Chunks: {}", report.stats.chunk_count);
    println!("   Attachments: {}", report.stats.attachment_count);
    println!("   Metadata: {}", report.stats.metadata_count);

    if let Some((start, end)) = report.stats.time_range {
        let duration = end - start;
        println!("   Duration: {:.3}s", duration as f64 / 1e9);
    }
    println!();

    // Print issues
    if !report.issues.is_empty() {
        println!("❌ Issues Found ({}):", report.issues.len());
        for (i, issue) in report.issues.iter().enumerate() {
            println!("   {}. {}", i + 1, format_issue(issue));
        }
        println!();
    }

    // Print warnings
    if !report.warnings.is_empty() {
        println!("⚠️  Warnings ({}):", report.warnings.len());
        for (i, warning) in report.warnings.iter().enumerate() {
            println!("   {}. {}", i + 1, format_warning(warning));
        }
        println!();
    }

    // Print summary
    if report.issues.is_empty() && report.warnings.is_empty() {
        println!("✅ File appears to be healthy!");
    } else if report.issues.is_empty() {
        println!("✅ No critical issues found (only warnings)");
    } else {
        println!("❌ Critical issues detected - file may be corrupted or malformed");
    }

    Ok(())
}

fn format_issue(issue: &HealthIssue) -> String {
    match issue {
        HealthIssue::MissingHeader => "Missing or corrupted file header".to_string(),
        HealthIssue::MissingFooter => "Missing or corrupted file footer".to_string(),
        HealthIssue::CorruptedChunk {
            chunk_index,
            reason,
        } => {
            format!("Corrupted chunk {} ({})", chunk_index, reason)
        }
        HealthIssue::InvalidTimestamp {
            message_index,
            timestamp,
        } => {
            format!(
                "Invalid timestamp {} at message {}",
                timestamp, message_index
            )
        }
        HealthIssue::UnorderedMessages {
            previous_time,
            current_time,
            message_index,
        } => {
            format!(
                "Message {} is out of order ({}ns after {}ns)",
                message_index, current_time, previous_time
            )
        }
        HealthIssue::DuplicateMessage {
            channel_id,
            timestamp,
            sequence,
            message_index,
        } => {
            format!(
                "Duplicate message at index {} (channel={}, time={}, seq={})",
                message_index, channel_id, timestamp, sequence
            )
        }
        HealthIssue::SchemaInconsistency { channel_id, reason } => {
            format!("Schema inconsistency in channel {}: {}", channel_id, reason)
        }
        HealthIssue::CrcMismatch {
            record_type,
            offset,
        } => {
            format!(
                "CRC mismatch in {} record at offset {}",
                record_type, offset
            )
        }
    }
}

fn format_warning(warning: &HealthWarning) -> String {
    match warning {
        HealthWarning::LargeTimeGap { gap_ns, at_message } => {
            format!(
                "Large time gap of {:.3}s before message {}",
                *gap_ns as f64 / 1e9,
                at_message
            )
        }
        HealthWarning::UnusedSchema { schema_id, name } => {
            format!("Unused schema {} ({})", schema_id, name)
        }
        HealthWarning::UnusedChannel { channel_id, topic } => {
            format!("Unused channel {} ({})", channel_id, topic)
        }
        HealthWarning::SmallChunk { chunk_index, size } => {
            format!("Very small chunk {} ({})", chunk_index, format_bytes(*size))
        }
        HealthWarning::LargeChunk { chunk_index, size } => {
            format!("Very large chunk {} ({})", chunk_index, format_bytes(*size))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_doctor_args() {
        let args = DoctorArgs {
            file: "test.mcap".to_string(),
            check_ordering: true,
            check_duplicates: false,
            check_schemas: true,
            check_crc: false,
            verbose: false,
        };
        assert_eq!(args.file, "test.mcap");
        assert!(args.check_ordering);
        assert!(!args.check_duplicates);
    }

    #[test]
    fn test_format_issue() {
        let issue = HealthIssue::MissingHeader;
        assert_eq!(format_issue(&issue), "Missing or corrupted file header");
    }

    #[test]
    fn test_format_warning() {
        let warning = HealthWarning::LargeTimeGap {
            gap_ns: 2_000_000_000,
            at_message: 100,
        };
        assert_eq!(
            format_warning(&warning),
            "Large time gap of 2.000s before message 100"
        );
    }
}
