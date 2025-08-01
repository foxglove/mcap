use crate::error::CliResult;
use crate::utils::validation::{validate_input_file, validate_output_file};
use clap::Args;
use mcap::{WriteOptions, Writer};
use std::fs::File;
use std::io::{BufWriter, Seek, Write};

#[derive(Args)]
pub struct ConvertArgs {
    /// Input file to convert
    input: String,

    /// Output MCAP file
    #[arg(short, long)]
    output: String,

    /// Input format (rosbag1, rosbag2, db3)
    #[arg(long, default_value = "auto")]
    input_format: String,

    /// Compression format for output (none, lz4, zstd)
    #[arg(long, default_value = "zstd")]
    compression: String,

    /// Chunk size for output file
    #[arg(long, default_value = "4194304")]
    chunk_size: u64,

    /// Don't chunk the output file
    #[arg(long)]
    unchunked: bool,

    /// Include only specified topics (can be used multiple times)
    #[arg(long)]
    include_topics: Vec<String>,

    /// Exclude specified topics (can be used multiple times)
    #[arg(long)]
    exclude_topics: Vec<String>,

    /// Verbose output
    #[arg(short, long)]
    verbose: bool,
}

#[derive(Debug)]
enum InputFormat {
    RosBag1,
    RosBag2,
    Db3,
}

#[derive(Debug, Default)]
struct ConversionStats {
    total_messages: u64,
    converted_messages: u64,
    skipped_messages: u64,
    topics_found: u64,
    topics_converted: u64,
}

pub async fn execute(args: ConvertArgs) -> CliResult<()> {
    // Validate input and output files
    validate_input_file(&args.input)?;
    validate_output_file(&args.output)?;

    println!("🔄 ROS Bag to MCAP Conversion");
    println!("=============================");
    println!("Input:  {}", args.input);
    println!("Output: {}", args.output);

    // Detect input format
    let input_format = detect_input_format(&args.input, &args.input_format)?;

    if args.verbose {
        println!("Input format: {:?}", input_format);
        println!("Compression: {}", args.compression);
        if !args.unchunked {
            println!("Chunk size: {} bytes", args.chunk_size);
        }
        if !args.include_topics.is_empty() {
            println!("Including topics: {:?}", args.include_topics);
        }
        if !args.exclude_topics.is_empty() {
            println!("Excluding topics: {:?}", args.exclude_topics);
        }
    }
    println!();

    // Create output file and writer
    let output_file = File::create(&args.output)?;
    let mut writer = create_writer(output_file, &args)?;

    // Perform conversion based on input format
    let stats = match input_format {
        InputFormat::RosBag1 => convert_rosbag1(&args.input, &mut writer, &args).await?,
        InputFormat::RosBag2 => convert_rosbag2(&args.input, &mut writer, &args).await?,
        InputFormat::Db3 => convert_db3(&args.input, &mut writer, &args).await?,
    };

    writer.finish()?;

    // Print conversion results
    print_conversion_results(&stats, &args)?;

    Ok(())
}

fn detect_input_format(input_path: &str, format_hint: &str) -> CliResult<InputFormat> {
    match format_hint {
        "auto" => {
            // Auto-detect based on file extension and content
            if input_path.ends_with(".bag") {
                Ok(InputFormat::RosBag1)
            } else if input_path.ends_with(".db3") {
                Ok(InputFormat::Db3)
            } else if input_path.contains("rosbag2") || std::path::Path::new(input_path).is_dir() {
                Ok(InputFormat::RosBag2)
            } else {
                // Default to ROS bag 1 for unknown extensions
                Ok(InputFormat::RosBag1)
            }
        }
        "rosbag1" => Ok(InputFormat::RosBag1),
        "rosbag2" => Ok(InputFormat::RosBag2),
        "db3" => Ok(InputFormat::Db3),
        other => Err(crate::error::CliError::invalid_argument(format!(
            "Unknown input format: {}. Supported formats: auto, rosbag1, rosbag2, db3",
            other
        ))),
    }
}

fn create_writer<W: Write + Seek>(
    writer: W,
    args: &ConvertArgs,
) -> CliResult<Writer<BufWriter<W>>> {
    let mut write_opts = WriteOptions::new();

    // Parse compression
    let compression = match args.compression.as_str() {
        "none" => None,
        "lz4" => Some(mcap::Compression::Lz4),
        "zstd" => Some(mcap::Compression::Zstd),
        other => {
            return Err(crate::error::CliError::invalid_argument(format!(
                "Unknown compression format: {}",
                other
            )))
        }
    };

    write_opts = write_opts.compression(compression);

    if args.unchunked {
        write_opts = write_opts.use_chunks(false);
    } else {
        write_opts = write_opts.chunk_size(Some(args.chunk_size));
    }

    Writer::with_options(BufWriter::new(writer), write_opts)
        .map_err(|e| crate::error::CliError::Mcap(e))
}

async fn convert_rosbag1<W: Write + Seek>(
    _input_path: &str,
    _writer: &mut Writer<W>,
    _args: &ConvertArgs,
) -> CliResult<ConversionStats> {
    // TODO: Implement ROS Bag 1 conversion
    // This would require:
    // 1. A ROS Bag 1 parser (rosbag crate or custom implementation)
    // 2. ROS message type definitions and parsing
    // 3. Conversion from ROS message format to MCAP

    Err(crate::error::CliError::invalid_argument(
        "ROS Bag 1 conversion not yet implemented. This requires additional ROS dependencies."
            .to_string(),
    ))
}

async fn convert_rosbag2<W: Write + Seek>(
    _input_path: &str,
    _writer: &mut Writer<W>,
    _args: &ConvertArgs,
) -> CliResult<ConversionStats> {
    // TODO: Implement ROS Bag 2 conversion
    // This would require:
    // 1. A ROS Bag 2 parser (can read SQLite databases)
    // 2. CDR (Common Data Representation) deserialization
    // 3. ROS 2 message type definitions

    Err(crate::error::CliError::invalid_argument(
        "ROS Bag 2 conversion not yet implemented. This requires additional ROS dependencies."
            .to_string(),
    ))
}

async fn convert_db3<W: Write + Seek>(
    _input_path: &str,
    _writer: &mut Writer<W>,
    _args: &ConvertArgs,
) -> CliResult<ConversionStats> {
    // TODO: Implement DB3 conversion
    // This would require:
    // 1. SQLite database reading capabilities
    // 2. Understanding of ROS 2 bag database schema
    // 3. CDR message parsing

    Err(crate::error::CliError::invalid_argument(
        "DB3 conversion not yet implemented. This requires SQLite and ROS 2 message parsing."
            .to_string(),
    ))
}

fn should_include_topic(topic: &str, include_topics: &[String], exclude_topics: &[String]) -> bool {
    // Check exclude list first
    for exclude_pattern in exclude_topics {
        if topic.contains(exclude_pattern) {
            return false;
        }
    }

    // If include list is specified, topic must be in it
    if !include_topics.is_empty() {
        for include_pattern in include_topics {
            if topic.contains(include_pattern) {
                return true;
            }
        }
        return false;
    }

    // No include list specified and not excluded - include it
    true
}

fn print_conversion_results(stats: &ConversionStats, args: &ConvertArgs) -> CliResult<()> {
    println!("📊 Conversion Results:");
    println!("=====================");

    if stats.total_messages > 0 {
        let conversion_rate =
            (stats.converted_messages as f64 / stats.total_messages as f64) * 100.0;
        println!("Messages:");
        println!("  Total found: {}", stats.total_messages);
        println!("  Successfully converted: {}", stats.converted_messages);
        println!("  Skipped: {}", stats.skipped_messages);
        println!("  Conversion rate: {:.1}%", conversion_rate);
        println!();
    }

    if stats.topics_found > 0 {
        println!("Topics:");
        println!("  Total found: {}", stats.topics_found);
        println!("  Converted: {}", stats.topics_converted);
        println!();
    }

    if stats.converted_messages > 0 {
        println!("✅ Conversion completed successfully!");
        println!("📁 MCAP file written to: {}", args.output);

        if stats.skipped_messages > 0 {
            println!("⚠️  Some messages were skipped (filtered or unsupported).");
        }
    } else {
        println!("❌ No messages were converted.");
        return Err(crate::error::CliError::invalid_argument(
            "Conversion failed - no convertible data found".to_string(),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_args() {
        let args = ConvertArgs {
            input: "input.bag".to_string(),
            output: "output.mcap".to_string(),
            input_format: "rosbag1".to_string(),
            compression: "zstd".to_string(),
            chunk_size: 4194304,
            unchunked: false,
            include_topics: vec!["/cmd_vel".to_string()],
            exclude_topics: vec!["/debug".to_string()],
            verbose: false,
        };
        assert_eq!(args.input, "input.bag");
        assert_eq!(args.output, "output.mcap");
        assert_eq!(args.input_format, "rosbag1");
    }

    #[test]
    fn test_detect_input_format() {
        assert!(matches!(
            detect_input_format("test.bag", "auto").unwrap(),
            InputFormat::RosBag1
        ));
        assert!(matches!(
            detect_input_format("test.db3", "auto").unwrap(),
            InputFormat::Db3
        ));
        assert!(matches!(
            detect_input_format("anything", "rosbag2").unwrap(),
            InputFormat::RosBag2
        ));
    }

    #[test]
    fn test_should_include_topic() {
        let include_topics = vec!["/cmd_vel".to_string()];
        let exclude_topics = vec!["/debug".to_string()];

        // Should include - matches include pattern
        assert!(should_include_topic(
            "/cmd_vel",
            &include_topics,
            &exclude_topics
        ));

        // Should exclude - matches exclude pattern
        assert!(!should_include_topic(
            "/debug/info",
            &include_topics,
            &exclude_topics
        ));

        // Should not include - doesn't match include pattern
        assert!(!should_include_topic(
            "/other",
            &include_topics,
            &exclude_topics
        ));

        // No patterns - should include everything
        assert!(should_include_topic("/any/topic", &[], &[]));
    }

    #[test]
    fn test_conversion_stats_default() {
        let stats = ConversionStats::default();
        assert_eq!(stats.total_messages, 0);
        assert_eq!(stats.converted_messages, 0);
        assert_eq!(stats.skipped_messages, 0);
    }
}
