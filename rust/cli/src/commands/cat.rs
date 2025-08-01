use crate::error::CliResult;
use crate::utils::{format_decimal_time, validation::validate_input_file};
use clap::Args;
use mcap::MessageStream;
use memmap2::Mmap;
use std::fs::File;
use std::io::{self, Write};

#[derive(Args)]
pub struct CatArgs {
    /// MCAP file(s) to read
    files: Vec<String>,

    /// Comma-separated list of topics to include
    #[arg(long, value_delimiter = ',')]
    topics: Vec<String>,

    /// Start time in seconds
    #[arg(long, conflicts_with = "start_nsecs")]
    start_secs: Option<u64>,

    /// End time in seconds
    #[arg(long, conflicts_with = "end_nsecs")]
    end_secs: Option<u64>,

    /// Start time in nanoseconds
    #[arg(long, conflicts_with = "start_secs")]
    start_nsecs: Option<u64>,

    /// End time in nanoseconds
    #[arg(long, conflicts_with = "end_secs")]
    end_nsecs: Option<u64>,

    /// Output messages as JSON
    #[arg(long)]
    json: bool,
}

pub async fn execute(args: CatArgs) -> CliResult<()> {
    let mut output = io::stdout().lock();

    if args.files.is_empty() {
        // TODO: Handle stdin input
        return Err(crate::error::CliError::invalid_argument("supply a file"));
    }

    // Calculate time range in nanoseconds
    let start_time = match (args.start_secs, args.start_nsecs) {
        (Some(secs), None) => Some(secs * 1_000_000_000),
        (None, Some(nsecs)) => Some(nsecs),
        (None, None) => None,
        _ => unreachable!(), // clap ensures mutual exclusion
    };

    let end_time = match (args.end_secs, args.end_nsecs) {
        (Some(secs), None) => Some(secs * 1_000_000_000),
        (None, Some(nsecs)) => Some(nsecs),
        (None, None) => None,
        _ => unreachable!(), // clap ensures mutual exclusion
    };

    // Process each file
    for filename in &args.files {
        validate_input_file(filename)?;
        process_file(
            &mut output,
            filename,
            &args.topics,
            start_time,
            end_time,
            args.json,
        )?;
    }

    Ok(())
}

fn process_file<W: Write>(
    output: &mut W,
    filename: &str,
    topic_filter: &[String],
    start_time: Option<u64>,
    end_time: Option<u64>,
    format_json: bool,
) -> CliResult<()> {
    // Memory map the file
    let file = File::open(filename)?;
    let mapped = unsafe { Mmap::map(&file)? };

    // Create message stream
    let message_stream = MessageStream::new(&mapped)?;

    for message_result in message_stream {
        let message = message_result?;

        // Apply time filtering
        if let Some(start) = start_time {
            if message.log_time < start {
                continue;
            }
        }
        if let Some(end) = end_time {
            if message.log_time > end {
                continue;
            }
        }

        // Apply topic filtering
        if !topic_filter.is_empty() && !topic_filter.contains(&message.channel.topic) {
            continue;
        }

        // Output the message
        if format_json {
            output_message_json(output, &message)?;
        } else {
            output_message_simple(output, &message)?;
        }
    }

    Ok(())
}

fn output_message_simple<W: Write>(output: &mut W, message: &mcap::Message) -> std::io::Result<()> {
    let schema_name = message
        .channel
        .schema
        .as_ref()
        .map(|s| s.name.as_str())
        .unwrap_or("no schema");

    // Format: timestamp topic [schema] data...
    let data_preview = if message.data.len() > 10 {
        format!("{:?}...", &message.data[..10])
    } else {
        format!("{:?}", message.data)
    };

    writeln!(
        output,
        "{} {} [{}] {}",
        format_decimal_time(message.log_time),
        message.channel.topic,
        schema_name,
        data_preview
    )?;

    Ok(())
}

fn output_message_json<W: Write>(output: &mut W, message: &mcap::Message) -> std::io::Result<()> {
    // For now, output a basic JSON structure
    // TODO: Implement proper message decoding for different encodings
    writeln!(
        output,
        r#"{{"topic":"{}","sequence":{},"log_time":{},"publish_time":{},"data":"(binary data)"}}"#,
        message.channel.topic, message.sequence, message.log_time, message.publish_time,
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_time_conversion() {
        let args = CatArgs {
            files: vec!["test.mcap".to_string()],
            topics: vec![],
            start_secs: Some(5),
            end_secs: None,
            start_nsecs: None,
            end_nsecs: None,
            json: false,
        };

        let start_time = match (args.start_secs, args.start_nsecs) {
            (Some(secs), None) => Some(secs * 1_000_000_000),
            (None, Some(nsecs)) => Some(nsecs),
            (None, None) => None,
            _ => unreachable!(),
        };

        assert_eq!(start_time, Some(5_000_000_000));
    }
}
