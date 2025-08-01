use crate::error::CliResult;
use crate::utils::validation::validate_input_file;
use chrono::DateTime;
use clap::Args;
use std::fs::File;
use std::io::Read;
use std::path::Path;

#[derive(Args)]
pub struct AddAttachmentArgs {
    /// MCAP file to add attachment to
    mcap_file: String,

    /// File to add as attachment
    #[arg(short, long)]
    file: String,

    /// Name of attachment (defaults to filename)
    #[arg(short, long)]
    name: Option<String>,

    /// Content type of attachment
    #[arg(long, default_value = "application/octet-stream")]
    content_type: String,

    /// Log time in nanoseconds or RFC3339 format (defaults to current time)
    #[arg(long)]
    log_time: Option<String>,

    /// Creation time in nanoseconds or RFC3339 format (defaults to file modification time)
    #[arg(long)]
    creation_time: Option<String>,
}

pub async fn execute(args: AddAttachmentArgs) -> CliResult<()> {
    // Validate input files
    validate_input_file(&args.mcap_file)?;
    validate_input_file(&args.file)?;

    println!("📎 Adding attachment to MCAP file");
    println!("MCAP file: {}", args.mcap_file);
    println!("Attachment: {}", args.file);

    // Get attachment file metadata
    let attachment_metadata = std::fs::metadata(&args.file)?;
    let attachment_size = attachment_metadata.len();

    // Determine attachment name
    let attachment_name = args.name.unwrap_or_else(|| {
        Path::new(&args.file)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or(&args.file)
            .to_string()
    });

    // Parse timestamps
    let creation_time = if let Some(ct) = args.creation_time {
        parse_timestamp(&ct)?
    } else {
        attachment_metadata
            .modified()
            .map_err(|e| crate::error::CliError::Io(e))?
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| {
                crate::error::CliError::invalid_argument(format!("Invalid file time: {}", e))
            })?
            .as_nanos() as u64
    };

    let log_time = if let Some(lt) = args.log_time {
        parse_timestamp(&lt)?
    } else {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| {
                crate::error::CliError::invalid_argument(format!("Invalid system time: {}", e))
            })?
            .as_nanos() as u64
    };

    println!("  Name: {}", attachment_name);
    println!("  Content type: {}", args.content_type);
    println!("  Size: {} bytes", attachment_size);
    println!();

    // Read attachment data
    let mut attachment_file = File::open(&args.file)?;
    let mut attachment_data = Vec::new();
    attachment_file.read_to_end(&mut attachment_data)?;

    // Add attachment to MCAP file using the amend functionality
    amend_mcap_with_attachment(
        &args.mcap_file,
        &attachment_name,
        &args.content_type,
        log_time,
        creation_time,
        attachment_data,
    )
    .await?;

    println!("✅ Attachment added successfully!");

    Ok(())
}

fn parse_timestamp(timestamp_str: &str) -> CliResult<u64> {
    // Try parsing as nanoseconds first
    if let Ok(nanos) = timestamp_str.parse::<u64>() {
        return Ok(nanos);
    }

    // Try parsing as RFC3339
    if let Ok(dt) = DateTime::parse_from_rfc3339(timestamp_str) {
        return Ok(dt.timestamp_nanos_opt().unwrap_or(0) as u64);
    }

    Err(crate::error::CliError::invalid_argument(format!(
        "Invalid timestamp format: {}. Use nanoseconds or RFC3339 format.",
        timestamp_str
    )))
}

async fn amend_mcap_with_attachment(
    mcap_path: &str,
    name: &str,
    media_type: &str,
    log_time: u64,
    creation_time: u64,
    data: Vec<u8>,
) -> CliResult<()> {
    // This is a simplified implementation. In a full implementation, we would need to:
    // 1. Read the existing MCAP file
    // 2. Parse its structure
    // 3. Append the new attachment record
    // 4. Update the summary section and footer
    //
    // For now, we'll use a temporary approach that creates a new file with the attachment
    // The mcap crate doesn't have built-in "amend" functionality like the Go version

    // Open the existing file for reading
    let input_file = File::open(mcap_path)?;
    let mapped = unsafe { memmap2::Mmap::map(&input_file)? };

    // Create a temporary output file
    let temp_path = format!("{}.tmp", mcap_path);
    let output_file = File::create(&temp_path)?;

    // Use MCAP writer to copy existing content and add attachment
    let mut writer = mcap::Writer::new(std::io::BufWriter::new(output_file))?;

    // Copy existing messages from the original file
    let message_stream = mcap::MessageStream::new(&mapped)?;
    let mut schema_mapping = std::collections::HashMap::new();
    let mut channel_mapping = std::collections::HashMap::new();

    for message_result in message_stream {
        let message = message_result?;

        // Add schema if we haven't seen it
        let schema_id = if let Some(schema) = &message.channel.schema {
            if let Some(&existing_id) = schema_mapping.get(&schema.id) {
                existing_id
            } else {
                let new_id = writer.add_schema(&schema.name, &schema.encoding, &schema.data)?;
                schema_mapping.insert(schema.id, new_id);
                new_id
            }
        } else {
            0
        };

        // Add channel if we haven't seen it
        let channel_id = if let Some(&existing_id) = channel_mapping.get(&message.channel.id) {
            existing_id
        } else {
            let new_id = writer.add_channel(
                schema_id,
                &message.channel.topic,
                &message.channel.message_encoding,
                &message.channel.metadata,
            )?;
            channel_mapping.insert(message.channel.id, new_id);
            new_id
        };

        // Write the message
        writer.write_to_known_channel(
            &mcap::records::MessageHeader {
                channel_id,
                sequence: message.sequence,
                log_time: message.log_time,
                publish_time: message.publish_time,
            },
            &message.data,
        )?;
    }

    // Add the new attachment using the correct API
    let attachment_header = mcap::records::AttachmentHeader {
        log_time,
        create_time: creation_time,
        name: name.to_string(),
        media_type: media_type.to_string(),
    };

    writer.start_attachment(data.len() as u64, attachment_header)?;
    writer.put_attachment_bytes(&data)?;
    writer.finish_attachment()?;

    writer.finish()?;
    drop(writer);
    drop(mapped);
    drop(input_file);

    // Replace the original file with the new one
    std::fs::rename(&temp_path, mcap_path)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_attachment_args() {
        let args = AddAttachmentArgs {
            mcap_file: "test.mcap".to_string(),
            file: "attachment.txt".to_string(),
            name: Some("my_attachment".to_string()),
            content_type: "text/plain".to_string(),
            log_time: None,
            creation_time: None,
        };
        assert_eq!(args.mcap_file, "test.mcap");
        assert_eq!(args.file, "attachment.txt");
        assert_eq!(args.name, Some("my_attachment".to_string()));
        assert_eq!(args.content_type, "text/plain");
    }

    #[test]
    fn test_parse_timestamp_nanos() {
        let result = parse_timestamp("1234567890123456789").unwrap();
        assert_eq!(result, 1234567890123456789);
    }

    #[test]
    fn test_parse_timestamp_rfc3339() {
        let result = parse_timestamp("2023-01-01T00:00:00Z").unwrap();
        assert!(result > 0);
    }

    #[test]
    fn test_parse_timestamp_invalid() {
        let result = parse_timestamp("invalid");
        assert!(result.is_err());
    }
}
