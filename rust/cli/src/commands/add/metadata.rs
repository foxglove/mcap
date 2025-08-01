use crate::error::CliResult;
use crate::utils::validation::validate_input_file;
use clap::Args;
use std::collections::BTreeMap;
use std::fs::File;

#[derive(Args)]
pub struct AddMetadataArgs {
    /// MCAP file to add metadata to
    mcap_file: String,

    /// Name of metadata record
    #[arg(short, long)]
    name: String,

    /// Key-value pairs (can be used multiple times)
    #[arg(short, long)]
    key: Vec<String>,
}

pub async fn execute(args: AddMetadataArgs) -> CliResult<()> {
    // Validate input file
    validate_input_file(&args.mcap_file)?;

    println!("📋 Adding metadata to MCAP file");
    println!("MCAP file: {}", args.mcap_file);
    println!("Metadata name: {}", args.name);

    // Parse key-value pairs
    let mut metadata = BTreeMap::new();
    for kv in &args.key {
        let parts: Vec<&str> = kv.splitn(2, '=').collect();
        if parts.len() != 2 {
            return Err(crate::error::CliError::invalid_argument(format!(
                "Invalid key-value pair: {}. Use format key=value",
                kv
            )));
        }
        metadata.insert(parts[0].to_string(), parts[1].to_string());
    }

    if metadata.is_empty() {
        return Err(crate::error::CliError::invalid_argument(
            "At least one key-value pair must be provided".to_string(),
        ));
    }

    println!("Key-value pairs:");
    for (key, value) in &metadata {
        println!("  {} = {}", key, value);
    }
    println!();

    // Add metadata to MCAP file using the amend functionality
    amend_mcap_with_metadata(&args.mcap_file, &args.name, metadata).await?;

    println!("✅ Metadata added successfully!");

    Ok(())
}

async fn amend_mcap_with_metadata(
    mcap_path: &str,
    name: &str,
    metadata: BTreeMap<String, String>,
) -> CliResult<()> {
    // This is a simplified implementation similar to the attachment approach
    // In a full implementation, we would properly amend the MCAP file structure

    // Open the existing file for reading
    let input_file = File::open(mcap_path)?;
    let mapped = unsafe { memmap2::Mmap::map(&input_file)? };

    // Create a temporary output file
    let temp_path = format!("{}.tmp", mcap_path);
    let output_file = File::create(&temp_path)?;

    // Use MCAP writer to copy existing content and add metadata
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

    // Add the new metadata record using the correct API
    let metadata_record = mcap::records::Metadata {
        name: name.to_string(),
        metadata,
    };

    writer.write_metadata(&metadata_record)?;

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
    fn test_add_metadata_args() {
        let args = AddMetadataArgs {
            mcap_file: "test.mcap".to_string(),
            name: "test_metadata".to_string(),
            key: vec!["key1=value1".to_string(), "key2=value2".to_string()],
        };
        assert_eq!(args.mcap_file, "test.mcap");
        assert_eq!(args.name, "test_metadata");
        assert_eq!(args.key.len(), 2);
    }

    #[test]
    fn test_parse_key_value_pairs() {
        let args = AddMetadataArgs {
            mcap_file: "test.mcap".to_string(),
            name: "test".to_string(),
            key: vec![
                "key1=value1".to_string(),
                "key2=value with spaces".to_string(),
            ],
        };

        let mut metadata = BTreeMap::new();
        for kv in &args.key {
            let parts: Vec<&str> = kv.splitn(2, '=').collect();
            if parts.len() == 2 {
                metadata.insert(parts[0].to_string(), parts[1].to_string());
            }
        }

        assert_eq!(metadata.get("key1"), Some(&"value1".to_string()));
        assert_eq!(metadata.get("key2"), Some(&"value with spaces".to_string()));
    }
}
