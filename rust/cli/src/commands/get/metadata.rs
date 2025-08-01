use crate::error::CliResult;
use crate::utils::validation::validate_input_file;
use clap::Args;
use mcap::Summary;
use memmap2::Mmap;
use serde_json;
use std::collections::BTreeMap;
use std::fs::File;

#[derive(Args)]
pub struct GetMetadataArgs {
    /// MCAP file to extract metadata from
    mcap_file: String,

    /// Name of metadata record to extract
    #[arg(short, long)]
    name: String,
}

pub async fn execute(args: GetMetadataArgs) -> CliResult<()> {
    // Validate input file
    validate_input_file(&args.mcap_file)?;

    println!("📋 Extracting metadata from MCAP file");
    println!("MCAP file: {}", args.mcap_file);
    println!("Metadata name: {}", args.name);
    println!();

    // Open and map the MCAP file
    let file = File::open(&args.mcap_file)?;
    let mapped = unsafe { Mmap::map(&file)? };

    // Read the summary to get metadata indexes
    let summary = Summary::read(&mapped)?;
    let summary = summary.ok_or_else(|| {
        crate::error::CliError::invalid_argument(
            "MCAP file does not contain a summary section".to_string(),
        )
    })?;

    // Find metadata records with the specified name
    let matching_metadata: Vec<_> = summary
        .metadata_indexes
        .iter()
        .filter(|idx| idx.name == args.name)
        .collect();

    if matching_metadata.is_empty() {
        return Err(crate::error::CliError::invalid_argument(format!(
            "Metadata '{}' not found",
            args.name
        )));
    }

    // Collect all metadata with the same name
    let mut combined_metadata = BTreeMap::new();

    for metadata_idx in matching_metadata {
        let metadata_record = extract_metadata_record(&mapped, metadata_idx)?;

        // Merge all key-value pairs
        for (key, value) in metadata_record {
            combined_metadata.insert(key, value);
        }
    }

    // Output as pretty-printed JSON
    let json_output = serde_json::to_string_pretty(&combined_metadata)?;
    println!("{}", json_output);

    Ok(())
}

fn extract_metadata_record(
    mapped: &[u8],
    metadata_idx: &mcap::records::MetadataIndex,
) -> CliResult<BTreeMap<String, String>> {
    // Calculate the offset to the metadata record content
    let record_offset = metadata_idx.offset + 1 + 8; // Skip opcode and record length

    let start = record_offset as usize;
    let end = start + metadata_idx.length as usize;

    if end > mapped.len() {
        return Err(crate::error::CliError::invalid_argument(
            "Metadata record extends beyond file bounds".to_string(),
        ));
    }

    let record_data = &mapped[start..end];

    // Parse the metadata record
    // The metadata record format is:
    // - name (string with length prefix)
    // - metadata map (with count and key-value pairs)

    let mut offset = 0;

    // Skip name (we already know it from the index)
    if offset + 4 > record_data.len() {
        return Err(crate::error::CliError::invalid_argument(
            "Invalid metadata record: insufficient data for name length".to_string(),
        ));
    }
    let name_length = u32::from_le_bytes([
        record_data[offset],
        record_data[offset + 1],
        record_data[offset + 2],
        record_data[offset + 3],
    ]) as usize;
    offset += 4 + name_length;

    // Read metadata count
    if offset + 4 > record_data.len() {
        return Err(crate::error::CliError::invalid_argument(
            "Invalid metadata record: insufficient data for metadata count".to_string(),
        ));
    }
    let metadata_count = u32::from_le_bytes([
        record_data[offset],
        record_data[offset + 1],
        record_data[offset + 2],
        record_data[offset + 3],
    ]);
    offset += 4;

    // Read key-value pairs
    let mut metadata = BTreeMap::new();
    for _ in 0..metadata_count {
        // Read key length and key
        if offset + 4 > record_data.len() {
            return Err(crate::error::CliError::invalid_argument(
                "Invalid metadata record: insufficient data for key length".to_string(),
            ));
        }
        let key_length = u32::from_le_bytes([
            record_data[offset],
            record_data[offset + 1],
            record_data[offset + 2],
            record_data[offset + 3],
        ]) as usize;
        offset += 4;

        if offset + key_length > record_data.len() {
            return Err(crate::error::CliError::invalid_argument(
                "Invalid metadata record: insufficient data for key".to_string(),
            ));
        }
        let key = String::from_utf8_lossy(&record_data[offset..offset + key_length]).into_owned();
        offset += key_length;

        // Read value length and value
        if offset + 4 > record_data.len() {
            return Err(crate::error::CliError::invalid_argument(
                "Invalid metadata record: insufficient data for value length".to_string(),
            ));
        }
        let value_length = u32::from_le_bytes([
            record_data[offset],
            record_data[offset + 1],
            record_data[offset + 2],
            record_data[offset + 3],
        ]) as usize;
        offset += 4;

        if offset + value_length > record_data.len() {
            return Err(crate::error::CliError::invalid_argument(
                "Invalid metadata record: insufficient data for value".to_string(),
            ));
        }
        let value =
            String::from_utf8_lossy(&record_data[offset..offset + value_length]).into_owned();
        offset += value_length;

        metadata.insert(key, value);
    }

    Ok(metadata)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_metadata_args() {
        let args = GetMetadataArgs {
            mcap_file: "test.mcap".to_string(),
            name: "test_metadata".to_string(),
        };
        assert_eq!(args.mcap_file, "test.mcap");
        assert_eq!(args.name, "test_metadata");
    }
}
