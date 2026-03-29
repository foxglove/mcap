use std::collections::BTreeMap;

use anyhow::{Context, Result};
use mcap::records;

use crate::{cli::AddMetadataArgs, commands::rewrite::rewrite_mcap_with_appends};

fn parse_key_values(kv_pairs: &[String]) -> Result<BTreeMap<String, String>> {
    let mut metadata = BTreeMap::new();
    for kv in kv_pairs {
        let (key, value) = kv
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!("invalid key/value '{}': expected key=value", kv))?;
        metadata.insert(key.to_string(), value.to_string());
    }
    Ok(metadata)
}

pub fn run(args: AddMetadataArgs) -> Result<()> {
    let metadata_map = parse_key_values(&args.key_values)?;
    let metadata = records::Metadata {
        name: args.name.clone(),
        metadata: metadata_map,
    };

    rewrite_mcap_with_appends(&args.file, None, Some(metadata))
        .with_context(|| format!("failed to append metadata to {}", args.file.display()))
}

#[cfg(test)]
mod tests {
    use super::parse_key_values;

    #[test]
    fn parses_key_values() {
        let parsed = parse_key_values(&["a=1".to_string(), "b=2".to_string()])
            .expect("key values should parse");
        assert_eq!(parsed.get("a").expect("a exists"), "1");
        assert_eq!(parsed.get("b").expect("b exists"), "2");
    }

    #[test]
    fn rejects_invalid_pairs() {
        parse_key_values(&["missing-separator".to_string()])
            .expect_err("invalid key/value should fail");
    }
}
