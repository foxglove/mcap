use std::collections::BTreeMap;

use anyhow::{Context, Result};

use crate::cli::AddMetadataCommand;
use crate::commands::add_common;
use crate::context::CommandContext;

pub fn run(_ctx: &CommandContext, args: AddMetadataCommand) -> Result<()> {
    let metadata_map = parse_key_values(&args.key_values)?;
    let metadata = mcap::records::Metadata {
        name: args.name,
        metadata: metadata_map,
    };
    add_common::amend_mcap_file(&args.file, &[], &[metadata])
        .with_context(|| format!("failed to add metadata to '{}'", args.file.display()))?;
    Ok(())
}

pub(crate) fn parse_key_values(values: &[String]) -> Result<BTreeMap<String, String>> {
    let mut out = BTreeMap::new();
    for value in values {
        let Some((key, val)) = value.split_once('=') else {
            anyhow::bail!("failed to parse key/value '{value}', expected key=value");
        };
        if key.is_empty() {
            anyhow::bail!("metadata key must not be empty in '{value}'");
        }
        if out.insert(key.to_string(), val.to_string()).is_some() {
            anyhow::bail!("duplicate metadata key '{key}'");
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::parse_key_values;

    #[test]
    fn parse_key_values_accepts_valid_pairs() {
        let out = parse_key_values(&["a=1".to_string(), "b=2".to_string()]).expect("parse");
        assert_eq!(out.get("a"), Some(&"1".to_string()));
        assert_eq!(out.get("b"), Some(&"2".to_string()));
    }

    #[test]
    fn parse_key_values_rejects_invalid_pair() {
        let err = parse_key_values(&["invalid".to_string()]).expect_err("must fail");
        assert!(err.to_string().contains("expected key=value"));
    }

    #[test]
    fn parse_key_values_rejects_empty_key() {
        let err = parse_key_values(&["=value".to_string()]).expect_err("must fail");
        assert!(err.to_string().contains("metadata key must not be empty"));
    }

    #[test]
    fn parse_key_values_rejects_duplicate_keys() {
        let err = parse_key_values(&["a=1".to_string(), "a=2".to_string()]).expect_err("must fail");
        assert!(err.to_string().contains("duplicate metadata key 'a'"));
    }
}
