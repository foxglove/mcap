#![allow(dead_code)]

use std::{
    env,
    fs::File,
    io::{self, IsTerminal},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use mcap::MAGIC;

pub fn open_local_mcap(path: &Path) -> Result<Vec<u8>> {
    if let Some(remote) = parse_remote_uri(path) {
        return download_remote_bytes(path, &remote);
    }
    std::fs::read(path).with_context(|| format!("failed to read file {}", path.display()))
}

pub fn open_local_file(path: &Path) -> Result<File> {
    ensure_local_path(path)?;
    File::open(path).with_context(|| format!("failed to open file {}", path.display()))
}

pub fn create_local_file(path: &Path) -> Result<File> {
    ensure_local_path(path)?;
    File::create(path).with_context(|| format!("failed to create file {}", path.display()))
}

pub fn reading_stdin() -> Result<bool> {
    Ok(!std::io::stdin().is_terminal())
}

pub fn stdout_redirected() -> Result<bool> {
    Ok(!std::io::stdout().is_terminal())
}

pub fn ensure_stdout_redirected_for_binary_output() -> Result<()> {
    if !stdout_redirected()? {
        anyhow::bail!(
            "Binary output can screw up your terminal. Supply -o or redirect to a file or pipe"
        );
    }
    Ok(())
}

pub fn read_paths_from_stdin() -> Result<Vec<PathBuf>> {
    if !reading_stdin()? {
        return Ok(Vec::new());
    }
    let mut input = String::new();
    io::stdin()
        .read_line(&mut input)
        .context("failed to read stdin")?;
    Ok(input
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(PathBuf::from)
        .collect())
}

pub fn has_mcap_magic(bytes: &[u8]) -> bool {
    bytes.starts_with(MAGIC) && bytes.ends_with(MAGIC)
}

pub fn is_remote_uri(path: &Path) -> bool {
    parse_remote_uri(path).is_some()
}

pub fn ensure_local_path(path: &Path) -> Result<()> {
    if is_remote_uri(path) {
        anyhow::bail!(
            "operation is not supported for remote URI '{}'; use a local file path",
            path.display()
        );
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RemoteUri {
    scheme: String,
    bucket: String,
    key: String,
}

fn parse_remote_uri(path: &Path) -> Option<RemoteUri> {
    let raw = path.to_string_lossy();
    let (scheme, rest) = raw.split_once("://")?;
    if scheme != "gs" && scheme != "s3" {
        return None;
    }
    let (bucket, key) = rest.split_once('/')?;
    if bucket.is_empty() || key.is_empty() {
        return None;
    }
    Some(RemoteUri {
        scheme: scheme.to_string(),
        bucket: bucket.to_string(),
        key: key.to_string(),
    })
}

fn download_remote_bytes(path: &Path, remote: &RemoteUri) -> Result<Vec<u8>> {
    let region = aws_region();
    let url = remote_url(remote, &region)?;
    let client = reqwest::blocking::Client::builder()
        .user_agent("mcap-cli-rust")
        .build()
        .context("failed to initialize HTTP client")?;

    let response = client
        .get(url.clone())
        .send()
        .with_context(|| format!("failed downloading {}", path.display()))?;
    if !response.status().is_success() {
        anyhow::bail!(
            "failed downloading {} ({}): HTTP {}",
            path.display(),
            url,
            response.status()
        );
    }

    let body = response
        .bytes()
        .with_context(|| format!("failed reading body for {}", path.display()))?;
    Ok(body.to_vec())
}

fn aws_region() -> String {
    env::var("AWS_REGION")
        .or_else(|_| env::var("AWS_DEFAULT_REGION"))
        .unwrap_or_else(|_| "us-east-1".to_string())
}

fn remote_url(remote: &RemoteUri, aws_region: &str) -> Result<String> {
    let mut url = match remote.scheme.as_str() {
        "gs" => reqwest::Url::parse("https://storage.googleapis.com")
            .context("failed constructing GCS URL")?,
        "s3" => reqwest::Url::parse(&format!("https://s3.{aws_region}.amazonaws.com"))
            .context("failed constructing S3 URL")?,
        _ => anyhow::bail!("unsupported remote scheme '{}'", remote.scheme),
    };

    {
        let mut segments = url
            .path_segments_mut()
            .map_err(|_| anyhow::anyhow!("failed to build remote URL path segments"))?;
        segments.push(&remote.bucket);
        for part in remote.key.split('/').filter(|part| !part.is_empty()) {
            segments.push(part);
        }
    }

    Ok(url.to_string())
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{ensure_local_path, is_remote_uri, parse_remote_uri, remote_url, RemoteUri};

    #[test]
    fn parses_gs_uri() {
        let parsed = parse_remote_uri(Path::new("gs://some-bucket/path/file.mcap"))
            .expect("uri should parse");
        assert_eq!(parsed.scheme, "gs");
        assert_eq!(parsed.bucket, "some-bucket");
        assert_eq!(parsed.key, "path/file.mcap");
    }

    #[test]
    fn parses_s3_uri() {
        let parsed =
            parse_remote_uri(Path::new("s3://example.bucket/data.mcap")).expect("uri should parse");
        assert_eq!(parsed.scheme, "s3");
        assert_eq!(parsed.bucket, "example.bucket");
        assert_eq!(parsed.key, "data.mcap");
    }

    #[test]
    fn builds_gs_url() {
        let remote = RemoteUri {
            scheme: "gs".to_string(),
            bucket: "bucket".to_string(),
            key: "a b/c.mcap".to_string(),
        };
        let url = remote_url(&remote, "us-east-1").expect("url should build");
        assert_eq!(url, "https://storage.googleapis.com/bucket/a%20b/c.mcap");
    }

    #[test]
    fn builds_s3_url() {
        let remote = RemoteUri {
            scheme: "s3".to_string(),
            bucket: "bucket".to_string(),
            key: "dir/file.mcap".to_string(),
        };
        let url = remote_url(&remote, "eu-north-1").expect("url should build");
        assert_eq!(
            url,
            "https://s3.eu-north-1.amazonaws.com/bucket/dir/file.mcap"
        );
    }

    #[test]
    fn detects_remote_uri() {
        assert!(is_remote_uri(Path::new("gs://bucket/path/file.mcap")));
        assert!(is_remote_uri(Path::new("s3://bucket/path/file.mcap")));
        assert!(!is_remote_uri(Path::new("/tmp/file.mcap")));
    }

    #[test]
    fn rejects_remote_output_paths() {
        let err = ensure_local_path(Path::new("gs://bucket/out.mcap"))
            .expect_err("remote output path should fail");
        assert!(err.to_string().contains("not supported for remote URI"));
    }
}
