use crate::error::{CliError, CliResult};
use std::io::SeekFrom;
use tokio::io::{AsyncRead, AsyncSeek};

// Create a trait alias to solve the trait object issue
pub trait AsyncReadSeek: AsyncRead + AsyncSeek + Unpin + Send {}
impl<T: AsyncRead + AsyncSeek + Unpin + Send> AsyncReadSeek for T {}

pub struct Reader {
    inner: Box<dyn AsyncReadSeek>,
}

impl Reader {
    pub fn new(reader: Box<dyn AsyncReadSeek>) -> Self {
        Self { inner: reader }
    }
}

impl AsyncRead for Reader {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncSeek for Reader {
    fn start_seek(mut self: std::pin::Pin<&mut Self>, position: SeekFrom) -> std::io::Result<()> {
        std::pin::Pin::new(&mut self.inner).start_seek(position)
    }

    fn poll_complete(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<u64>> {
        std::pin::Pin::new(&mut self.inner).poll_complete(cx)
    }
}

/// Get a reader for the given path, supporting local files and cloud storage URLs
pub async fn get_reader(path: &str) -> CliResult<Reader> {
    if let Some((scheme, bucket, object_path)) = parse_url(path) {
        match scheme {
            #[cfg(feature = "cloud")]
            "gs" => get_gcs_reader(bucket, object_path).await,
            #[cfg(not(feature = "cloud"))]
            "gs" => Err(CliError::invalid_argument(
                "Cloud storage support not enabled. Rebuild with --features cloud".to_string(),
            )),
            _ => Err(CliError::invalid_argument(format!(
                "Unsupported remote file scheme: {}",
                scheme
            ))),
        }
    } else {
        // Local file
        let file = tokio::fs::File::open(path)
            .await
            .map_err(|_| CliError::file_not_found(path))?;
        Ok(Reader::new(Box::new(file)))
    }
}

/// Parse URL into (scheme, bucket, path) components
fn parse_url(url: &str) -> Option<(&str, &str, &str)> {
    if let Some(scheme_end) = url.find("://") {
        let scheme = &url[..scheme_end];
        let remainder = &url[scheme_end + 3..];

        if let Some(bucket_end) = remainder.find('/') {
            let bucket = &remainder[..bucket_end];
            let path = &remainder[bucket_end + 1..];
            Some((scheme, bucket, path))
        } else {
            None
        }
    } else {
        None
    }
}

#[cfg(feature = "cloud")]
async fn get_gcs_reader(bucket: &str, object_path: &str) -> CliResult<Reader> {
    // For now, return an error with a helpful message
    // TODO: Implement proper GCS integration once we figure out the correct API
    Err(CliError::invalid_argument(format!(
        "GCS support not yet implemented for gs://{}/{}",
        bucket, object_path
    )))
}

/// Check if we're reading from stdin
pub fn reading_stdin() -> CliResult<bool> {
    use std::io::IsTerminal;
    Ok(!std::io::stdin().is_terminal())
}

/// Check if stdout is redirected
pub fn stdout_redirected() -> bool {
    use std::io::IsTerminal;
    !std::io::stdout().is_terminal()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_url() {
        assert_eq!(
            parse_url("gs://my-bucket/path/to/file.mcap"),
            Some(("gs", "my-bucket", "path/to/file.mcap"))
        );

        assert_eq!(parse_url("local/file.mcap"), None);
        assert_eq!(parse_url("gs://bucket-only"), None);
    }
}
