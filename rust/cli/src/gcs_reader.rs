use async_http_range_reader::{AsyncHttpRangeReader, CheckSupportMethod};
use async_trait::async_trait;
use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
use reqwest_middleware::reqwest::header::HeaderMap;
use url::Url;

use crate::{error::CliResult, traits::McapReader};

const GCS_ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'*')
    .remove(b'-')
    .remove(b'.')
    .remove(b'_');

/// Create a reader the implements [`AsyncRead`] and [`AsyncWrite`], and is backed by a GCS file.
///
/// The current implementation does not support authenticated requests to GCS.
pub async fn create_gcs_reader(
    bucket_name: &str,
    object_name: &str,
) -> CliResult<AsyncHttpRangeReader> {
    let bucket = utf8_percent_encode(bucket_name, GCS_ENCODE_SET);
    let object = utf8_percent_encode(object_name, GCS_ENCODE_SET);

    let url = format!(
        "https://storage.googleapis.com/download/storage/v1/b/{bucket}/o/{object}?alt=media"
    );

    let (mut file, _) = AsyncHttpRangeReader::new(
        reqwest_middleware::ClientWithMiddleware::default(),
        Url::parse(&url)?,
        // Fetch the last 8KiB of the file. This will include the footer and for many files much of
        // the summary information.
        CheckSupportMethod::NegativeRangeRequest(8192 as _),
        HeaderMap::default(),
    )
    .await?;

    // Also prefetch the beginning of the file as we'll need it to extract the header information.
    file.prefetch(0..1024).await;

    Ok(file)
}

#[async_trait]
impl McapReader for AsyncHttpRangeReader {
    // Since GCS files are latency bound, add an implementation of prefetch so certain known
    // regions of the file can be ready to go.
    async fn prefetch(&mut self, bytes: std::ops::Range<u64>) {
        self.prefetch(bytes).await;
    }
}
