use async_http_range_reader::AsyncHttpRangeReader;
use async_trait::async_trait;
use google_cloud_auth::{project::Config as GcloudConfig, token::DefaultTokenSourceProvider};
use google_cloud_token::TokenSourceProvider;
use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
use reqwest_middleware::reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};
use tracing::{debug, instrument};
use url::Url;

use crate::{error::CliResult, reader::McapReader};

const GCS_ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'*')
    .remove(b'-')
    .remove(b'.')
    .remove(b'_');

/// Authenticate with gcloud and return an authorization header
#[instrument]
async fn get_gcloud_header() -> Option<HeaderValue> {
    let token_source_fut = DefaultTokenSourceProvider::new(GcloudConfig {
        audience: None,
        scopes: Some(&[
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/devstorage.full_control",
        ]),
        sub: None,
    });

    let token_source = match token_source_fut.await {
        Ok(t) => t,
        Err(e) => {
            debug!(
                target = "mcap::cli",
                "Ignoring gcloud credentials due to error: {e:#?}"
            );
            return None;
        }
    };

    let token = match token_source.token_source().token().await {
        Ok(t) => t,
        Err(e) => {
            debug!(
                target = "mcap::cli",
                "Failed to fetch gcloud credentials: {e:#?}"
            );
            return None;
        }
    };

    match HeaderValue::from_str(&token) {
        Ok(t) => Some(t),
        Err(e) => {
            debug!(
                target = "mcap::cli",
                "Returned gcloud header was malformed: {e:#?}"
            );
            None
        }
    }
}

/// Create a reader the implements [`AsyncRead`] and [`AsyncWrite`], and is backed by a GCS file.
///
/// The current implementation does not support authenticated requests to GCS.
#[instrument]
pub async fn create_gcs_reader(
    bucket_name: &str,
    object_name: &str,
) -> CliResult<AsyncHttpRangeReader> {
    let bucket = utf8_percent_encode(bucket_name, GCS_ENCODE_SET);
    let object = utf8_percent_encode(object_name, GCS_ENCODE_SET);

    let url = format!(
        "https://storage.googleapis.com/download/storage/v1/b/{bucket}/o/{object}?alt=media"
    );

    let mut headers = HeaderMap::default();

    if let Some(header) = get_gcloud_header().await {
        headers.insert(AUTHORIZATION, header);
    }

    let url = Url::parse(&url)?;
    let client = reqwest_middleware::ClientWithMiddleware::default();

    let tail_response = AsyncHttpRangeReader::initial_tail_request(
        client.clone(),
        url.clone(),
        // Fetch the last 8KiB of the file. This will include the footer and for many files much of
        // the summary information.
        8192,
        headers.clone(),
    )
    .await?;

    let mut reader =
        AsyncHttpRangeReader::from_tail_response(client, tail_response, url, headers).await?;

    // Also prefetch the beginning of the file as we'll need it to extract the header information.
    reader.prefetch(0..1024).await;

    Ok(reader)
}

#[async_trait]
impl McapReader for AsyncHttpRangeReader {
    // Since remote files are latency bound, add an implementation of prefetch so certain known
    // regions of the file can be ready to go before reading.
    async fn prefetch(&mut self, bytes: std::ops::Range<u64>) {
        self.prefetch(bytes).await;
    }
}
