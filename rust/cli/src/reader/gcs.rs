use async_http_range_reader::AsyncHttpRangeReader;
use async_trait::async_trait;
use google_cloud_auth::{project::Config as GcloudConfig, token::DefaultTokenSourceProvider};
use google_cloud_token::TokenSourceProvider;
use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
use reqwest_middleware::reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};
use tokio::io::AsyncRead;
use tracing::{debug, instrument};
use url::Url;

use crate::{error::CliResult, reader::SeekableMcapReader};

use super::url::{create_seekable_url_reader, create_url_reader};

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

async fn get_gcs_reader_args(bucket_name: &str, object_name: &str) -> CliResult<(Url, HeaderMap)> {
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

    Ok((url, headers))
}

/// Create a reader the implements [`AsyncRead`] and [`AsyncWrite`], and is backed by a GCS file.
///
/// If you have default GCS credentials available it will use them to authenticate with Google
/// Cloud.
#[instrument]
pub async fn create_seekable_gcs_reader(
    bucket_name: &str,
    object_name: &str,
) -> CliResult<impl SeekableMcapReader> {
    let (url, headers) = get_gcs_reader_args(bucket_name, object_name).await?;
    create_seekable_url_reader(url, headers).await
}

/// Create a reader the implements [`AsyncRead`] and is backed by a GCS file.
///
/// See [`create_seekable_gcs_reader`] if you need the [`AsyncSeek`] trait to be imp
#[instrument]
pub async fn create_gcs_reader(bucket_name: &str, object_name: &str) -> CliResult<impl AsyncRead> {
    let (url, headers) = get_gcs_reader_args(bucket_name, object_name).await?;
    create_url_reader(url, headers).await
}

#[async_trait]
impl SeekableMcapReader for AsyncHttpRangeReader {
    // Since remote files are latency bound, add an implementation of prefetch so certain known
    // regions of the file can be ready to go before reading.
    async fn prefetch(&mut self, bytes: std::ops::Range<u64>) {
        self.prefetch(bytes).await;
    }
}
