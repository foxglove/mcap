use async_http_range_reader::AsyncHttpRangeReader;
use async_trait::async_trait;
use futures::TryStreamExt;
use reqwest_middleware::reqwest::header::HeaderMap;
use tokio::io::{AsyncRead, BufReader};
use tokio_util::io::StreamReader;
use tracing::instrument;
use url::Url;

use crate::error::CliResult;

use super::SeekableMcapReader;

/// Create a reader the implements [`AsyncRead`] and [`AsyncWrite`], and is backed by an arbitrary URL.
///
/// For contiguous reads it is recommended to use [`create_url_reader`] instead.
#[instrument]
pub async fn create_seekable_url_reader(
    url: Url,
    headers: HeaderMap,
) -> CliResult<impl SeekableMcapReader> {
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

    // Wrap with a 1MiB buffer so frequent small reads don't slow everything down
    Ok(BufReader::with_capacity(1024 * 1024, reader))
}

/// Create a reader the implements [`AsyncRead`] and is backed by an arbitrary URL.
///
/// If you need a reader that implements [`AsyncSeek`] use [`create_seekable_url_reader`].
#[instrument]
pub async fn create_url_reader(url: Url, headers: HeaderMap) -> CliResult<impl AsyncRead> {
    let client = reqwest_middleware::ClientWithMiddleware::default();

    let stream = client
        .get(url)
        .headers(headers)
        .send()
        .await?
        .bytes_stream()
        .map_err(std::io::Error::other);

    // Wrap with a 1MiB buffer so frequent small reads don't slow everything down
    Ok(BufReader::with_capacity(
        1024 * 1024,
        StreamReader::new(stream),
    ))
}

#[async_trait]
impl SeekableMcapReader for BufReader<AsyncHttpRangeReader> {
    // Since remote files are latency bound, add an implementation of prefetch so certain known
    // regions of the file can be ready to go before reading.
    async fn prefetch(&mut self, bytes: std::ops::Range<u64>) {
        self.get_mut().prefetch(bytes).await;
    }
}
