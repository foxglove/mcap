use async_http_range_reader::{AsyncHttpRangeReader, CheckSupportMethod};
use reqwest_middleware::reqwest::header::HeaderMap;
use url::Url;

use crate::error::CliResult;

pub async fn create_url_reader(url: Url) -> CliResult<AsyncHttpRangeReader> {
    let (mut file, _) = AsyncHttpRangeReader::new(
        reqwest_middleware::ClientWithMiddleware::default(),
        url,
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
