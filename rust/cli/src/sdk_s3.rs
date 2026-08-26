//! S3 access through the official AWS SDK (`aws-sdk-s3`), exposed as an
//! `object_store::ObjectStore` so the CLI's remote plumbing (range reader,
//! chunked downloads, error formatting) is unchanged.
//!
//! `object_store` resolves AWS credentials from environment variables and
//! instance metadata only; the SDK brings the full aws CLI credential chain
//! (env, `~/.aws` profiles, SSO, `credential_process`, IMDS) with the same
//! precedence, plus credential caching/refresh via the service client's
//! identity cache and SDK region resolution (env, `~/.aws/config`, IMDS).
//!
//! Read paths only (`get_opts`/`head`): the CLI never writes to remote
//! stores, so every mutating method returns `NotImplemented`.
//!
//! Of object_store's `AWS_*` options this backend honors `AWS_ENDPOINT`,
//! `AWS_VIRTUAL_HOSTED_STYLE_REQUEST`, `AWS_SKIP_SIGNATURE` (unsigned reads
//! of public buckets), and `AWS_REQUEST_PAYER`; setting another recognized
//! object_store key prints a warning instead of being silently ignored.

use async_trait::async_trait;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::types::RequestPayer;
use futures_util::stream::BoxStream;
use futures_util::StreamExt as _;
use object_store::aws::AmazonS3ConfigKey;
use object_store::path::Path as ObjectStorePath;
use object_store::ClientConfigKey;
use object_store::{
    Attribute, Attributes, GetOptions, GetRange, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreScheme, PutMultipartOptions, PutOptions,
    PutPayload, PutResult,
};
use url::Url;

/// Build an SDK-backed S3 store for `url`. Credentials, region, and endpoint
/// resolution follow the SDK default chain (which reads the same `AWS_*` env
/// vars object_store did, plus `~/.aws` config); the object_store-specific
/// `AWS_ENDPOINT` and `AWS_VIRTUAL_HOSTED_STYLE_REQUEST` options are
/// translated for compatibility with the existing CLI docs.
pub fn build_s3_store(
    runtime: &tokio::runtime::Runtime,
    url: &Url,
    options: Vec<(String, String)>,
) -> object_store::Result<(Box<dyn ObjectStore>, ObjectStorePath)> {
    let (_, object_path) = ObjectStoreScheme::parse(url)?;
    let bucket = url
        .host_str()
        .ok_or_else(|| object_store::Error::Generic {
            store: "S3",
            source: format!("S3 URL {url} has no bucket").into(),
        })?
        .to_string();

    let mut endpoint = None;
    let mut virtual_hosted_style = None;
    let mut skip_signature = false;
    let mut request_payer = false;
    for (key, value) in options {
        match key.to_ascii_lowercase().parse::<AmazonS3ConfigKey>() {
            Ok(AmazonS3ConfigKey::Endpoint) => endpoint = Some(value),
            Ok(AmazonS3ConfigKey::VirtualHostedStyleRequest) => {
                virtual_hosted_style = Some(config_truthy(&value))
            }
            Ok(AmazonS3ConfigKey::SkipSignature) => skip_signature = config_truthy(&value),
            Ok(AmazonS3ConfigKey::RequestPayer) => request_payer = config_truthy(&value),
            // The SDK chain reads these env vars itself.
            Ok(
                AmazonS3ConfigKey::AccessKeyId
                | AmazonS3ConfigKey::SecretAccessKey
                | AmazonS3ConfigKey::Token
                | AmazonS3ConfigKey::Region
                | AmazonS3ConfigKey::DefaultRegion
                | AmazonS3ConfigKey::WebIdentityTokenFile
                | AmazonS3ConfigKey::RoleArn
                | AmazonS3ConfigKey::RoleSessionName
                | AmazonS3ConfigKey::ContainerCredentialsRelativeUri
                | AmazonS3ConfigKey::ContainerCredentialsFullUri
                | AmazonS3ConfigKey::ContainerAuthorizationTokenFile,
            ) => {}
            // The SDK does not gate plain-HTTP endpoints.
            Ok(AmazonS3ConfigKey::Client(ClientConfigKey::AllowHttp)) => {}
            // Recognized object_store option this backend does not support:
            // say so instead of silently changing behavior.
            Ok(_) => {
                eprintln!("Warning: ignoring {key}: not supported for s3:// reads");
            }
            // Not an object_store config key (e.g. AWS_PROFILE): the SDK
            // reads what it understands from the environment directly.
            Err(_) => {}
        }
    }

    let client = runtime.block_on(async {
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .http_client(ring_http_client());
        if skip_signature {
            // AWS_SKIP_SIGNATURE=true: send unsigned requests (public
            // buckets), like object_store did.
            loader = loader.no_credentials();
        }
        let sdk_config = loader.load().await;
        let mut builder = aws_sdk_s3::config::Builder::from(&sdk_config);
        if sdk_config.region().is_none() {
            if endpoint.is_some() {
                // S3-compatible endpoints (MinIO etc.) still need a region
                // string for SigV4 but typically ignore its value; match
                // object_store's default.
                builder = builder.region(aws_sdk_s3::config::Region::new("us-east-1"));
            } else {
                // Without a region every request fails with an opaque
                // dispatch error; fail up front with the fix instead.
                return Err(object_store::Error::Generic {
                    store: "S3",
                    source: "no AWS region configured: set AWS_REGION or add a region to \
                             ~/.aws/config"
                        .into(),
                });
            }
        }
        if let Some(endpoint) = endpoint {
            // A custom endpoint defaults to path-style, like object_store.
            builder = builder
                .endpoint_url(endpoint)
                .force_path_style(!virtual_hosted_style.unwrap_or(false));
        } else if virtual_hosted_style == Some(false) {
            builder = builder.force_path_style(true);
        }
        Ok(aws_sdk_s3::Client::from_conf(builder.build()))
    })?;

    Ok((
        Box::new(SdkS3Store {
            client,
            bucket,
            request_payer,
        }),
        object_path,
    ))
}

// Mirrors object_store's boolean config parsing ("1"/"true"/"on"/"yes"/"y").
fn config_truthy(value: &str) -> bool {
    matches!(
        value.to_ascii_lowercase().as_str(),
        "1" | "true" | "on" | "yes" | "y"
    )
}

// The SDK's HTTP client on the `ring` crypto backend: the default `aws-lc`
// backend compiles a C library via cmake, which the release cross-builds
// don't have, and `object_store`'s reqwest already uses `ring` anyway.
fn ring_http_client() -> aws_smithy_runtime_api::client::http::SharedHttpClient {
    aws_smithy_http_client::Builder::new()
        .tls_provider(aws_smithy_http_client::tls::Provider::Rustls(
            aws_smithy_http_client::tls::rustls_provider::CryptoMode::Ring,
        ))
        .build_https()
}

struct SdkS3Store {
    client: aws_sdk_s3::Client,
    bucket: String,
    // Send `x-amz-request-payer: requester` (requester-pays buckets).
    request_payer: bool,
}

impl std::fmt::Display for SdkS3Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SdkS3({})", self.bucket)
    }
}

impl std::fmt::Debug for SdkS3Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SdkS3Store({})", self.bucket)
    }
}

#[async_trait]
impl ObjectStore for SdkS3Store {
    async fn get_opts(
        &self,
        location: &ObjectStorePath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if options.head {
            return self.head_result(location).await;
        }
        let mut request = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(location.as_ref());
        if self.request_payer {
            request = request.request_payer(RequestPayer::Requester);
        }
        let ranged = options.range.is_some();
        if let Some(range) = &options.range {
            request = request.range(http_range_header(range));
        }
        if let Some(etag) = &options.if_match {
            request = request.if_match(etag);
        }
        let response = request
            .send()
            .await
            .map_err(|err| map_sdk_error(location, err))?;

        // object_store models a server that ignores `Range` (200 with the
        // full body) as NotSupported; the CLI's range probing relies on it.
        let content_range = response.content_range().and_then(parse_content_range);
        if ranged && content_range.is_none() {
            return Err(object_store::Error::NotSupported {
                source: "S3 server ignored the range request".into(),
            });
        }
        let content_length = response.content_length().unwrap_or(0).max(0) as u64;
        let (range, size) = match content_range {
            Some((start, end, total)) => (start..end + 1, total),
            None => (0..content_length, content_length),
        };
        let meta = ObjectMeta {
            location: location.clone(),
            last_modified: smithy_datetime_to_chrono(response.last_modified()),
            size,
            e_tag: response.e_tag().map(str::to_string),
            version: None,
        };
        let mut attributes = Attributes::new();
        if let Some(encoding) = response.content_encoding() {
            attributes.insert(Attribute::ContentEncoding, encoding.to_string().into());
        }
        let stream = futures_util::stream::try_unfold(response.body, |mut body| async move {
            match body.try_next().await {
                Ok(Some(bytes)) => Ok(Some((bytes, body))),
                Ok(None) => Ok(None),
                Err(err) => Err(object_store::Error::Generic {
                    store: "S3",
                    source: Box::new(err),
                }),
            }
        })
        .boxed();
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream),
            meta,
            range,
            attributes,
        })
    }

    async fn put_opts(
        &self,
        _location: &ObjectStorePath,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        Err(not_implemented("put"))
    }

    async fn put_multipart_opts(
        &self,
        _location: &ObjectStorePath,
        _opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Err(not_implemented("put_multipart"))
    }

    fn delete_stream(
        &self,
        _locations: BoxStream<'static, object_store::Result<ObjectStorePath>>,
    ) -> BoxStream<'static, object_store::Result<ObjectStorePath>> {
        futures_util::stream::once(async { Err(not_implemented("delete")) }).boxed()
    }

    fn list(
        &self,
        _prefix: Option<&ObjectStorePath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        futures_util::stream::once(async { Err(not_implemented("list")) }).boxed()
    }

    async fn list_with_delimiter(
        &self,
        _prefix: Option<&ObjectStorePath>,
    ) -> object_store::Result<ListResult> {
        Err(not_implemented("list_with_delimiter"))
    }

    async fn copy_opts(
        &self,
        _from: &ObjectStorePath,
        _to: &ObjectStorePath,
        _options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        Err(not_implemented("copy"))
    }
}

impl SdkS3Store {
    async fn head_result(&self, location: &ObjectStorePath) -> object_store::Result<GetResult> {
        let mut request = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(location.as_ref());
        if self.request_payer {
            request = request.request_payer(RequestPayer::Requester);
        }
        let response = request
            .send()
            .await
            .map_err(|err| map_sdk_error(location, err))?;
        let size = response.content_length().unwrap_or(0).max(0) as u64;
        let meta = ObjectMeta {
            location: location.clone(),
            last_modified: smithy_datetime_to_chrono(response.last_modified()),
            size,
            e_tag: response.e_tag().map(str::to_string),
            version: None,
        };
        Ok(GetResult {
            payload: GetResultPayload::Stream(futures_util::stream::empty().boxed()),
            meta,
            range: 0..size,
            attributes: Attributes::new(),
        })
    }
}

fn not_implemented(operation: &str) -> object_store::Error {
    object_store::Error::NotImplemented {
        operation: operation.to_string(),
        implementer: "SdkS3Store (read-only)".to_string(),
    }
}

fn http_range_header(range: &GetRange) -> String {
    match range {
        GetRange::Bounded(range) => {
            format!("bytes={}-{}", range.start, range.end.saturating_sub(1))
        }
        GetRange::Offset(offset) => format!("bytes={offset}-"),
        GetRange::Suffix(n) => format!("bytes=-{n}"),
    }
}

// "bytes start-end/total" (total may be "*", which S3 proper never sends for
// 206 responses; treat it like a missing header).
fn parse_content_range(header: &str) -> Option<(u64, u64, u64)> {
    let spec = header.strip_prefix("bytes ")?;
    let (range, total) = spec.split_once('/')?;
    let (start, end) = range.split_once('-')?;
    Some((
        start.trim().parse().ok()?,
        end.trim().parse().ok()?,
        total.trim().parse().ok()?,
    ))
}

fn smithy_datetime_to_chrono(
    datetime: Option<&aws_smithy_types::DateTime>,
) -> chrono::DateTime<chrono::Utc> {
    datetime
        .and_then(|dt| chrono::DateTime::from_timestamp(dt.secs(), dt.subsec_nanos()))
        .unwrap_or_default()
}

// Map SDK errors onto the typed object_store variants the CLI's concise
// error formatting understands; other statuses use the same Display phrasing
// object_store produces so `status_from_object_store_message` still parses.
fn map_sdk_error<E, R>(location: &ObjectStorePath, err: SdkError<E, R>) -> object_store::Error
where
    E: std::error::Error + Send + Sync + 'static,
    R: std::fmt::Debug + Send + Sync + 'static,
    SdkError<E, R>: RawStatus,
{
    let path = location.to_string();
    let status = err.raw_status();
    let source: Box<dyn std::error::Error + Send + Sync> = Box::new(err);
    match status {
        Some(404) => object_store::Error::NotFound { path, source },
        Some(412) => object_store::Error::Precondition { path, source },
        Some(304) => object_store::Error::NotModified { path, source },
        Some(401) => object_store::Error::Unauthenticated { path, source },
        Some(403) => object_store::Error::PermissionDenied { path, source },
        Some(code) => object_store::Error::Generic {
            store: "S3",
            source: format!("Server returned non-2xx status code: {code}: {source}").into(),
        },
        None => object_store::Error::Generic {
            store: "S3",
            source,
        },
    }
}

trait RawStatus {
    fn raw_status(&self) -> Option<u16>;
}

impl<E> RawStatus for SdkError<E, aws_smithy_runtime_api::http::Response> {
    fn raw_status(&self) -> Option<u16> {
        self.raw_response()
            .map(|response| response.status().as_u16())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_s3::config::{Credentials, Region, SharedCredentialsProvider};
    use futures_util::TryStreamExt as _;
    use object_store::ObjectStoreExt as _;
    use std::io::{BufRead as _, BufReader, Write as _};
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    fn test_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build runtime")
    }

    // Minimal S3 endpoint: serves ranged GETs of `body` (or `status` when
    // non-200), records request heads, counts requests.
    struct FakeS3 {
        url: String,
        heads: Arc<Mutex<Vec<String>>>,
        requests: Arc<AtomicUsize>,
    }

    fn serve_fake_s3(body: &'static [u8], status: Option<&'static str>) -> FakeS3 {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake s3");
        let addr = listener.local_addr().expect("addr");
        let heads = Arc::new(Mutex::new(Vec::new()));
        let requests = Arc::new(AtomicUsize::new(0));
        let (server_heads, server_requests) = (heads.clone(), requests.clone());
        std::thread::spawn(move || {
            for stream in listener.incoming().take(8) {
                let stream = stream.expect("accept");
                let mut reader = BufReader::new(stream);
                let mut head = String::new();
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).is_err() || line == "\r\n" || line.is_empty() {
                        break;
                    }
                    head.push_str(&line);
                }
                if head.is_empty() {
                    continue;
                }
                server_requests.fetch_add(1, Ordering::SeqCst);
                let is_head = head.starts_with("HEAD ");
                let range = head
                    .lines()
                    .find_map(|line| {
                        line.strip_prefix("range: bytes=")
                            .or_else(|| line.strip_prefix("Range: bytes="))
                    })
                    .and_then(|spec| spec.trim().split_once('-'))
                    .and_then(|(start, end)| {
                        Some((start.parse::<usize>().ok()?, end.parse::<usize>().ok()?))
                    });
                server_heads.lock().expect("lock heads").push(head);
                let mut stream = reader.into_inner();
                if let Some(status) = status {
                    stream
                        .write_all(
                            format!(
                                "HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                            )
                            .as_bytes(),
                        )
                        .expect("write status");
                    continue;
                }
                let response = match range {
                    Some((start, end)) => {
                        let end = end.min(body.len().saturating_sub(1));
                        let content = &body[start..=end];
                        let mut response = format!(
                            "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{end}/{}\r\nETag: \"fake\"\r\nConnection: close\r\n\r\n",
                            content.len(),
                            body.len(),
                        )
                        .into_bytes();
                        if !is_head {
                            response.extend_from_slice(content);
                        }
                        response
                    }
                    None => {
                        let mut response = format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nETag: \"fake\"\r\nConnection: close\r\n\r\n",
                            body.len()
                        )
                        .into_bytes();
                        if !is_head {
                            response.extend_from_slice(body);
                        }
                        response
                    }
                };
                stream.write_all(&response).expect("write response");
            }
        });
        FakeS3 {
            url: format!("http://{addr}"),
            heads,
            requests,
        }
    }

    fn test_store_config(endpoint: &str, signed: bool, request_payer: bool) -> SdkS3Store {
        let mut builder = aws_sdk_s3::config::Builder::new()
            .behavior_version(aws_config::BehaviorVersion::latest())
            .region(Region::new("us-east-2"))
            .endpoint_url(endpoint)
            .force_path_style(true)
            .http_client(ring_http_client());
        if signed {
            builder = builder.credentials_provider(SharedCredentialsProvider::new(
                Credentials::new("AKIATEST", "secret", None, None, "test"),
            ));
        }
        SdkS3Store {
            client: aws_sdk_s3::Client::from_conf(builder.build()),
            bucket: "test-bucket".to_string(),
            request_payer,
        }
    }

    fn test_store(endpoint: &str) -> SdkS3Store {
        test_store_config(endpoint, true, false)
    }

    #[test]
    fn unsigned_store_sends_no_authorization_header() {
        let body: &'static [u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
        let server = serve_fake_s3(body, None);
        // No credentials provider, like build_s3_store with
        // AWS_SKIP_SIGNATURE=true (`no_credentials()`).
        let store = test_store_config(&server.url, false, false);
        test_runtime()
            .block_on(store.get_opts(
                &ObjectStorePath::from("demo.mcap"),
                GetOptions {
                    range: Some(GetRange::Bounded(0..8)),
                    ..GetOptions::default()
                },
            ))
            .expect("unsigned ranged get");
        let heads = server.heads.lock().expect("lock heads");
        assert!(
            heads
                .iter()
                .all(|head| !head.to_ascii_lowercase().contains("authorization:")),
            "unsigned request should carry no Authorization header, got:\n{heads:?}"
        );
    }

    #[test]
    fn request_payer_get_carries_requester_header() {
        let body: &'static [u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
        let server = serve_fake_s3(body, None);
        let store = test_store_config(&server.url, true, true);
        test_runtime()
            .block_on(store.get_opts(
                &ObjectStorePath::from("demo.mcap"),
                GetOptions {
                    range: Some(GetRange::Bounded(0..8)),
                    ..GetOptions::default()
                },
            ))
            .expect("requester-pays ranged get");
        let heads = server.heads.lock().expect("lock heads");
        assert!(
            heads.iter().all(|head| head
                .to_ascii_lowercase()
                .contains("x-amz-request-payer: requester")),
            "request should carry x-amz-request-payer, got:\n{heads:?}"
        );
    }

    #[test]
    fn ranged_get_is_signed_and_parses_content_range() {
        let body: &'static [u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
        let server = serve_fake_s3(body, None);
        let store = test_store(&server.url);
        let result = test_runtime()
            .block_on(async {
                let result = store
                    .get_opts(
                        &ObjectStorePath::from("demo.mcap"),
                        GetOptions {
                            range: Some(GetRange::Bounded(8..16)),
                            ..GetOptions::default()
                        },
                    )
                    .await?;
                let meta = result.meta.clone();
                let range = result.range.clone();
                let GetResultPayload::Stream(stream) = result.payload else {
                    panic!("expected a stream payload");
                };
                let bytes: Vec<_> = stream.try_collect().await?;
                Ok::<_, object_store::Error>((meta, range, bytes.concat()))
            })
            .expect("ranged get");
        let (meta, range, bytes) = result;
        assert_eq!(bytes, &body[8..16]);
        assert_eq!(range, 8..16);
        assert_eq!(
            meta.size,
            body.len() as u64,
            "size comes from Content-Range total"
        );
        assert_eq!(meta.e_tag.as_deref(), Some("\"fake\""));
        let heads = server.heads.lock().expect("lock heads");
        assert!(
            heads.iter().any(|head| head.contains("AKIATEST")),
            "request should be SigV4-signed, got:\n{heads:?}"
        );
    }

    #[test]
    fn head_uses_head_object() {
        let body: &'static [u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
        let server = serve_fake_s3(body, None);
        let store = test_store(&server.url);
        let meta = test_runtime()
            .block_on(store.head(&ObjectStorePath::from("demo.mcap")))
            .expect("head");
        assert_eq!(meta.size, body.len() as u64);
        let heads = server.heads.lock().expect("lock heads");
        assert!(
            heads.iter().all(|head| head.starts_with("HEAD ")),
            "head() should issue a HeadObject, got:\n{heads:?}"
        );
    }

    #[test]
    fn missing_object_maps_to_typed_not_found() {
        let server = serve_fake_s3(b"", Some("404 Not Found"));
        let store = test_store(&server.url);
        let err = test_runtime()
            .block_on(store.get_opts(
                &ObjectStorePath::from("missing.mcap"),
                GetOptions::default(),
            ))
            .expect_err("404 should map to an error");
        assert!(
            matches!(err, object_store::Error::NotFound { .. }),
            "expected NotFound, got: {err:?}"
        );
        assert_eq!(server.requests.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn range_headers_match_http_syntax() {
        assert_eq!(http_range_header(&GetRange::Bounded(0..8)), "bytes=0-7");
        assert_eq!(http_range_header(&GetRange::Offset(16)), "bytes=16-");
        assert_eq!(
            http_range_header(&GetRange::Suffix(250_000)),
            "bytes=-250000"
        );
    }

    #[test]
    fn content_range_parses_start_end_total() {
        assert_eq!(parse_content_range("bytes 0-7/36"), Some((0, 7, 36)));
        assert_eq!(parse_content_range("bytes 8-35/36"), Some((8, 35, 36)));
        assert_eq!(parse_content_range("bytes 0-0/*"), None);
        assert_eq!(parse_content_range("garbage"), None);
    }
}
