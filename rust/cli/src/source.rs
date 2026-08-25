use std::io::{IsTerminal as _, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use binrw::BinRead;
use futures_util::TryStreamExt;
use mcap::records::{self, Record};
use object_store::{
    path::Path as ObjectStorePath, Attribute, BackoffConfig, ClientConfigKey, GetOptions, GetRange,
    ObjectStore, ObjectStoreExt, ObjectStoreScheme, RetryConfig,
};
use tempfile::NamedTempFile;
use url::Url;

use crate::parse::{self, ParsedMcap};
use crate::render::human_bytes;

pub const PLEASE_REDIRECT: &str =
    "Binary output can screw up your terminal. Supply -o or redirect to a file or pipe";
pub const PLEASE_SUPPLY_FILE: &str = "please supply a file. see --help for usage details.";
// Size of the single range request issued from the end of a remote file to discover
// the summary section. One read proves range support (for HTTP), discovers the file
// size via `Content-Range`, and in the common case already contains the whole summary
// section (footer + summary + summary offset records). When the summary is larger than
// this, exactly one additional range request back-fills the missing prefix. 250 kB
// comfortably covers the summaries of typical multi-hundred-MB to low-GB files while
// keeping the per-open transfer small on bandwidth-constrained links.
const REMOTE_SUMMARY_TAIL_BYTES: u64 = 250_000;
// Guards aggregate remote reads that should stay index-like (summary bytes, or
// multiple metadata records selected from indexes) from becoming unexpectedly large.
pub(crate) const MAX_REMOTE_INDEXED_BYTES_WITHOUT_SCAN: u64 = 100_000_000;
// Ranged GET size for whole-file downloads; each part is a new request with its
// own ETag check and retry budget. Resumes are byte-granular, so this size does
// not affect how much is re-fetched.
const REMOTE_DOWNLOAD_CHUNK_BYTES: u64 = 64 * 1024 * 1024;
// object_store's default 30s timeout spans the whole body and kills large
// transfers; use an effectively unlimited one and enforce liveness below instead.
const REMOTE_REQUEST_TIMEOUT: &str = "7days";
const REMOTE_STALL_TIMEOUT: Duration = Duration::from_secs(120);
// A hung connection never errors, so object_store's retries never see it. Bound
// the wait for the response head here and retry it a few times.
const REMOTE_RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);
const REMOTE_RESPONSE_ATTEMPTS: usize = 3;
// object_store's retries run inside the REMOTE_RESPONSE_TIMEOUT window, so they
// must finish within it or a throttled request would be reported as a hang.
const REMOTE_STORE_RETRIES: usize = 10;
const REMOTE_STORE_RETRY_TIMEOUT: Duration = Duration::from_secs(15);
const REMOTE_STORE_MAX_BACKOFF: Duration = Duration::from_secs(5);
const _: () = assert!(
    REMOTE_STORE_RETRY_TIMEOUT.as_secs() + REMOTE_STORE_MAX_BACKOFF.as_secs()
        < REMOTE_RESPONSE_TIMEOUT.as_secs(),
    "object_store's retry sequence must end before the response-head timeout"
);
// Consecutive zero-progress attempts before a chunked download gives up.
// Any delivered bytes reset the budget, so only a dead connection exhausts it.
const REMOTE_DOWNLOAD_NO_PROGRESS_ATTEMPTS: usize = 5;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SourceOptions {
    pub allow_remote_scan: bool,
    pub scan_data_without_statistics: bool,
}

impl SourceOptions {
    pub fn new(allow_remote_scan: bool) -> Self {
        Self {
            allow_remote_scan,
            ..Self::default()
        }
    }

    pub fn scan_data_without_statistics(mut self, scan_data_without_statistics: bool) -> Self {
        self.scan_data_without_statistics = scan_data_without_statistics;
        self
    }
}

pub struct MaterializedInput {
    temp_file: Option<NamedTempFile>,
    local_path: Option<std::path::PathBuf>,
}

impl MaterializedInput {
    pub fn path(&self) -> &Path {
        if let Some(temp_file) = &self.temp_file {
            temp_file.path()
        } else {
            self.local_path
                .as_deref()
                .expect("materialized input should have a path")
        }
    }
}

pub fn ensure_distinct_local_input_output(input: &Path, output: &Path) -> Result<()> {
    if is_remote_url(input) {
        return Ok(());
    }

    let input_path = match input.canonicalize() {
        Ok(path) => path,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => {
            return Err(err)
                .with_context(|| format!("failed to resolve input path '{}'", input.display()));
        }
    };
    let (output_path, output_exists) = match output.canonicalize() {
        Ok(path) => (path, true),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            let parent = output
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."));
            let file_name = output
                .file_name()
                .with_context(|| format!("invalid output path '{}'", output.display()))?;
            match parent.canonicalize() {
                Ok(parent_path) => (parent_path.join(file_name), false),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                Err(err) => {
                    return Err(err).with_context(|| {
                        format!("failed to resolve output parent '{}'", parent.display())
                    });
                }
            }
        }
        Err(err) => {
            return Err(err)
                .with_context(|| format!("failed to resolve output path '{}'", output.display()));
        }
    };

    anyhow::ensure!(
        !local_paths_refer_to_same_file(input, output, &input_path, &output_path, output_exists)?,
        "input and output paths must be different"
    );
    Ok(())
}

fn local_paths_refer_to_same_file(
    input: &Path,
    output: &Path,
    input_path: &Path,
    output_path: &Path,
    output_exists: bool,
) -> Result<bool> {
    if input_path == output_path {
        return Ok(true);
    }
    if !output_exists {
        return Ok(false);
    }

    local_paths_have_same_file_id(input, output)
}

#[cfg(unix)]
fn local_paths_have_same_file_id(input: &Path, output: &Path) -> Result<bool> {
    use std::os::unix::fs::MetadataExt;

    let input_metadata = input
        .metadata()
        .with_context(|| format!("failed to stat input path '{}'", input.display()))?;
    let output_metadata = output
        .metadata()
        .with_context(|| format!("failed to stat output path '{}'", output.display()))?;
    Ok(input_metadata.dev() == output_metadata.dev()
        && input_metadata.ino() == output_metadata.ino())
}

#[cfg(not(unix))]
fn local_paths_have_same_file_id(_input: &Path, _output: &Path) -> Result<bool> {
    // Same-path and symlink aliases are still caught by canonical path equality above. Detecting
    // NTFS hard links would require platform-specific file IDs beyond the standard library.
    Ok(false)
}

pub fn parse_mcap_from_path(path: &Path, options: SourceOptions) -> Result<ParsedMcap> {
    if is_remote_url(path) {
        let mut stats_scan_fallback = false;
        match open_remote_range_reader(path)? {
            Some(mut reader) => {
                if let Some(summary_bytes) = read_summary_bytes_from_remote(&mut reader, options)
                    .map_err(|err| remote_read_error(path, err))?
                {
                    let header = read_header_from_seekable(&mut reader).map_err(|err| {
                        remote_read_error(path, classify_remote_summary_error(&reader, err))
                    })?;
                    let parsed = parse::parsed_mcap_from_summary_section(header, &summary_bytes)
                        .map_err(|err| {
                            remote_read_error(path, classify_remote_summary_error(&reader, err))
                        })?;
                    if parsed.statistics.is_some()
                        || !options.scan_data_without_statistics
                        || !options.allow_remote_scan
                    {
                        return Ok(parsed);
                    }
                    stats_scan_fallback = true;
                } else if !options.allow_remote_scan {
                    bail!(
                        "failed to read {}\nRemote file has no summary section; reading without one requires opt-in; {}",
                        redacted_display(path),
                        remote_scan_opt_in_suffix()
                    );
                }
            }
            None if !options.allow_remote_scan => {
                bail!(
                    "failed to read {}\nRemote server does not support range requests; {}",
                    redacted_display(path),
                    remote_scan_opt_in_suffix()
                );
            }
            None => {}
        }

        // Remote linear / stats-scan fallback via ByteSource (no mmap).
        let mut source = crate::byte_source::open_byte_source(Some(path), options)?;
        let header = crate::byte_source::read_header(source.as_mut())
            .map_err(|err| remote_read_error(path, err))?;
        if stats_scan_fallback {
            eprintln!(
                "Warning: Statistics record not available; full scan may be slow. Run `mcap doctor` for details."
            );
        } else {
            eprintln!(
                "Warning: summary section not available; full scan may be slow. Run `mcap doctor` for details."
            );
        }
        return parse::parse_mcap_linear_from_byte_source(source.as_mut(), header)
            .map_err(|err| remote_read_error(path, err));
    }

    let mut source = crate::byte_source::open_byte_source(Some(path), options)?;
    let header = crate::byte_source::read_header(source.as_mut())?;
    if let Some(parsed) = parse::try_parsed_mcap_from_summary(source.as_mut(), header.clone())? {
        let want_stats_scan = options.scan_data_without_statistics && parsed.statistics.is_none();
        if !want_stats_scan {
            return Ok(parsed);
        }
        eprintln!(
            "Warning: Statistics record not available; full scan may be slow. Run `mcap doctor` for details."
        );
        return parse::parse_mcap_linear_from_byte_source(source.as_mut(), header);
    }

    eprintln!(
        "Warning: summary section not available; full scan may be slow. Run `mcap doctor` for details."
    );
    parse::parse_mcap_linear_from_byte_source(source.as_mut(), header)
}

pub fn materialize_input(path: &Path, options: SourceOptions) -> Result<MaterializedInput> {
    if !is_remote_url(path) {
        return Ok(MaterializedInput {
            temp_file: None,
            local_path: Some(path.to_path_buf()),
        });
    }

    require_remote_scan_allowed(path, options)?;
    let suffix = remote_or_local_extension(path)
        .filter(|extension| !extension.is_empty())
        .map(|extension| format!(".{extension}"));
    let mut builder = tempfile::Builder::new();
    builder.prefix("mcap-cli-remote-input-");
    if let Some(suffix) = suffix.as_deref() {
        builder.suffix(suffix);
    }
    let mut temp_file = builder
        .tempfile()
        .context("failed to create temporary remote input file")?;
    // Downloads arrive as many small network frames; buffer them so the
    // temp file sees large writes instead of one syscall per frame.
    let mut writer = std::io::BufWriter::new(temp_file.as_file_mut());
    read_remote_input_to_writer(path, &mut writer)?;
    writer
        .flush()
        .context("failed to flush temporary remote input file")?;
    // `writer` mutably borrows `temp_file` and has a `Drop` impl, so the borrow
    // lasts until it is dropped; release it before moving `temp_file` out.
    drop(writer);
    Ok(MaterializedInput {
        temp_file: Some(temp_file),
        local_path: None,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RemoteUrlKind {
    /// Plain HTTP(S). Range support is not guaranteed: a server may ignore `Range`
    /// and return the whole body, which we detect and treat as "no range support".
    Http,
    /// Cloud object store that supports HTTP suffix range requests (`bytes=-N`), so
    /// the file size and trailing summary can be fetched in a single request without
    /// a prior HEAD. Covers AWS S3 (and S3-compatible) and Google Cloud Storage.
    CloudSuffix,
    /// Cloud object store that does not support suffix range requests (Azure Blob
    /// Storage). Bounded ranges work, so we discover the size with a HEAD first and
    /// then read a bounded tail. `object_store` rejects `GetRange::Suffix` for Azure
    /// before issuing any request.
    CloudNoSuffix,
}

impl RemoteUrlKind {
    fn from_scheme(scheme: &str) -> Option<Self> {
        match scheme.to_ascii_lowercase().as_str() {
            "http" | "https" => Some(Self::Http),
            "s3" | "s3a" | "gs" => Some(Self::CloudSuffix),
            "az" | "adl" | "azure" | "abfs" | "abfss" => Some(Self::CloudNoSuffix),
            _ => None,
        }
    }

    /// Whether range support is guaranteed by the store. When false (HTTP), an
    /// open-time read that comes back without partial content means the server does
    /// not honor ranges and the caller must fall back to a scan/full download.
    fn range_support_is_guaranteed(self) -> bool {
        !matches!(self, Self::Http)
    }

    /// Whether `bytes=-N` suffix range requests are supported, letting us fetch the
    /// tail (and learn the size) in a single request without a prior HEAD.
    fn supports_suffix_range(self) -> bool {
        !matches!(self, Self::CloudNoSuffix)
    }
}

fn remote_url_kind(path: &Path) -> Option<RemoteUrlKind> {
    let text = path.to_str()?;
    let (scheme, _) = text.split_once("://")?;
    RemoteUrlKind::from_scheme(scheme)
}

pub fn is_remote_url(path: &Path) -> bool {
    remote_url_kind(path).is_some()
}

#[derive(Debug, Clone)]
struct RemoteUrl {
    url: Url,
    display_url: String,
    kind: RemoteUrlKind,
}

impl RemoteUrl {
    fn parse(path: &Path) -> Result<Self> {
        let raw_url = path.to_str().ok_or_else(|| {
            anyhow::anyhow!("remote URL is not valid UTF-8: '{}'", path.display())
        })?;
        let display_url = redact_url(raw_url);
        let url = Url::parse(raw_url).with_context(|| format!("failed to parse {display_url}"))?;
        let kind = RemoteUrlKind::from_scheme(url.scheme()).ok_or_else(|| {
            anyhow::anyhow!(
                "unsupported remote URL scheme '{}' for {display_url}",
                url.scheme()
            )
        })?;
        Ok(Self {
            url,
            display_url,
            kind,
        })
    }

    fn options(&self) -> Vec<(String, String)> {
        self.options_from_env_vars(std::env::vars_os())
    }

    /// `options()` plus the long request timeout (last-wins over any env timeout).
    fn store_options(&self) -> Vec<(String, String)> {
        let mut options = self.options();
        options.push((
            ClientConfigKey::Timeout.as_ref().to_string(),
            REMOTE_REQUEST_TIMEOUT.to_string(),
        ));
        options
    }

    fn options_from_env_vars(
        &self,
        vars: impl IntoIterator<Item = (std::ffi::OsString, std::ffi::OsString)>,
    ) -> Vec<(String, String)> {
        match self.kind {
            RemoteUrlKind::Http if self.url.scheme() == "http" => {
                vec![("allow_http".to_string(), "true".to_string())]
            }
            RemoteUrlKind::Http => Vec::new(),
            RemoteUrlKind::CloudSuffix | RemoteUrlKind::CloudNoSuffix => {
                object_store_options_from_env_vars(vars)
            }
        }
    }

    fn extension(&self) -> Option<String> {
        Path::new(self.url.path())
            .extension()
            .and_then(|extension| extension.to_str())
            .map(str::to_string)
    }
}

pub fn remote_or_local_extension(path: &Path) -> Option<String> {
    if is_remote_url(path) {
        return RemoteUrl::parse(path).ok()?.extension();
    }
    path.extension()
        .and_then(|extension| extension.to_str())
        .map(str::to_string)
}

struct ObjectStoreSource {
    runtime: Arc<tokio::runtime::Runtime>,
    store: Arc<dyn ObjectStore>,
    path: ObjectStorePath,
    display_url: String,
    // `REMOTE_RESPONSE_TIMEOUT`; a field so tests can shorten it.
    response_timeout: Duration,
}

impl ObjectStoreSource {
    fn open_for_download(path: &Path) -> Result<Self> {
        Self::open_remote(RemoteUrl::parse(path)?)
    }

    fn open_remote(remote_url: RemoteUrl) -> Result<Self> {
        let runtime = object_store_runtime()?;
        // S3 goes through the AWS SDK for its full credential chain (profiles,
        // SSO) and manages its own timeouts and retries; other stores use
        // object_store with the timeout and retry budget chosen above.
        let result = if matches!(remote_url.url.scheme(), "s3" | "s3a") {
            crate::sdk_s3::build_s3_store(&runtime, &remote_url.url, remote_url.options())
        } else {
            build_object_store(&remote_url.url, remote_url.store_options())
        };
        // object_store errors repeat their source in Display, so flatten to a
        // single message instead of letting the anyhow chain print it twice.
        let (store, object_path) = result.map_err(|err| {
            anyhow::anyhow!(
                "failed to configure remote store for {}: {err}",
                remote_url.display_url
            )
        })?;
        Ok(Self {
            runtime,
            store: Arc::from(store),
            path: object_path,
            display_url: remote_url.display_url,
            response_timeout: REMOTE_RESPONSE_TIMEOUT,
        })
    }

    fn stat(&self) -> Result<object_store::ObjectMeta> {
        self.block_on_bounded(REMOTE_RESPONSE_ATTEMPTS, || self.store.head(&self.path))?
            .map_err(|err| concise_remote_stat_error(&self.display_url, err))
    }

    fn head_size(&self) -> Result<u64> {
        Ok(self.stat()?.size)
    }

    /// Read a bounded byte range and return its bytes, validating the content
    /// encoding. The range is assumed to be valid (non-empty, within the object).
    fn get_range(&self, range: std::ops::Range<u64>) -> Result<Vec<u8>> {
        let response = self
            .get_opts_bounded(
                REMOTE_RESPONSE_ATTEMPTS,
                GetOptions {
                    range: Some(GetRange::Bounded(range)),
                    ..GetOptions::default()
                },
            )?
            .map_err(|err| {
                concise_remote_operation_error("fetching range from", &self.display_url, err)
            })?;
        validate_identity_content_encoding(&response.attributes, &self.display_url)?;
        self.collect_body(response)
    }

    /// Run a store request, retrying up to `attempts` times when no response
    /// head arrives within `response_timeout`. The inner result is the store's own.
    fn block_on_bounded<T, F>(
        &self,
        attempts: usize,
        mut request: impl FnMut() -> F,
    ) -> Result<object_store::Result<T>>
    where
        F: std::future::Future<Output = object_store::Result<T>>,
    {
        self.runtime.block_on(async {
            let mut attempt = 0;
            loop {
                attempt += 1;
                match tokio::time::timeout(self.response_timeout, request()).await {
                    Ok(result) => return Ok(result),
                    Err(_) if attempt < attempts => eprintln!(
                        "Warning: no response from {} after {:?}, retrying",
                        self.display_url, self.response_timeout
                    ),
                    Err(_) => {
                        return Err(anyhow::anyhow!(
                            "failed to read {}: timed out waiting for response ({:?}, {attempt} attempts)",
                            self.display_url,
                            self.response_timeout
                        ))
                    }
                }
            }
        })
    }

    fn get_opts_bounded(
        &self,
        attempts: usize,
        options: GetOptions,
    ) -> Result<std::result::Result<object_store::GetResult, object_store::Error>> {
        self.block_on_bounded(attempts, || {
            self.store.get_opts(&self.path, options.clone())
        })
    }

    /// Like `GetResult::bytes`, but failing if no bytes arrive for `REMOTE_STALL_TIMEOUT`.
    fn collect_body(&self, response: object_store::GetResult) -> Result<Vec<u8>> {
        let expected = response.range.end.saturating_sub(response.range.start);
        let mut out = Vec::with_capacity(
            usize::try_from(expected.min(REMOTE_DOWNLOAD_CHUNK_BYTES)).unwrap_or(0),
        );
        self.runtime.block_on(async {
            let mut stream = response.into_stream();
            loop {
                let Ok(next) = tokio::time::timeout(REMOTE_STALL_TIMEOUT, stream.try_next()).await
                else {
                    bail!(
                        "failed to read range from {}: stalled (no data for {}s)",
                        self.display_url,
                        REMOTE_STALL_TIMEOUT.as_secs()
                    );
                };
                match next {
                    Ok(Some(bytes)) => out.extend_from_slice(&bytes),
                    Ok(None) => return Ok(out),
                    Err(err) => {
                        return Err(anyhow::Error::new(err)
                            .context(format!("failed to read range from {}", self.display_url)))
                    }
                }
            }
        })
    }

    /// Probe bounded range support with a one-byte request, returning the object
    /// size parsed from `Content-Range`. `Ok(None)` means the server does not honor
    /// range requests at all. Used as a fallback for HTTP servers that accept bounded
    /// ranges but reject suffix ranges, and to learn the size without a HEAD (which
    /// some HTTP servers reject).
    fn probe_bounded_range_size(&self) -> Result<Option<u64>> {
        match self.get_opts_bounded(
            REMOTE_RESPONSE_ATTEMPTS,
            GetOptions {
                range: Some(GetRange::Bounded(0..1)),
                ..GetOptions::default()
            },
        )? {
            Ok(response) => {
                validate_identity_content_encoding(&response.attributes, &self.display_url)?;
                // A `*` total in `Content-Range` fails object_store's parse and
                // surfaces as a fetch error rather than a bogus size.
                Ok(Some(response.meta.size))
            }
            Err(err) if remote_range_not_supported(&err) => Ok(None),
            Err(err) => Err(concise_remote_operation_error(
                "fetching range from",
                &self.display_url,
                err,
            )),
        }
    }

    /// Read the final `tail_bytes` of an object of known `size` as a bounded range.
    fn bounded_tail(&self, size: u64, tail_bytes: u64) -> Result<RemoteTail> {
        let bytes = self.get_range(size.saturating_sub(tail_bytes)..size)?;
        let start = size.saturating_sub(bytes.len() as u64);
        Ok(RemoteTail { start, bytes })
    }

    /// Read the final `tail_bytes` of the object in a single request, returning the
    /// file size and the fetched tail. Returns `Ok(None)` only when the store does
    /// not support range requests at all (an HTTP server that ignores `Range`),
    /// signalling the caller to fall back to a scan/full download.
    fn read_summary_tail(
        &self,
        kind: RemoteUrlKind,
        tail_bytes: u64,
    ) -> Result<Option<(u64, RemoteTail)>> {
        if kind.supports_suffix_range() {
            // A suffix request proves range support, discovers the size via
            // `Content-Range`, and returns the tail in one round trip. If the object
            // is shorter than `tail_bytes`, servers return the entire object.
            match self.get_opts_bounded(
                REMOTE_RESPONSE_ATTEMPTS,
                GetOptions {
                    range: Some(GetRange::Suffix(tail_bytes)),
                    ..GetOptions::default()
                },
            )? {
                Ok(response) => {
                    validate_identity_content_encoding(&response.attributes, &self.display_url)?;
                    // Relies on object_store parsing a numeric total from
                    // `Content-Range` (for example `bytes 9-99/100`). A `*` total
                    // fails object_store's parse and surfaces as a fetch error rather
                    // than a bogus size.
                    let size = response.meta.size;
                    let bytes = self.collect_body(response)?;
                    let start = size.saturating_sub(bytes.len() as u64);
                    return Ok(Some((size, RemoteTail { start, bytes })));
                }
                // HTTP servers may honor bounded ranges but not suffix ranges, either
                // ignoring the suffix (`200` -> `NotSupported`) or rejecting it (e.g.
                // `416`, `500`, etc.). object_store does not expose every
                // unsupported-suffix case as a distinct error, so retry with a bounded
                // probe even for odd suffix errors like 404 and let that request decide
                // whether ranges are usable or the caller should fall back to a scan.
                Err(_) if !kind.range_support_is_guaranteed() => {
                    return Ok(match self.probe_bounded_range_size()? {
                        Some(size) => Some((size, self.bounded_tail(size, tail_bytes)?)),
                        None => None,
                    });
                }
                // A suffix-capable cloud store should never report the suffix as
                // unsupported, but if it does we still have guaranteed range support,
                // so fall through to the bounded path using a HEAD-discovered size.
                Err(err) if remote_range_not_supported(&err) => {}
                Err(err) => {
                    return Err(concise_remote_operation_error(
                        "fetching range from",
                        &self.display_url,
                        err,
                    ));
                }
            }
        }

        // Cloud stores with guaranteed range support but no suffix support (Azure):
        // discover the size with a HEAD and read a bounded tail.
        let size = self.head_size()?;
        Ok(Some((size, self.bounded_tail(size, tail_bytes)?)))
    }

    /// Download the object to `writer` in `chunk_bytes` ranged parts, falling back
    /// to an unranged GET when the store ignores `Range` or the object is empty.
    fn download_to_writer(&self, writer: &mut impl Write, chunk_bytes: u64) -> Result<()> {
        if chunk_bytes == 0 {
            bail!("remote download chunk size must be non-zero");
        }
        match self.get_opts_bounded(
            REMOTE_RESPONSE_ATTEMPTS,
            GetOptions {
                range: Some(GetRange::Bounded(0..chunk_bytes)),
                ..GetOptions::default()
            },
        )? {
            Ok(response) => self.download_chunked(response, writer, chunk_bytes),
            Err(err) if remote_range_not_supported(&err) || remote_range_unsatisfiable(&err) => {
                self.download_unranged(writer)
            }
            Err(err) => Err(concise_remote_operation_error(
                "reading remote input from",
                &self.display_url,
                err,
            )),
        }
    }

    /// Stream `first` and the remaining ranges to `writer`, resuming from the last
    /// written byte on body errors. object_store retries ETag'd bodies in-stream first.
    fn download_chunked(
        &self,
        first: object_store::GetResult,
        writer: &mut impl Write,
        chunk_bytes: u64,
    ) -> Result<()> {
        let total = first.meta.size;
        let first_meta = first.meta.clone();
        // If-Match is a strong comparison (RFC 9110 §13.1.1): a weak ETag would
        // 412 every resume, so rely on the size check instead.
        let if_match = first
            .meta
            .e_tag
            .clone()
            .filter(|etag| !etag.starts_with("W/"));
        let mut progress = DownloadProgress::new(total);
        let mut offset = 0u64;
        let mut pending = Some(first);
        let mut attempts_without_progress = 0usize;
        while offset < total {
            let response = match pending.take() {
                Some(response) => Ok(response),
                None => {
                    let end = offset.saturating_add(chunk_bytes).min(total);
                    self.resume_chunk_get(offset..end, &if_match, &first_meta)
                }
            };
            let err = match response {
                Ok(response) => {
                    let (written, result) =
                        self.stream_get_to_writer(response, writer, &mut progress);
                    offset = offset.saturating_add(written);
                    if written > 0 {
                        attempts_without_progress = 0;
                    }
                    match result {
                        Ok(()) if written > 0 => continue,
                        // An empty body with more bytes expected: retry like
                        // a dropped connection.
                        Ok(()) => anyhow::anyhow!(
                            "failed to read remote input {}: download made no progress",
                            self.display_url
                        ),
                        Err(DownloadError::Fatal(err)) => return Err(err),
                        Err(DownloadError::Retryable(err)) => err,
                    }
                }
                Err(DownloadError::Fatal(err)) => return Err(err),
                Err(DownloadError::Retryable(err)) => err,
            };
            attempts_without_progress += 1;
            if attempts_without_progress >= REMOTE_DOWNLOAD_NO_PROGRESS_ATTEMPTS {
                return Err(err.context(format!(
                    "remote download failed after {attempts_without_progress} attempts with no progress"
                )));
            }
            progress.note(&format!(
                "Warning: remote download interrupted at {} / {}, retrying: {}",
                human_bytes(offset),
                human_bytes(total),
                single_line_error(&err)
            ));
        }
        Ok(())
    }

    /// Resume GET at `range`. Fatal: 412, a changed size or last-modified (the
    /// no-ETag guard; a missing header is the epoch on both backends), a wrong
    /// range, or 404/401/403.
    fn resume_chunk_get(
        &self,
        range: std::ops::Range<u64>,
        if_match: &Option<String>,
        first: &object_store::ObjectMeta,
    ) -> std::result::Result<object_store::GetResult, DownloadError> {
        let changed = |what: String| {
            DownloadError::Fatal(anyhow::anyhow!(
                "failed to read {}: remote object changed while downloading ({what})",
                self.display_url
            ))
        };
        // One attempt: the download loop counts and retries head timeouts itself.
        match self.get_opts_bounded(
            1,
            GetOptions {
                range: Some(GetRange::Bounded(range.clone())),
                if_match: if_match.clone(),
                ..GetOptions::default()
            },
        ) {
            Ok(Ok(response)) if response.meta.size != first.size => Err(changed(format!(
                "size {} -> {}",
                first.size, response.meta.size
            ))),
            Ok(Ok(response)) if response.meta.last_modified != first.last_modified => {
                Err(changed(format!(
                    "last modified {} -> {}",
                    first.last_modified.to_rfc3339(),
                    response.meta.last_modified.to_rfc3339()
                )))
            }
            // Both backends already validate Content-Range; this guards the offset accounting.
            Ok(Ok(response)) if response.range.start != range.start => {
                Err(DownloadError::Fatal(anyhow::anyhow!(
                    "failed to read {}: remote server returned range {:?} for requested range {:?}",
                    self.display_url,
                    response.range,
                    range
                )))
            }
            Ok(Ok(response)) => Ok(response),
            Ok(Err(object_store::Error::Precondition { .. })) => {
                Err(DownloadError::Fatal(anyhow::anyhow!(
                    "failed to read {}: remote object changed while downloading",
                    self.display_url
                )))
            }
            Ok(Err(
                err @ (object_store::Error::NotFound { .. }
                | object_store::Error::PermissionDenied { .. }
                | object_store::Error::Unauthenticated { .. }),
            )) => Err(DownloadError::Fatal(concise_remote_operation_error(
                "reading remote input from",
                &self.display_url,
                err,
            ))),
            Ok(Err(err)) => Err(DownloadError::Retryable(concise_remote_operation_error(
                "reading remote input from",
                &self.display_url,
                err,
            ))),
            // The response-head wait expired.
            Err(err) => Err(DownloadError::Retryable(err)),
        }
    }

    fn download_unranged(&self, writer: &mut impl Write) -> Result<()> {
        let response = self
            .get_opts_bounded(REMOTE_RESPONSE_ATTEMPTS, GetOptions::default())?
            .map_err(|err| {
                concise_remote_operation_error("reading remote input from", &self.display_url, err)
            })?;
        let mut progress = DownloadProgress::new(response.meta.size);
        // Without ranges there is no way to resume mid-body; any failure is terminal.
        let (_, result) = self.stream_get_to_writer(response, writer, &mut progress);
        result.map_err(DownloadError::into_error)
    }

    /// Stream one GET response to `writer`, reporting bytes written even on
    /// failure so the caller can resume past partial progress.
    fn stream_get_to_writer(
        &self,
        response: object_store::GetResult,
        writer: &mut impl Write,
        progress: &mut DownloadProgress,
    ) -> (u64, std::result::Result<(), DownloadError>) {
        if let Err(err) =
            validate_identity_content_encoding(&response.attributes, &self.display_url)
        {
            return (0, Err(DownloadError::Fatal(err)));
        }
        let mut written = 0u64;
        let result = self.runtime.block_on(async {
            let mut stream = response.into_stream();
            loop {
                let Ok(next) = tokio::time::timeout(REMOTE_STALL_TIMEOUT, stream.try_next()).await
                else {
                    return Err(DownloadError::Retryable(anyhow::anyhow!(
                        "failed to read remote input {}: download stalled (no data for {}s)",
                        self.display_url,
                        REMOTE_STALL_TIMEOUT.as_secs()
                    )));
                };
                let bytes = match next {
                    Ok(Some(bytes)) => bytes,
                    Ok(None) => break,
                    // object_store errors already print their cause chain, so
                    // flatten rather than letting anyhow repeat it.
                    Err(err) => {
                        return Err(DownloadError::Retryable(concise_remote_operation_error(
                            "reading remote input from",
                            &self.display_url,
                            err,
                        )))
                    }
                };
                if let Err(err) = writer.write_all(bytes.as_ref()) {
                    return Err(DownloadError::Fatal(anyhow::Error::new(err).context(
                        format!("failed to write remote input {}", self.display_url),
                    )));
                }
                let n = bytes.len() as u64;
                written = written.saturating_add(n);
                progress.add(n);
            }
            Ok(())
        });
        (written, result)
    }
}

// `Retryable` failures leave the object re-fetchable from the current offset;
// `Fatal` ones (write errors, a changed or unreadable object) would repeat.
enum DownloadError {
    Retryable(anyhow::Error),
    Fatal(anyhow::Error),
}

impl DownloadError {
    fn into_error(self) -> anyhow::Error {
        match self {
            Self::Retryable(err) | Self::Fatal(err) => err,
        }
    }
}

/// Flatten a (possibly two-line) remote error for use inside a warning line.
fn single_line_error(err: &anyhow::Error) -> String {
    format!("{err:#}")
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join(": ")
}

struct DownloadProgress {
    is_tty: bool,
    total: u64,
    written: u64,
    last_report: Instant,
    // The in-place progress line is on screen without a trailing newline.
    line_open: bool,
}

impl DownloadProgress {
    fn new(total: u64) -> Self {
        Self {
            is_tty: std::io::stderr().is_terminal(),
            total,
            written: 0,
            last_report: Instant::now(),
            line_open: false,
        }
    }

    fn add(&mut self, n: u64) {
        let first = self.written == 0;
        self.written = self.written.saturating_add(n);
        if first || self.last_report.elapsed() >= Duration::from_millis(200) {
            self.report(false);
            self.last_report = Instant::now();
        }
    }

    // Print a standalone line without corrupting the in-place progress line.
    fn note(&mut self, message: &str) {
        let _ = self.write_note(&mut std::io::stderr().lock(), message);
    }

    fn write_note(&mut self, out: &mut impl Write, message: &str) -> std::io::Result<()> {
        if self.line_open {
            writeln!(out)?;
            self.line_open = false;
        }
        writeln!(out, "{message}")?;
        out.flush()
    }

    fn report(&mut self, final_line: bool) {
        if !self.is_tty {
            return;
        }
        let _ = self.write_report(&mut std::io::stderr().lock(), final_line);
    }

    fn write_report(&mut self, out: &mut impl Write, final_line: bool) -> std::io::Result<()> {
        // Pad so a shorter update erases the tail of a longer previous line.
        let message = format!(
            "Downloading {} / {}",
            human_bytes(self.written),
            human_bytes(self.total)
        );
        write!(out, "\r{message:<48}")?;
        if final_line {
            writeln!(out)?;
        }
        self.line_open = !final_line;
        out.flush()
    }
}

impl Drop for DownloadProgress {
    fn drop(&mut self) {
        if self.is_tty && self.written > 0 {
            self.report(true);
        }
    }
}

/// The trailing bytes of a remote object fetched in a single request, used to
/// discover the summary section. `start` is the absolute file offset of `bytes[0]`.
#[derive(Debug)]
struct RemoteTail {
    start: u64,
    bytes: Vec<u8>,
}

pub struct RemoteRangeReader {
    source: ObjectStoreSource,
    kind: RemoteUrlKind,
    size: u64,
    offset: u64,
    // Trailing bytes prefetched at open time, consumed once by summary discovery.
    // Cleared afterwards so it does not linger while the reader services data reads.
    tail: Option<RemoteTail>,
}

impl RemoteRangeReader {
    fn open(path: &Path) -> Result<Option<Self>> {
        let remote_url = RemoteUrl::parse(path)?;
        let kind = remote_url.kind;
        let source = ObjectStoreSource::open_remote(remote_url)?;
        let Some((size, tail)) = source.read_summary_tail(kind, REMOTE_SUMMARY_TAIL_BYTES)? else {
            return Ok(None);
        };
        Ok(Some(Self {
            source,
            kind,
            size,
            offset: 0,
            tail: Some(tail),
        }))
    }

    #[cfg(test)]
    fn new_for_test(store: Arc<dyn ObjectStore>, path: ObjectStorePath, size: u64) -> Result<Self> {
        Ok(Self {
            source: ObjectStoreSource {
                runtime: object_store_runtime()?,
                store,
                path,
                display_url: "memory:///test".to_string(),
                response_timeout: REMOTE_RESPONSE_TIMEOUT,
            },
            kind: RemoteUrlKind::CloudSuffix,
            size,
            offset: 0,
            tail: None,
        })
    }

    // Construct a reader whose prefetched tail covers only `[tail_start, size)`,
    // forcing summary discovery to back-fill the missing prefix via a range read.
    #[cfg(test)]
    fn new_for_test_with_tail(
        store: Arc<dyn ObjectStore>,
        path: ObjectStorePath,
        bytes: Vec<u8>,
        tail_start: u64,
    ) -> Result<Self> {
        let size = bytes.len() as u64;
        let tail = RemoteTail {
            start: tail_start,
            bytes: bytes[tail_start as usize..].to_vec(),
        };
        Ok(Self {
            source: ObjectStoreSource {
                runtime: object_store_runtime()?,
                store,
                path,
                display_url: "memory:///test".to_string(),
                response_timeout: REMOTE_RESPONSE_TIMEOUT,
            },
            kind: RemoteUrlKind::CloudSuffix,
            size,
            offset: 0,
            tail: Some(tail),
        })
    }

    pub(crate) fn read_range(&self, offset: u64, length: usize) -> Result<Vec<u8>> {
        if length == 0 || offset >= self.size {
            return Ok(Vec::new());
        }
        let end = offset
            .checked_add(length as u64)
            .map(|end| end.min(self.size))
            .ok_or_else(|| anyhow::anyhow!("remote range overflow"))?;
        self.source.get_range(offset..end)
    }

    pub(crate) fn size(&self) -> u64 {
        self.size
    }

    pub(crate) fn display_url(&self) -> &str {
        &self.source.display_url
    }
}

impl std::io::Read for RemoteRangeReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self.read_range(self.offset, buf.len()) {
            Ok(bytes) => {
                let n = bytes.len();
                buf[..n].copy_from_slice(&bytes);
                self.offset = self.offset.saturating_add(n as u64);
                Ok(n)
            }
            Err(err) => Err(std::io::Error::other(err)),
        }
    }
}

impl std::io::Seek for RemoteRangeReader {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let target = match pos {
            SeekFrom::Start(offset) => offset as i128,
            SeekFrom::End(offset) => self.size as i128 + offset as i128,
            SeekFrom::Current(offset) => self.offset as i128 + offset as i128,
        };
        if target < 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "remote seek out of bounds",
            ));
        }
        self.offset = target as u64;
        Ok(self.offset)
    }
}

// object_store config keys accept unprefixed aliases (for example `endpoint`,
// `region`, and `token`), so forwarding the whole environment would let unrelated
// shell variables silently reconfigure the store. Restrict to the prefixes the
// object_store builders themselves read in their `from_env` constructors.
const OBJECT_STORE_ENV_PREFIXES: [&str; 3] = ["AWS_", "GOOGLE_", "AZURE_"];

fn object_store_options_from_env_vars(
    vars: impl IntoIterator<Item = (std::ffi::OsString, std::ffi::OsString)>,
) -> Vec<(String, String)> {
    vars.into_iter()
        .filter_map(|(key, value)| Some((key.into_string().ok()?, value.into_string().ok()?)))
        .filter(|(key, _)| {
            OBJECT_STORE_ENV_PREFIXES
                .iter()
                .any(|prefix| key.starts_with(prefix))
        })
        .collect()
}

// object_store maps a `200 OK` response to a ranged request onto `NotSupported`,
// which is how we detect a server that ignores `Range` headers. This couples us to
// object_store's mapping, so the no-range fallback tests guard against version drift.
fn remote_range_not_supported(err: &object_store::Error) -> bool {
    matches!(err, object_store::Error::NotSupported { .. })
}

// Empty objects reject `bytes=0..N` with HTTP 416. Match the parsed status,
// not a "416" substring — error text can contain request ids with those digits.
fn remote_range_unsatisfiable(err: &object_store::Error) -> bool {
    object_store_error_status(err).is_some_and(|status| status.starts_with("416"))
}

fn concise_remote_stat_error(display_url: &str, err: object_store::Error) -> anyhow::Error {
    if let Some(status) = object_store_error_status(&err) {
        return remote_status_read_error(display_url, &status);
    }
    anyhow::anyhow!("failed to read {display_url}\nFailed to stat remote input: {err}")
}

fn concise_remote_operation_error(
    operation: &str,
    display_url: &str,
    err: object_store::Error,
) -> anyhow::Error {
    if let Some(status) = object_store_error_status(&err) {
        return remote_status_read_error(display_url, &status);
    }
    anyhow::anyhow!("failed to read {display_url}\nFailed while {operation}: {err}")
}

fn remote_status_read_error(display_url: &str, status: &str) -> anyhow::Error {
    anyhow::anyhow!("failed to read {display_url}\nRemote server returned {status}")
}

fn remote_read_error(path: &Path, err: anyhow::Error) -> anyhow::Error {
    let message = format!("{err:#}");
    if message.starts_with("failed to read ") {
        return anyhow::anyhow!("{message}");
    }
    anyhow::anyhow!(
        "failed to read {}\n{}",
        redacted_display(path),
        remote_read_error_detail(&message)
    )
}

fn remote_read_error_detail(message: &str) -> String {
    if message.contains("MCAP file ended in the middle of a record") {
        return recoverable_mcap_error().to_string();
    }
    capitalize_first(message)
}

fn capitalize_first(message: &str) -> String {
    let mut chars = message.chars();
    let Some(first) = chars.next() else {
        return String::new();
    };
    first.to_uppercase().chain(chars).collect()
}

fn object_store_error_status(err: &object_store::Error) -> Option<String> {
    let status = match err {
        object_store::Error::NotFound { .. } => "404 Not Found",
        object_store::Error::PermissionDenied { .. } => "403 Forbidden",
        object_store::Error::Unauthenticated { .. } => "401 Unauthorized",
        object_store::Error::NotModified { .. } => "304 Not Modified",
        object_store::Error::Precondition { .. } => "412 Precondition Failed",
        object_store::Error::AlreadyExists { .. } => "409 Conflict",
        _ => return status_from_object_store_message(&err.to_string()),
    };
    Some(status.to_string())
}

fn status_from_object_store_message(message: &str) -> Option<String> {
    // object_store does not expose every HTTP status as a typed variant. Keep the
    // concise formatting best-effort and let the status-message tests catch
    // upstream Display wording changes.
    let (_, status) = message.split_once("Server returned non-2xx status code: ")?;
    let status = status.trim().trim_end_matches(':').trim();
    let status = status
        .split_once(':')
        .map_or(status, |(status, _)| status.trim());
    (!status.is_empty()).then(|| status.to_string())
}

pub(crate) fn open_remote_range_reader(path: &Path) -> Result<Option<RemoteRangeReader>> {
    if is_remote_url(path) {
        return RemoteRangeReader::open(path);
    }
    Ok(None)
}

pub(crate) fn redacted_display(path: &Path) -> String {
    path.to_str()
        .map(redact_url)
        .unwrap_or_else(|| path.display().to_string())
}

/// Like `object_store::parse_url_opts`, which cannot set a retry budget, with
/// `REMOTE_STORE_*` applied so retries end inside the head timeout.
fn build_object_store(
    url: &Url,
    options: Vec<(String, String)>,
) -> object_store::Result<(Box<dyn ObjectStore>, ObjectStorePath)> {
    let (scheme, object_path) = ObjectStoreScheme::parse(url)?;
    let retry = RetryConfig {
        backoff: BackoffConfig {
            max_backoff: REMOTE_STORE_MAX_BACKOFF,
            ..BackoffConfig::default()
        },
        max_retries: REMOTE_STORE_RETRIES,
        retry_timeout: REMOTE_STORE_RETRY_TIMEOUT,
    };
    // Mirrors object_store's private `builder_opts!`: unknown keys are skipped
    // because the option list is the process environment.
    macro_rules! build {
        ($builder:ty, $url:expr) => {{
            let builder = options.into_iter().fold(
                <$builder>::new()
                    .with_url($url.to_string())
                    .with_retry(retry),
                |builder, (key, value)| match key.to_ascii_lowercase().parse() {
                    Ok(key) => builder.with_config(key, value),
                    Err(_) => builder,
                },
            );
            Box::new(builder.build()?) as Box<dyn ObjectStore>
        }};
    }
    let store = match scheme {
        ObjectStoreScheme::AmazonS3 => build!(object_store::aws::AmazonS3Builder, url),
        ObjectStoreScheme::GoogleCloudStorage => {
            build!(object_store::gcp::GoogleCloudStorageBuilder, url)
        }
        ObjectStoreScheme::MicrosoftAzure => {
            build!(object_store::azure::MicrosoftAzureBuilder, url)
        }
        ObjectStoreScheme::Http => {
            build!(
                object_store::http::HttpBuilder,
                &url[..url::Position::BeforePath]
            )
        }
        scheme => {
            return Err(object_store::Error::Generic {
                store: "parse_url",
                source: format!("unsupported remote scheme {scheme:?}").into(),
            })
        }
    };
    Ok((store, object_path))
}

pub(crate) fn read_remote_input_to_writer(path: &Path, writer: &mut impl Write) -> Result<()> {
    let source = ObjectStoreSource::open_for_download(path)?;
    eprintln!("Warning: reading entire remote file {}", source.display_url);
    source.download_to_writer(writer, REMOTE_DOWNLOAD_CHUNK_BYTES)
}

fn object_store_runtime() -> Result<Arc<tokio::runtime::Runtime>> {
    static RUNTIME: std::sync::OnceLock<Arc<tokio::runtime::Runtime>> = std::sync::OnceLock::new();
    if let Some(runtime) = RUNTIME.get() {
        return Ok(runtime.clone());
    }
    let runtime = Arc::new(
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("failed to create object store runtime")?,
    );
    Ok(RUNTIME.get_or_init(|| runtime).clone())
}

fn redact_url(url: &str) -> String {
    let without_fragment_or_query = remote_url_without_fragment_or_query(url);
    let Some((scheme, rest)) = without_fragment_or_query.split_once("://") else {
        return without_fragment_or_query.to_string();
    };
    let authority_end = rest.find('/').unwrap_or(rest.len());
    let (authority, path) = rest.split_at(authority_end);
    let authority = authority
        .rsplit_once('@')
        .map(|(_, suffix)| suffix)
        .unwrap_or(authority);
    format!("{scheme}://{authority}{path}")
}

fn remote_url_without_fragment_or_query(url: &str) -> &str {
    url.split('#')
        .next()
        .unwrap_or(url)
        .split('?')
        .next()
        .unwrap_or(url)
}

fn validate_identity_content_encoding(
    attributes: &object_store::Attributes,
    display_url: &str,
) -> Result<()> {
    let Some(value) = attributes.get(&Attribute::ContentEncoding) else {
        return Ok(());
    };
    let value = value.as_ref();
    if value.eq_ignore_ascii_case("identity") {
        return Ok(());
    }
    bail!(
        "remote server returned Content-Encoding: {value} for {display_url}; MCAP remote reads require identity encoding"
    );
}

pub(crate) fn remote_scan_opt_in_suffix() -> &'static str {
    "pass --allow-remote-scan to continue"
}

pub(crate) fn require_remote_indexed_read_budget(
    total_bytes: u64,
    options: SourceOptions,
    description: &str,
) -> Result<()> {
    if options.allow_remote_scan || total_bytes <= MAX_REMOTE_INDEXED_BYTES_WITHOUT_SCAN {
        return Ok(());
    }
    bail!(
        "{description} would read {} (exceeds {} cap without --allow-remote-scan); {}",
        human_bytes(total_bytes),
        human_bytes(MAX_REMOTE_INDEXED_BYTES_WITHOUT_SCAN),
        remote_scan_opt_in_suffix()
    );
}

pub(crate) fn require_remote_scan_allowed(path: &Path, options: SourceOptions) -> Result<()> {
    if options.allow_remote_scan {
        return Ok(());
    }
    let display = redacted_display(path);
    bail!(
        "remote input {display} requires opt-in because this command must download or scan remote data; {}",
        remote_scan_opt_in_suffix()
    );
}

/// Errors when a remote [`ByteSource`] would need a full linear scan without `--allow-remote-scan`.
pub(crate) fn require_remote_scan_for_linear(
    source: &dyn crate::byte_source::ByteSource,
    options: SourceOptions,
) -> Result<()> {
    if source.is_remote() && !options.allow_remote_scan {
        bail!(
            "{}: remote file requires a full scan; {}",
            source.display_name(),
            remote_scan_opt_in_suffix()
        );
    }
    Ok(())
}

fn read_summary_bytes_from_remote(
    reader: &mut RemoteRangeReader,
    options: SourceOptions,
) -> Result<Option<Vec<u8>>> {
    let file_size = reader.size();
    let tail_len = parse::FOOTER_RECORD_AND_END_MAGIC_LEN as u64;
    if file_size < tail_len + mcap::MAGIC.len() as u64 {
        return Err(classify_remote_summary_error(
            reader,
            mcap::McapError::UnexpectedEof.into(),
        ));
    }

    // The tail was prefetched at open time. Consume it here; subsequent data reads
    // do not need it. It always covers at least the footer + trailing magic.
    let tail = reader
        .tail
        .take()
        .ok_or_else(|| anyhow::anyhow!("remote reader is missing its prefetched tail"))?;
    if (tail.bytes.len() as u64) < tail_len || tail.start > file_size - tail_len {
        return Err(classify_remote_summary_error(
            reader,
            mcap::McapError::UnexpectedEof.into(),
        ));
    }
    // The `footer_bytes` / `summary_end_in_tail` slicing below assumes the tail ends
    // exactly at EOF; both `read_summary_tail` paths uphold this by construction.
    debug_assert_eq!(
        tail.start + tail.bytes.len() as u64,
        file_size,
        "prefetched remote tail must end at end of file"
    );

    let footer_start = file_size - tail_len;
    let footer_bytes = &tail.bytes[tail.bytes.len() - parse::FOOTER_RECORD_AND_END_MAGIC_LEN..];
    if footer_bytes[0] != records::op::FOOTER {
        return Err(classify_remote_summary_error(
            reader,
            mcap::McapError::BadFooter.into(),
        ));
    }
    let record_len =
        u64::from_le_bytes(footer_bytes[1..9].try_into().expect("footer length slice"));
    if record_len != 20 {
        return Err(classify_remote_summary_error(
            reader,
            mcap::McapError::BadFooter.into(),
        ));
    }
    if &footer_bytes[parse::FOOTER_RECORD_AND_END_MAGIC_LEN - mcap::MAGIC.len()..] != mcap::MAGIC {
        return Err(classify_remote_summary_error(
            reader,
            mcap::McapError::BadMagic.into(),
        ));
    }

    let mut cursor = std::io::Cursor::new(
        &footer_bytes[9..parse::FOOTER_RECORD_AND_END_MAGIC_LEN - mcap::MAGIC.len()],
    );
    let footer = records::Footer::read_le(&mut cursor)
        .map_err(|err| classify_remote_summary_error(reader, err.into()))?;
    if footer.summary_start == 0 {
        return Ok(None);
    }
    if footer.summary_start > footer_start {
        return Err(classify_remote_summary_error(
            reader,
            mcap::McapError::UnexpectedEof.into(),
        ));
    }
    let summary_len = usize::try_from(footer_start - footer.summary_start)
        .context("remote summary section is too large to read on this platform")?;
    require_remote_indexed_read_budget(summary_len as u64, options, "remote summary section")?;

    // `[summary_start, footer_start)` is the summary + summary offset region. The
    // portion at or after `tail.start` is already in the prefetched tail; only the
    // prefix before the tail (the uncommon large-summary case) needs another request.
    let summary_end_in_tail = (footer_start - tail.start) as usize;
    let summary_bytes = if footer.summary_start >= tail.start {
        let summary_start_in_tail = (footer.summary_start - tail.start) as usize;
        tail.bytes[summary_start_in_tail..summary_end_in_tail].to_vec()
    } else {
        let prefix_len = (tail.start - footer.summary_start) as usize;
        let mut summary_bytes = reader.read_range(footer.summary_start, prefix_len)?;
        if summary_bytes.len() != prefix_len {
            return Err(classify_remote_summary_error(
                reader,
                mcap::McapError::UnexpectedEof.into(),
            ));
        }
        summary_bytes.extend_from_slice(&tail.bytes[..summary_end_in_tail]);
        summary_bytes
    };
    if summary_bytes.len() != summary_len {
        return Err(classify_remote_summary_error(
            reader,
            mcap::McapError::UnexpectedEof.into(),
        ));
    }
    Ok(Some(summary_bytes))
}

fn classify_remote_summary_error(reader: &RemoteRangeReader, err: anyhow::Error) -> anyhow::Error {
    if reader.kind == RemoteUrlKind::Http {
        if let Err(head_err) = reader.source.stat() {
            return head_err;
        }
    }
    if let Some(mcap_err) = err.downcast_ref::<mcap::McapError>() {
        match mcap_err {
            mcap::McapError::BadFooter
            | mcap::McapError::BadMagic
            | mcap::McapError::UnexpectedEof => {
                if let Some(err) = remote_mcap_tail_error(reader, mcap_err) {
                    return err;
                }
            }
            _ => {}
        }
    }
    err
}

fn remote_mcap_tail_error(
    reader: &RemoteRangeReader,
    mcap_err: &mcap::McapError,
) -> Option<anyhow::Error> {
    let has_start_magic = remote_range_matches_magic(reader, 0)?;
    if !has_start_magic {
        return Some(anyhow::anyhow!("Input does not appear to be an MCAP file"));
    }

    let trailing_magic_offset = reader.size().checked_sub(mcap::MAGIC.len() as u64)?;
    let has_trailing_magic = remote_range_matches_magic(reader, trailing_magic_offset)?;
    if matches!(mcap_err, mcap::McapError::BadFooter) && has_trailing_magic {
        return Some(anyhow::anyhow!("MCAP file is missing its footer record"));
    }

    Some(anyhow::anyhow!(recoverable_mcap_error()))
}

fn remote_range_matches_magic(reader: &RemoteRangeReader, offset: u64) -> Option<bool> {
    Some(reader.read_range(offset, mcap::MAGIC.len()).ok()? == mcap::MAGIC)
}

fn recoverable_mcap_error() -> &'static str {
    "MCAP file appears truncated or incomplete (try running `mcap --allow-remote-scan recover`)"
}

fn read_header_from_seekable(
    reader: &mut (impl std::io::Read + std::io::Seek),
) -> Result<Option<records::Header>> {
    reader.seek(SeekFrom::Start(0))?;
    let mut linear_reader = mcap::sans_io::LinearReader::new();
    while let Some(event) = linear_reader.next_event() {
        match event? {
            mcap::sans_io::LinearReadEvent::ReadRequest(n) => {
                let read = reader.read(linear_reader.insert(n))?;
                linear_reader.notify_read(read);
                if read == 0 {
                    return Ok(None);
                }
            }
            mcap::sans_io::LinearReadEvent::Record { opcode, data } => {
                if let Record::Header(header) = mcap::parse_record(opcode, data)?.into_owned() {
                    return Ok(Some(header));
                }
                return Ok(None);
            }
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::net::{TcpListener, TcpStream};
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;

    use super::materialize_input;
    use crate::render::human_bytes;
    use mcap::records;
    use object_store::ObjectStoreExt;

    #[test]
    fn distinct_local_input_output_rejects_same_file() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let input = dir.path().join("input.mcap");
        std::fs::write(&input, b"placeholder").expect("write input");

        let err = super::ensure_distinct_local_input_output(&input, &input)
            .expect_err("same input/output should fail");
        assert!(err.to_string().contains("input and output paths"));
    }

    #[test]
    fn distinct_local_input_output_rejects_existing_output_alias() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let input = dir.path().join("input.mcap");
        let output = dir.path().join("output-link.mcap");
        std::fs::write(&input, b"placeholder").expect("write input");
        #[cfg(unix)]
        std::os::unix::fs::symlink(&input, &output).expect("symlink");
        #[cfg(windows)]
        std::os::windows::fs::symlink_file(&input, &output).expect("symlink");

        let err = super::ensure_distinct_local_input_output(&input, &output)
            .expect_err("aliased output should fail");
        assert!(err.to_string().contains("input and output paths"));
    }

    #[cfg(unix)]
    #[test]
    fn distinct_local_input_output_rejects_existing_output_hard_link() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let input = dir.path().join("input.mcap");
        let output = dir.path().join("output-hard-link.mcap");
        std::fs::write(&input, b"placeholder").expect("write input");
        std::fs::hard_link(&input, &output).expect("hard link");

        let err = super::ensure_distinct_local_input_output(&input, &output)
            .expect_err("hard-linked output should fail");
        assert!(err.to_string().contains("input and output paths"));
    }

    #[test]
    fn distinct_local_input_output_allows_missing_output() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let input = dir.path().join("input.mcap");
        let output = dir.path().join("new-output.mcap");
        std::fs::write(&input, b"placeholder").expect("write input");

        super::ensure_distinct_local_input_output(&input, &output)
            .expect("missing output should be allowed");
    }

    #[test]
    fn distinct_local_input_output_allows_single_component_missing_output() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let input = dir.path().join("input.mcap");
        let output_name = format!(
            "mcap-cli-missing-output-{}-{}.mcap",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time should be after epoch")
                .as_nanos()
        );
        std::fs::write(&input, b"placeholder").expect("write input");
        let _ = std::fs::remove_file(&output_name);

        super::ensure_distinct_local_input_output(&input, Path::new(&output_name))
            .expect("single-component missing output should resolve through current directory");
    }

    #[test]
    fn distinct_local_input_output_allows_missing_input() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let input = dir.path().join("missing.mcap");
        let output = dir.path().join("output.mcap");

        super::ensure_distinct_local_input_output(&input, &output)
            .expect("missing input should be left to command open errors");
    }

    fn serve_http(body: &'static [u8], supports_ranges: bool) -> String {
        serve_http_with_headers(body, supports_ranges, &[])
    }

    fn object_store_memory_reader(bytes: Vec<u8>) -> super::RemoteRangeReader {
        let store = Arc::new(object_store::memory::InMemory::new());
        let path = object_store::path::Path::from("demo.mcap");
        let runtime = super::object_store_runtime().expect("runtime");
        runtime
            .block_on(store.put(&path, bytes.clone().into()))
            .expect("put memory object");
        super::RemoteRangeReader::new_for_test(store, path, bytes.len() as u64)
            .expect("memory range reader")
    }

    fn object_store_memory_reader_with_tail(
        bytes: Vec<u8>,
        tail_start: u64,
    ) -> super::RemoteRangeReader {
        let store = Arc::new(object_store::memory::InMemory::new());
        let path = object_store::path::Path::from("demo.mcap");
        let runtime = super::object_store_runtime().expect("runtime");
        runtime
            .block_on(store.put(&path, bytes.clone().into()))
            .expect("put memory object");
        super::RemoteRangeReader::new_for_test_with_tail(store, path, bytes, tail_start)
            .expect("memory range reader")
    }

    fn summary_mcap_with_channel() -> (Vec<u8>, u16) {
        let mut buffer = Vec::new();
        let channel_id = {
            let mut writer = mcap::Writer::new(std::io::Cursor::new(&mut buffer)).expect("writer");
            let schema_id = writer
                .add_schema("demo_schema", "jsonschema", br#"{"type":"object"}"#)
                .expect("schema");
            let channel_id = writer
                .add_channel(schema_id, "/demo", "json", &BTreeMap::new())
                .expect("channel");
            writer.finish().expect("finish writer");
            channel_id
        };
        (buffer, channel_id)
    }

    // What the test server does with the request at a given 0-based index.
    #[derive(Clone, Copy)]
    enum ScriptedResponse {
        // Serve the request normally from the configured body.
        Normal,
        // Respond with this status line and an empty body.
        Status(&'static str),
        // Serve the request from a different object.
        Body(&'static [u8]),
        // Serve the request from a different object with this `Last-Modified`.
        Modified(&'static [u8], &'static str),
        // Accept the request and never answer it, like a hung connection.
        Hang,
    }

    type Script = Arc<dyn Fn(usize) -> ScriptedResponse + Send + Sync>;

    // The one HTTP test server behind every `serve_http*` helper. It answers
    // `Connection: close`, so the returned counter counts requests.
    struct TestHttpServer {
        body: &'static [u8],
        supports_ranges: bool,
        extra_headers: &'static [(&'static str, &'static str)],
        // Answer HEAD with 403, like servers that only allow GET.
        reject_head: bool,
        // Emit `Content-Range: bytes <start>-<end>/*` (unknown total) instead of a numeric total.
        unknown_range_total: bool,
        // Reject suffix ranges (`bytes=-N`) with 416 while honoring bounded ones.
        reject_suffix: bool,
        // Sent on every response; `If-Match` is then enforced (weak ETags never match).
        etag: Option<&'static str>,
        last_modified: Option<&'static str>,
        // 428 on any resumed range without `If-Match`, to prove the client pinned
        // the ETag. Unrealistic: object_store's own resume sends none.
        require_if_match: bool,
        script: Option<Script>,
        // Close the first `truncated_bodies` bodies after `truncated_len(len)` bytes.
        truncated_bodies: usize,
        truncated_len: fn(usize) -> usize,
        // Status line and body for requests not served as a range.
        unranged_status: Option<(String, &'static [u8])>,
    }

    impl TestHttpServer {
        fn new(body: &'static [u8]) -> Self {
            Self {
                body,
                supports_ranges: true,
                extra_headers: &[],
                reject_head: false,
                unknown_range_total: false,
                reject_suffix: false,
                etag: None,
                last_modified: None,
                require_if_match: false,
                script: None,
                truncated_bodies: 0,
                truncated_len: |len| len,
                unranged_status: None,
            }
        }

        fn serve(self) -> (String, Arc<AtomicUsize>) {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
            let addr = listener.local_addr().expect("test server addr");
            let request_count = Arc::new(AtomicUsize::new(0));
            let server_request_count = request_count.clone();
            thread::spawn(move || {
                let mut remaining_truncations = self.truncated_bodies;
                // Hung connections are kept open here so the client sees a stall, not a reset.
                let mut hung = Vec::new();
                for stream in listener.incoming().take(64) {
                    let mut stream = stream.expect("accept test connection");
                    let index = server_request_count.fetch_add(1, Ordering::SeqCst);
                    let mut request = [0u8; 4096];
                    let read = stream.read(&mut request).expect("read request");
                    let request = String::from_utf8_lossy(&request[..read]);
                    let is_head = request.starts_with("HEAD ");
                    let header = |name: &str| {
                        request.lines().find_map(|line| {
                            let (key, value) = line.split_once(':')?;
                            key.eq_ignore_ascii_case(name).then(|| value.trim())
                        })
                    };
                    let write_status = |stream: &mut TcpStream, status: &str, body: &[u8]| {
                        stream
                            .write_all(
                                format!(
                                    "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                    body.len()
                                )
                                .as_bytes(),
                            )
                            .expect("write status");
                        if !is_head {
                            stream.write_all(body).expect("write status body");
                        }
                    };
                    let write_416 = |stream: &mut TcpStream, total: usize| {
                        stream
                            .write_all(
                                format!(
                                    "HTTP/1.1 416 Range Not Satisfiable\r\nContent-Range: bytes */{total}\r\nConnection: close\r\n\r\n"
                                )
                                .as_bytes(),
                            )
                            .expect("write 416");
                    };

                    if self.reject_head && is_head {
                        write_status(&mut stream, "403 Forbidden", b"");
                        continue;
                    }
                    let scripted = self
                        .script
                        .as_ref()
                        .map_or(ScriptedResponse::Normal, |script| script(index));
                    let (body, last_modified) = match scripted {
                        ScriptedResponse::Normal => (self.body, self.last_modified),
                        ScriptedResponse::Body(other) => (other, self.last_modified),
                        ScriptedResponse::Modified(other, modified) => (other, Some(modified)),
                        ScriptedResponse::Status(status) => {
                            write_status(&mut stream, status, b"");
                            continue;
                        }
                        ScriptedResponse::Hang => {
                            hung.push(stream);
                            continue;
                        }
                    };
                    let range_spec = header("Range").and_then(|spec| spec.strip_prefix("bytes="));
                    // Resolve `S-E` (bounded), `-N` (suffix), and `S-` (open ended)
                    // to an inclusive (start, end) over the body.
                    let requested_range = range_spec
                        .and_then(|spec| spec.split_once('-'))
                        .and_then(|(start, end)| {
                            let len = body.len();
                            match (start.trim(), end.trim()) {
                                ("", suffix) => {
                                    let n = suffix.parse::<usize>().ok()?;
                                    Some((len.saturating_sub(n), len.saturating_sub(1)))
                                }
                                (start, "") => {
                                    Some((start.parse::<usize>().ok()?, len.saturating_sub(1)))
                                }
                                (start, end) => {
                                    Some((start.parse::<usize>().ok()?, end.parse::<usize>().ok()?))
                                }
                            }
                        });
                    if let Some(etag) = self.etag {
                        match header("If-Match") {
                            Some(_) if etag.starts_with("W/") => {
                                write_status(&mut stream, "412 Precondition Failed", b"");
                                continue;
                            }
                            Some(if_match) if if_match != etag => {
                                write_status(&mut stream, "412 Precondition Failed", b"");
                                continue;
                            }
                            None if self.require_if_match
                                && requested_range.is_some_and(|(start, _)| start > 0) =>
                            {
                                write_status(&mut stream, "428 Precondition Required", b"");
                                continue;
                            }
                            _ => {}
                        }
                    }
                    if self.reject_suffix
                        && range_spec.is_some_and(|spec| spec.trim_start().starts_with('-'))
                    {
                        write_416(&mut stream, body.len());
                        continue;
                    }
                    let headers = self
                        .extra_headers
                        .iter()
                        .map(|(name, value)| format!("{name}: {value}\r\n"))
                        .chain(self.etag.map(|etag| format!("ETag: {etag}\r\n")))
                        .chain(last_modified.map(|value| format!("Last-Modified: {value}\r\n")))
                        .collect::<String>();
                    let content = match (self.supports_ranges, requested_range) {
                        (true, Some((start, end))) => {
                            if start >= body.len() {
                                write_416(&mut stream, body.len());
                                continue;
                            }
                            let end = end.min(body.len().saturating_sub(1));
                            let start = start.min(end);
                            let content = &body[start..=end];
                            let total = if self.unknown_range_total {
                                "*".to_string()
                            } else {
                                body.len().to_string()
                            };
                            let response = format!(
                                "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{end}/{total}\r\nAccept-Ranges: bytes\r\n{headers}Connection: close\r\n\r\n",
                                content.len(),
                            );
                            stream
                                .write_all(response.as_bytes())
                                .expect("write headers");
                            content
                        }
                        _ => {
                            if let Some((status, status_body)) = &self.unranged_status {
                                write_status(&mut stream, status, status_body);
                                continue;
                            }
                            let accept_ranges = if self.supports_ranges {
                                "bytes"
                            } else {
                                "none"
                            };
                            let response = format!(
                                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: {accept_ranges}\r\n{headers}Connection: close\r\n\r\n",
                                body.len()
                            );
                            stream
                                .write_all(response.as_bytes())
                                .expect("write headers");
                            body
                        }
                    };
                    if is_head {
                        continue;
                    }
                    if remaining_truncations > 0 {
                        remaining_truncations -= 1;
                        // Fewer bytes than Content-Length promised: the client
                        // sees a body error.
                        stream
                            .write_all(&content[..(self.truncated_len)(content.len())])
                            .expect("write truncated body");
                        continue;
                    }
                    stream.write_all(content).expect("write body");
                }
            });
            (format!("http://{addr}/demo.mcap"), request_count)
        }
    }

    fn serve_http_with_headers(
        body: &'static [u8],
        supports_ranges: bool,
        extra_headers: &'static [(&'static str, &'static str)],
    ) -> String {
        TestHttpServer {
            supports_ranges,
            extra_headers,
            ..TestHttpServer::new(body)
        }
        .serve()
        .0
    }

    // Like `serve_http` but also returns the request counter.
    fn serve_http_counting(
        body: &'static [u8],
        supports_ranges: bool,
    ) -> (String, Arc<AtomicUsize>) {
        TestHttpServer {
            supports_ranges,
            ..TestHttpServer::new(body)
        }
        .serve()
    }

    // A server that honors bounded ranges (`bytes=S-E`) but rejects suffix ranges
    // (`bytes=-N`) with `416`, like HTTP servers/proxies that omit the suffix form.
    fn serve_http_bounded_only(body: &'static [u8]) -> (String, Arc<AtomicUsize>) {
        TestHttpServer {
            reject_suffix: true,
            ..TestHttpServer::new(body)
        }
        .serve()
    }

    // Ranged requests get `range_body`; everything else gets `status`.
    fn serve_http_status_with_range_body(
        range_body: &'static [u8],
        status_code: u16,
        reason: &'static str,
        status_body: &'static [u8],
    ) -> String {
        TestHttpServer {
            unranged_status: Some((format!("{status_code} {reason}"), status_body)),
            ..TestHttpServer::new(range_body)
        }
        .serve()
        .0
    }

    // Answer every request with `status_code`.
    fn serve_http_status(status_code: u16, reason: &'static str, body: &'static [u8]) -> String {
        TestHttpServer {
            supports_ranges: false,
            unranged_status: Some((format!("{status_code} {reason}"), body)),
            ..TestHttpServer::new(b"")
        }
        .serve()
        .0
    }

    // Truncates the first `truncated_bodies` bodies to `truncated_len(len)` bytes.
    // An `etag` switches on object_store's own in-stream resume.
    fn serve_http_truncating_bodies(
        body: &'static [u8],
        etag: Option<&'static str>,
        truncated_bodies: usize,
        truncated_len: fn(usize) -> usize,
    ) -> (String, Arc<AtomicUsize>) {
        TestHttpServer {
            etag,
            truncated_bodies,
            truncated_len,
            ..TestHttpServer::new(body)
        }
        .serve()
    }

    // A range-supporting server that responds to the 0-based `failing_index`th
    // request with `status` and serves every other request normally.
    fn serve_http_failing_request(
        body: &'static [u8],
        failing_index: usize,
        status: &'static str,
    ) -> (String, Arc<AtomicUsize>) {
        serve_http_scripted(body, None, move |index| {
            if index == failing_index {
                ScriptedResponse::Status(status)
            } else {
                ScriptedResponse::Normal
            }
        })
    }

    // A range-supporting server whose response to each request is chosen by
    // `script`; see `TestHttpServer::etag` for how an ETag is enforced.
    fn serve_http_scripted(
        body: &'static [u8],
        etag: Option<&'static str>,
        script: impl Fn(usize) -> ScriptedResponse + Send + Sync + 'static,
    ) -> (String, Arc<AtomicUsize>) {
        TestHttpServer {
            etag,
            script: Some(Arc::new(script)),
            ..TestHttpServer::new(body)
        }
        .serve()
    }

    #[test]
    fn remote_errors_redact_query_strings() {
        let url = "http://127.0.0.1:1/demo.mcap?X-Amz-Signature=secret-token";
        let err = materialize_input(Path::new(url), super::SourceOptions::default())
            .err()
            .expect("remote scan rejection should report redacted URL");
        assert!(!err.to_string().contains("secret-token"));
        assert!(!err.to_string().contains("X-Amz-Signature"));
    }

    #[test]
    fn remote_errors_redact_userinfo() {
        let url = "http://AKIA:secret@127.0.0.1:1/demo.mcap";
        let err = materialize_input(Path::new(url), super::SourceOptions::default())
            .err()
            .expect("remote scan rejection should report redacted URL");
        assert!(!err.to_string().contains("AKIA"));
        assert!(!err.to_string().contains("secret"));
        assert!(err.to_string().contains("http://127.0.0.1:1/demo.mcap"));
    }

    #[test]
    fn remote_http_input_requires_remote_scan_opt_in() {
        let url = serve_http(b"hello remote", true);
        let err = materialize_input(Path::new(&url), super::SourceOptions::default())
            .err()
            .expect("remote full read should require opt-in");
        assert!(err.to_string().contains("--allow-remote-scan"));
    }

    #[test]
    fn remote_object_store_input_requires_remote_scan_opt_in_before_network() {
        let err = materialize_input(
            Path::new("s3://bucket/demo.mcap?X-Amz-Signature=secret-token"),
            super::SourceOptions::default(),
        )
        .err()
        .expect("cloud remote full read should require opt-in");
        assert!(err.to_string().contains("--allow-remote-scan"));
        assert!(!err.to_string().contains("secret-token"));
        assert!(!err.to_string().contains("X-Amz-Signature"));
    }

    #[test]
    fn remote_http_input_reads_entire_file() {
        let url = serve_http(b"hello remote", true);
        let input = materialize_input(Path::new(&url), super::SourceOptions::new(true))
            .expect("remote read");
        assert_eq!(
            std::fs::read(input.path()).expect("read materialized"),
            b"hello remote"
        );
    }

    #[test]
    fn remote_http_download_uses_ranged_chunks() {
        let body: &'static [u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
        let (url, requests) = serve_http_counting(body, true);
        let mut out = Vec::new();
        let source = super::ObjectStoreSource::open_for_download(Path::new(&url))
            .expect("open download source");
        source
            .download_to_writer(&mut out, 8)
            .expect("chunked download");
        assert_eq!(out, body);
        assert_eq!(
            requests.load(Ordering::SeqCst),
            5,
            "36-byte body at 8-byte chunks should issue five range requests"
        );
    }

    #[test]
    fn remote_http_download_resumes_after_mid_body_failures() {
        let body: &'static [u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
        // The first two GET bodies are cut off halfway through.
        let (url, requests) = serve_http_truncating_bodies(body, None, 2, |len| len / 2);
        let mut out = Vec::new();
        let source = super::ObjectStoreSource::open_for_download(Path::new(&url))
            .expect("open download source");
        source
            .download_to_writer(&mut out, 8)
            .expect("download should resume past truncated bodies");
        assert_eq!(out, body);
        assert_eq!(
            requests.load(Ordering::SeqCst),
            6,
            "truncated GETs (0..8 -> 4 bytes, 4..12 -> 4 bytes) then four full chunks"
        );
    }

    #[test]
    fn remote_http_download_lets_object_store_resume_etag_bodies_first() {
        let body: &'static [u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
        // With an ETag, object_store resumes in-stream within the current chunk:
        // 0..8 (4 bytes), 4..8 (2), 6..8, then four full chunks = 7 requests.
        // Without one, our own resume of 4..12 makes it six.
        let (url, requests) = serve_http_truncating_bodies(body, Some("\"v1\""), 2, |len| len / 2);
        let mut out = Vec::new();
        let source = super::ObjectStoreSource::open_for_download(Path::new(&url))
            .expect("open download source");
        source
            .download_to_writer(&mut out, 8)
            .expect("download should complete through object_store's in-stream resume");
        assert_eq!(out, body);
        assert_eq!(
            requests.load(Ordering::SeqCst),
            7,
            "object_store's resumes should stay within the 0..8 chunk"
        );
    }

    #[test]
    fn single_line_error_joins_remote_error_lines() {
        let err = super::remote_status_read_error("http://example.com/demo.mcap", "425 Too Early");
        assert_eq!(
            super::single_line_error(&err),
            "failed to read http://example.com/demo.mcap: Remote server returned 425 Too Early"
        );
        let plain = anyhow::anyhow!("download stalled (no data for 120s)");
        assert_eq!(
            super::single_line_error(&plain),
            "download stalled (no data for 120s)"
        );
    }

    #[test]
    fn download_progress_notes_break_the_progress_line_once() {
        let mut progress = super::DownloadProgress {
            is_tty: true,
            total: 100,
            written: 8,
            last_report: std::time::Instant::now(),
            line_open: false,
        };
        let mut out = Vec::new();
        progress.write_report(&mut out, false).unwrap();
        progress.write_note(&mut out, "Warning: first").unwrap();
        progress.write_note(&mut out, "Warning: second").unwrap();
        progress.write_report(&mut out, false).unwrap();
        progress.write_report(&mut out, true).unwrap();
        let text = String::from_utf8(out).unwrap();
        let progress_line = format!(
            "\r{:<48}",
            format!(
                "Downloading {} / {}",
                crate::render::human_bytes(8),
                crate::render::human_bytes(100)
            )
        );
        assert_eq!(
            text,
            format!(
                "{progress_line}\nWarning: first\nWarning: second\n{progress_line}{progress_line}\n"
            ),
            "notes should end the open progress line exactly once, got {text:?}"
        );
    }

    #[test]
    fn remote_http_download_gives_up_after_no_progress_attempts() {
        let body: &'static [u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
        // Every GET body is closed before sending any bytes.
        let (url, requests) = serve_http_truncating_bodies(body, None, usize::MAX, |_| 0);
        let mut out = Vec::new();
        let source = super::ObjectStoreSource::open_for_download(Path::new(&url))
            .expect("open download source");
        let err = source
            .download_to_writer(&mut out, 8)
            .expect_err("download with no progress should give up");
        assert!(
            err.to_string().contains("attempts with no progress"),
            "unexpected error: {err:#}"
        );
        assert_eq!(
            requests.load(Ordering::SeqCst),
            super::REMOTE_DOWNLOAD_NO_PROGRESS_ATTEMPTS,
            "should stop after the no-progress attempt budget"
        );
    }

    #[test]
    fn remote_http_download_retries_transient_resume_head_failure() {
        let body: &'static [u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
        // The second connection is the first resume GET; fail its head request
        // with a status object_store neither classifies nor retries itself.
        let (url, requests) = serve_http_failing_request(body, 1, "425 Too Early");
        let mut out = Vec::new();
        let source = super::ObjectStoreSource::open_for_download(Path::new(&url))
            .expect("open download source");
        source
            .download_to_writer(&mut out, 8)
            .expect("download should retry a transient resume head failure");
        assert_eq!(out, body);
        assert_eq!(
            requests.load(Ordering::SeqCst),
            6,
            "five range requests plus one retried head failure"
        );
    }

    #[test]
    fn remote_http_download_aborts_on_permanent_resume_head_failure() {
        let body: &'static [u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
        for status in ["404 Not Found", "403 Forbidden", "401 Unauthorized"] {
            let (url, requests) = serve_http_failing_request(body, 1, status);
            let mut out = Vec::new();
            let source = super::ObjectStoreSource::open_for_download(Path::new(&url))
                .expect("open download source");
            let err = source
                .download_to_writer(&mut out, 8)
                .expect_err("a permanent resume head failure should abort the download");
            let code = status.split(' ').next().unwrap();
            assert!(
                format!("{err:#}").contains(code),
                "error for {status} should name the status: {err:#}"
            );
            assert_eq!(
                requests.load(Ordering::SeqCst),
                2,
                "{status} should not be retried"
            );
        }
    }

    #[test]
    fn remote_http_download_aborts_when_object_size_changes_mid_download() {
        let body: &'static [u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
        let replaced: &'static [u8] = b"a different object entirely";
        // No ETag, so nothing pins the object: the size check is the only
        // guard against splicing two objects together.
        let (url, requests) = serve_http_scripted(body, None, move |index| {
            if index == 1 {
                ScriptedResponse::Body(replaced)
            } else {
                ScriptedResponse::Normal
            }
        });
        let mut out = Vec::new();
        let source = super::ObjectStoreSource::open_for_download(Path::new(&url))
            .expect("open download source");
        let err = source
            .download_to_writer(&mut out, 8)
            .expect_err("a size change should abort the download");
        assert!(
            err.to_string().contains("remote object changed"),
            "unexpected error: {err:#}"
        );
        assert_eq!(
            requests.load(Ordering::SeqCst),
            2,
            "a size change should not be retried"
        );
        assert_eq!(out, &body[..8], "nothing from the new object is written");
    }

    #[test]
    fn remote_http_download_aborts_when_same_size_object_is_rewritten_mid_download() {
        let body: &'static [u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
        let rewritten: &'static [u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        assert_eq!(body.len(), rewritten.len());
        // No ETag and no size change: only Last-Modified reveals the rewrite.
        let (url, requests) = TestHttpServer {
            last_modified: Some("Mon, 01 Sep 2025 00:00:00 GMT"),
            script: Some(Arc::new(move |index| {
                if index == 1 {
                    ScriptedResponse::Modified(rewritten, "Tue, 02 Sep 2025 00:00:00 GMT")
                } else {
                    ScriptedResponse::Normal
                }
            })),
            ..TestHttpServer::new(body)
        }
        .serve();
        let mut out = Vec::new();
        let source = super::ObjectStoreSource::open_for_download(Path::new(&url))
            .expect("open download source");
        let err = source
            .download_to_writer(&mut out, 8)
            .expect_err("a Last-Modified change should abort the download");
        let message = err.to_string();
        assert!(
            message.contains("remote object changed") && message.contains("last modified"),
            "unexpected error: {err:#}"
        );
        assert_eq!(requests.load(Ordering::SeqCst), 2);
        assert_eq!(out, &body[..8], "nothing from the new object is written");
    }

    #[test]
    fn remote_range_read_absorbs_a_short_server_error_burst_inside_object_store() {
        let body: &'static [u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
        // Two 503s then success: object_store retries these itself, well inside
        // the head timeout, so the read succeeds without any CLI-level retry.
        let (url, requests) = serve_http_scripted(body, None, |index| {
            if index < 2 {
                ScriptedResponse::Status("503 Service Unavailable")
            } else {
                ScriptedResponse::Normal
            }
        });
        let source =
            super::ObjectStoreSource::open_for_download(Path::new(&url)).expect("open source");
        let bytes = source
            .get_range(0..8)
            .expect("object_store should retry a short 5xx burst");
        assert_eq!(bytes, &body[..8]);
        assert_eq!(requests.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn remote_range_read_retries_a_hung_response_head() {
        let body: &'static [u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
        // The first connection is accepted but never answered.
        let (url, requests) = TestHttpServer {
            script: Some(Arc::new(|index| {
                if index == 0 {
                    ScriptedResponse::Hang
                } else {
                    ScriptedResponse::Normal
                }
            })),
            ..TestHttpServer::new(body)
        }
        .serve();
        let mut source =
            super::ObjectStoreSource::open_for_download(Path::new(&url)).expect("open source");
        // Long enough that a healthy retry cannot miss the timeout on a loaded CI runner.
        source.response_timeout = std::time::Duration::from_secs(2);
        let bytes = source
            .get_range(0..8)
            .expect("a hung response head should be retried");
        assert_eq!(bytes, &body[..8]);
        assert_eq!(requests.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn remote_range_read_gives_up_after_response_attempts() {
        let body: &'static [u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
        let (url, requests) = TestHttpServer {
            script: Some(Arc::new(|_| ScriptedResponse::Hang)),
            ..TestHttpServer::new(body)
        }
        .serve();
        let mut source =
            super::ObjectStoreSource::open_for_download(Path::new(&url)).expect("open source");
        source.response_timeout = std::time::Duration::from_millis(200);
        let err = source
            .get_range(0..8)
            .expect_err("a server that never answers should fail");
        assert!(
            err.to_string().contains("timed out waiting for response"),
            "unexpected error: {err:#}"
        );
        assert_eq!(
            requests.load(Ordering::SeqCst),
            super::REMOTE_RESPONSE_ATTEMPTS
        );
    }

    #[test]
    fn remote_http_download_pins_strong_etag_on_resume() {
        let body: &'static [u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
        // The server answers 428 to any resumed range without If-Match.
        let (url, requests) = TestHttpServer {
            etag: Some("\"v1\""),
            require_if_match: true,
            ..TestHttpServer::new(body)
        }
        .serve();
        let mut out = Vec::new();
        let source = super::ObjectStoreSource::open_for_download(Path::new(&url))
            .expect("open download source");
        source
            .download_to_writer(&mut out, 8)
            .expect("resumes should carry the strong ETag in If-Match");
        assert_eq!(out, body);
        assert_eq!(requests.load(Ordering::SeqCst), 5);
    }

    #[test]
    fn remote_http_download_does_not_pin_weak_etag_on_resume() {
        let body: &'static [u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
        // A compliant server never matches a weak ETag under If-Match, so
        // sending it would 412 every resume.
        let (url, requests) = TestHttpServer {
            etag: Some("W/\"v1\""),
            ..TestHttpServer::new(body)
        }
        .serve();
        let mut out = Vec::new();
        let source = super::ObjectStoreSource::open_for_download(Path::new(&url))
            .expect("open download source");
        source
            .download_to_writer(&mut out, 8)
            .expect("resumes must not send a weak ETag in If-Match");
        assert_eq!(out, body);
        assert_eq!(requests.load(Ordering::SeqCst), 5);
    }

    #[test]
    fn remote_http_download_aborts_when_object_changes_mid_download() {
        let body: &'static [u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
        // A 412 on the resume GET means the pinned ETag no longer matches.
        let (url, requests) = serve_http_failing_request(body, 1, "412 Precondition Failed");
        let mut out = Vec::new();
        let source = super::ObjectStoreSource::open_for_download(Path::new(&url))
            .expect("open download source");
        let err = source
            .download_to_writer(&mut out, 8)
            .expect_err("a precondition failure should abort the download");
        assert!(
            err.to_string().contains("remote object changed"),
            "unexpected error: {err:#}"
        );
        assert_eq!(
            requests.load(Ordering::SeqCst),
            2,
            "a precondition failure should not be retried"
        );
    }

    #[test]
    fn remote_http_download_does_not_retry_local_write_errors() {
        struct FailingWriter;
        impl Write for FailingWriter {
            fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
                Err(std::io::Error::other("disk full"))
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let body: &'static [u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
        let (url, requests) = serve_http_truncating_bodies(body, None, 0, |len| len);
        let source = super::ObjectStoreSource::open_for_download(Path::new(&url))
            .expect("open download source");
        let err = source
            .download_to_writer(&mut FailingWriter, 8)
            .expect_err("local write errors should fail the download");
        assert!(
            err.to_string().contains("failed to write remote input"),
            "unexpected error: {err:#}"
        );
        assert_eq!(
            requests.load(Ordering::SeqCst),
            1,
            "a local write error should not be retried"
        );
    }

    #[test]
    fn remote_http_download_empty_object_falls_back_to_unranged_get() {
        let url = serve_http(b"", true);
        let mut out = Vec::new();
        super::read_remote_input_to_writer(Path::new(&url), &mut out)
            .expect("empty ranged object should fall back to an unranged GET");
        assert!(out.is_empty());
    }

    #[test]
    fn remote_http_input_rejects_gzip_content_encoding() {
        let url = serve_http_with_headers(b"hello remote", false, &[("Content-Encoding", "gzip")]);
        let err = materialize_input(Path::new(&url), super::SourceOptions::new(true))
            .err()
            .expect("gzip-encoded remote read should fail");
        let message = format!("{err:#}");
        assert!(message.contains("MCAP remote reads require identity encoding"));
    }

    #[test]
    fn remote_http_range_status_error_is_concise() {
        let url = serve_http_status(404, "Not Found", b"Not found");
        let err = super::parse_mcap_from_path(Path::new(&url), super::SourceOptions::default())
            .expect_err("range HTTP status error should surface cleanly");
        let message = format!("{err:#}");
        assert!(
            message.starts_with(&format!(
                "failed to read {url}\nRemote server returned 404 Not Found"
            )),
            "{message}"
        );
        assert!(!message.contains("Object at location"), "{message}");
        assert!(!message.contains("Error performing GET"), "{message}");
        assert!(!message.contains("MCAP file ended"), "{message}");
    }

    #[test]
    fn remote_http_truncated_mcap_suggests_recover() {
        let url = serve_http(mcap::MAGIC, true);
        let err = super::parse_mcap_from_path(Path::new(&url), super::SourceOptions::default())
            .expect_err("truncated remote MCAP should fail");
        let message = format!("{err:#}");
        assert!(
            message.starts_with(&format!(
                "failed to read {url}\nMCAP file appears truncated or incomplete"
            )),
            "{message}"
        );
        assert!(
            message.contains("(try running `mcap --allow-remote-scan recover`)"),
            "{message}"
        );
    }

    #[test]
    fn remote_http_non_mcap_input_reports_not_mcap() {
        let url = serve_http(b"hello remote", true);
        let err = super::parse_mcap_from_path(Path::new(&url), super::SourceOptions::default())
            .expect_err("non-MCAP remote input should fail");
        let message = format!("{err:#}");
        assert!(
            message.starts_with(&format!(
                "failed to read {url}\nInput does not appear to be an MCAP file"
            )),
            "{message}"
        );
        assert!(
            !message.contains("Footer record couldn't be found"),
            "{message}"
        );
        assert!(!message.contains("mcap recover"), "{message}");
    }

    #[test]
    fn remote_http_trailing_magic_without_footer_reports_missing_footer() {
        let mut body = Vec::new();
        body.extend_from_slice(mcap::MAGIC);
        body.extend_from_slice(&[0; 40]);
        body.extend_from_slice(mcap::MAGIC);
        let url = serve_http(Box::leak(body.into_boxed_slice()), true);
        let err = super::parse_mcap_from_path(Path::new(&url), super::SourceOptions::default())
            .expect_err("missing footer before trailing magic should fail");
        let message = format!("{err:#}");
        assert!(
            message.starts_with(&format!(
                "failed to read {url}\nMCAP file is missing its footer record"
            )),
            "{message}"
        );
    }

    #[test]
    fn remote_range_probe_rejects_gzip_content_encoding() {
        let mut buffer = Vec::new();
        {
            let mut writer = mcap::Writer::new(std::io::Cursor::new(&mut buffer)).expect("writer");
            writer.finish().expect("finish writer");
        }
        let body: &'static [u8] = Box::leak(buffer.into_boxed_slice());
        let url = serve_http_with_headers(body, true, &[("Content-Encoding", "gzip")]);
        let err = match crate::byte_source::open_byte_source(
            Some(Path::new(&url)),
            super::SourceOptions::default(),
        ) {
            Ok(_) => panic!("gzip-encoded range probe should fail"),
            Err(err) => err,
        };
        let message = format!("{err:#}");
        assert!(message.contains("MCAP remote reads require identity encoding"));
    }

    #[test]
    fn remote_http_not_found_prefers_http_error_over_mcap_parse_error() {
        let url = serve_http_status_with_range_body(b"Not found", 404, "Not Found", b"Not found");
        let err = super::parse_mcap_from_path(Path::new(&url), super::SourceOptions::default())
            .expect_err("missing HTTP object should surface as an HTTP error");
        let message = format!("{err:#}");
        assert!(
            message.starts_with(&format!(
                "failed to read {url}\nRemote server returned 404 Not Found"
            )),
            "{message}"
        );
        assert!(!message.contains("Object at location"), "{message}");
        assert!(!message.contains("Error performing HEAD"), "{message}");
        assert!(!message.contains("MCAP file ended"), "{message}");
    }

    #[test]
    fn remote_http_status_error_prefers_http_error_over_mcap_parse_error() {
        let url =
            serve_http_status_with_range_body(b"Access denied", 403, "Forbidden", b"Forbidden");
        let err = super::parse_mcap_from_path(Path::new(&url), super::SourceOptions::default())
            .expect_err("HTTP status error should surface instead of an MCAP parse error");
        let message = format!("{err:#}");
        assert!(
            message.starts_with(&format!(
                "failed to read {url}\nRemote server returned 403 Forbidden"
            )),
            "{message}"
        );
        assert!(!message.contains("Error performing HEAD"), "{message}");
        assert!(!message.contains("MCAP file ended"), "{message}");
    }

    #[test]
    fn remote_range_probe_errors_on_unknown_content_range_total() {
        // object_store cannot parse a `*` total in `Content-Range`, so the probe must
        // surface a fetch error instead of trusting a bogus size. This guards the
        // assumption documented in `read_summary_tail`.
        let (buffer, _) = summary_mcap_with_channel();
        let body: &'static [u8] = Box::leak(buffer.into_boxed_slice());
        let (url, _requests) = TestHttpServer {
            unknown_range_total: true,
            ..TestHttpServer::new(body)
        }
        .serve();
        let err = match crate::byte_source::open_byte_source(
            Some(Path::new(&url)),
            super::SourceOptions::default(),
        ) {
            Ok(_) => panic!("unknown range total should surface as an error, not a bogus size"),
            Err(err) => err,
        };
        let message = format!("{err:#}");
        assert!(message.contains("Failed while fetching range from"));
    }

    #[test]
    fn remote_summary_read_requires_scan_for_oversized_summary_section() {
        let len = usize::try_from(super::MAX_REMOTE_INDEXED_BYTES_WITHOUT_SCAN)
            .expect("remote indexed budget should fit usize")
            + crate::parse::FOOTER_RECORD_AND_END_MAGIC_LEN
            + mcap::MAGIC.len()
            + 1;
        let mut body = vec![0u8; len];
        body[..mcap::MAGIC.len()].copy_from_slice(mcap::MAGIC);
        let footer_start = len - crate::parse::FOOTER_RECORD_AND_END_MAGIC_LEN;
        body[footer_start] = records::op::FOOTER;
        body[footer_start + 1..footer_start + 9].copy_from_slice(&20u64.to_le_bytes());
        body[footer_start + 9..footer_start + 17]
            .copy_from_slice(&(mcap::MAGIC.len() as u64).to_le_bytes());
        body[footer_start + 17..footer_start + 25].copy_from_slice(&0u64.to_le_bytes());
        body[footer_start + 25..footer_start + 29].copy_from_slice(&0u32.to_le_bytes());
        body[len - mcap::MAGIC.len()..].copy_from_slice(mcap::MAGIC);

        let url = serve_http(Box::leak(body.into_boxed_slice()), true);
        let mut reader = super::open_remote_range_reader(Path::new(&url))
            .expect("remote open")
            .expect("range support");
        let err =
            super::read_summary_bytes_from_remote(&mut reader, super::SourceOptions::default())
                .expect_err("oversized remote summary should require scan opt-in");
        let message = format!("{err:#}");
        assert!(message.contains("remote summary section"));
        assert!(message.contains("--allow-remote-scan"));
    }

    #[test]
    fn remote_indexed_read_budget_requires_scan_for_oversized_total() {
        let err = super::require_remote_indexed_read_budget(
            super::MAX_REMOTE_INDEXED_BYTES_WITHOUT_SCAN + 1,
            super::SourceOptions::default(),
            "remote metadata records",
        )
        .expect_err("oversized indexed read should require scan opt-in");
        assert!(err.to_string().contains("remote metadata records"));
        assert!(err
            .to_string()
            .contains(&human_bytes(super::MAX_REMOTE_INDEXED_BYTES_WITHOUT_SCAN)));
        assert!(err.to_string().contains("--allow-remote-scan"));
    }

    #[test]
    fn remote_url_scheme_is_case_insensitive() {
        assert!(super::is_remote_url(Path::new(
            "HTTP://example.com/demo.mcap"
        )));
        assert!(super::is_remote_url(Path::new(
            "Https://example.com/demo.mcap"
        )));
    }

    #[test]
    fn remote_url_recognizes_cloud_schemes() {
        for url in [
            "s3://bucket/demo.mcap",
            "s3a://bucket/demo.mcap",
            "gs://bucket/demo.mcap",
            "az://container@account.blob.core.windows.net/demo.mcap",
            "azure://container@account.blob.core.windows.net/demo.mcap",
            "adl://container@account.dfs.core.windows.net/demo.mcap",
            "abfs://container@account.dfs.core.windows.net/demo.mcap",
            "abfss://container@account.dfs.core.windows.net/demo.mcap",
        ] {
            assert!(super::is_remote_url(Path::new(url)), "{url}");
        }
    }

    #[test]
    fn remote_url_kind_classifies_suffix_capability() {
        use super::RemoteUrlKind;
        for scheme in ["http", "https"] {
            let kind = RemoteUrlKind::from_scheme(scheme).expect(scheme);
            assert_eq!(kind, RemoteUrlKind::Http, "{scheme}");
            assert!(kind.supports_suffix_range(), "{scheme}");
            assert!(!kind.range_support_is_guaranteed(), "{scheme}");
        }
        for scheme in ["s3", "s3a", "gs"] {
            let kind = RemoteUrlKind::from_scheme(scheme).expect(scheme);
            assert_eq!(kind, RemoteUrlKind::CloudSuffix, "{scheme}");
            assert!(kind.supports_suffix_range(), "{scheme}");
            assert!(kind.range_support_is_guaranteed(), "{scheme}");
        }
        for scheme in ["az", "azure", "adl", "abfs", "abfss"] {
            let kind = RemoteUrlKind::from_scheme(scheme).expect(scheme);
            assert_eq!(kind, RemoteUrlKind::CloudNoSuffix, "{scheme}");
            assert!(!kind.supports_suffix_range(), "{scheme}");
            assert!(kind.range_support_is_guaranteed(), "{scheme}");
        }
    }

    #[test]
    fn remote_extension_ignores_query_and_fragment() {
        assert_eq!(
            super::remote_or_local_extension(Path::new(
                "https://example.com/demo.bag?token=secret#section"
            ))
            .as_deref(),
            Some("bag")
        );
        assert_eq!(
            super::remote_or_local_extension(Path::new(
                "s3://bucket/path/demo.db3?X-Amz-Signature=secret#fragment"
            ))
            .as_deref(),
            Some("db3")
        );
    }

    #[test]
    fn http_range_reader_uses_object_store_and_allows_seek_past_end() {
        let url = serve_http(b"hello remote", true);
        let mut reader = super::RemoteRangeReader::open(Path::new(&url))
            .expect("HTTP range reader should open through object_store")
            .expect("HTTP range reader should support ranges");

        assert_eq!(reader.read_range(0, 5).expect("range"), b"hello");
        assert_eq!(
            std::io::Seek::seek(&mut reader, SeekFrom::End(1)).unwrap(),
            13
        );
        let mut byte = [0_u8; 1];
        assert_eq!(std::io::Read::read(&mut reader, &mut byte).unwrap(), 0);
    }

    #[test]
    fn object_store_range_reader_reads_and_seeks() {
        let mut reader = object_store_memory_reader(b"hello remote object".to_vec());

        assert_eq!(reader.read_range(0, 5).expect("range"), b"hello");
        assert_eq!(reader.read_range(13, 20).expect("clamped range"), b"object");

        let mut buf = [0_u8; 6];
        assert_eq!(reader.read(&mut buf).expect("read"), 6);
        assert_eq!(&buf, b"hello ");

        assert_eq!(reader.seek(SeekFrom::Current(1)).expect("seek"), 7);
        let mut buf = [0_u8; 6];
        assert_eq!(reader.read(&mut buf).expect("read"), 6);
        assert_eq!(&buf, b"emote ");

        assert_eq!(reader.seek(SeekFrom::End(-6)).expect("seek end"), 13);
        let mut tail = Vec::new();
        reader.read_to_end(&mut tail).expect("read tail");
        assert_eq!(tail, b"object");

        assert_eq!(reader.seek(SeekFrom::End(1)).expect("seek past end"), 20);
        let mut byte = [0_u8; 1];
        assert_eq!(reader.read(&mut byte).expect("eof"), 0);
    }

    #[test]
    fn object_store_source_open_uses_url_parser() {
        let source = super::ObjectStoreSource::open_for_download(Path::new(
            "https://example.com/demo.mcap?token=secret",
        ))
        .expect("HTTP object store URL should parse");
        assert_eq!(source.path.as_ref(), "demo.mcap");
        assert_eq!(source.display_url, "https://example.com/demo.mcap");
    }

    #[test]
    fn remote_url_options_keep_http_and_cloud_config_separate() {
        use std::ffi::OsString;

        let vars = [
            (OsString::from("AWS_ACCESS_KEY_ID"), OsString::from("akid")),
            (OsString::from("GOOGLE_BUCKET"), OsString::from("bucket")),
            (
                OsString::from("AZURE_STORAGE_ACCOUNT_NAME"),
                OsString::from("account"),
            ),
        ];
        let http =
            super::RemoteUrl::parse(Path::new("http://example.com/demo.mcap")).expect("http URL");
        assert_eq!(
            http.options_from_env_vars(vars.clone()),
            vec![("allow_http".to_string(), "true".to_string())]
        );

        let https =
            super::RemoteUrl::parse(Path::new("https://example.com/demo.mcap")).expect("https URL");
        assert!(https.options_from_env_vars(vars.clone()).is_empty());

        let s3 = super::RemoteUrl::parse(Path::new("s3://bucket/demo.mcap")).expect("s3 URL");
        assert_eq!(
            s3.options_from_env_vars(vars),
            vec![
                ("AWS_ACCESS_KEY_ID".to_string(), "akid".to_string()),
                ("GOOGLE_BUCKET".to_string(), "bucket".to_string()),
                (
                    "AZURE_STORAGE_ACCOUNT_NAME".to_string(),
                    "account".to_string()
                ),
            ]
        );
    }

    #[test]
    fn remote_url_store_options_set_long_request_timeout() {
        let url = super::RemoteUrl::parse(Path::new("https://example.com/demo.mcap")).expect("url");
        let options = url.store_options();
        let timeouts: Vec<_> = options
            .iter()
            .filter(|(key, _)| key == object_store::ClientConfigKey::Timeout.as_ref())
            .collect();
        assert_eq!(
            timeouts,
            vec![&(
                object_store::ClientConfigKey::Timeout.as_ref().to_string(),
                super::REMOTE_REQUEST_TIMEOUT.to_string()
            )],
            "store options should set exactly one long request timeout, got {options:?}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn object_store_env_options_ignore_non_utf8_values() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let options = super::object_store_options_from_env_vars([
            (OsString::from("AWS_REGION"), OsString::from("us-east-1")),
            (
                OsString::from_vec(vec![0xFF, b'B', b'A', b'D']),
                OsString::from("ignored-key"),
            ),
            (
                OsString::from("IGNORED_VALUE"),
                OsString::from_vec(vec![0xFF, b'v']),
            ),
        ]);
        assert_eq!(
            options,
            vec![("AWS_REGION".to_string(), "us-east-1".to_string())]
        );
    }

    #[test]
    fn object_store_env_options_only_forward_recognized_prefixes() {
        use std::ffi::OsString;

        let options = super::object_store_options_from_env_vars([
            (OsString::from("AWS_ACCESS_KEY_ID"), OsString::from("akid")),
            (OsString::from("GOOGLE_BUCKET"), OsString::from("bucket")),
            (
                OsString::from("AZURE_STORAGE_ACCOUNT_NAME"),
                OsString::from("account"),
            ),
            // Unprefixed aliases like `endpoint`/`region`/`token` would otherwise
            // be applied by object_store; they must not be forwarded.
            (
                OsString::from("ENDPOINT"),
                OsString::from("http://attacker"),
            ),
            (OsString::from("REGION"), OsString::from("elsewhere")),
            (OsString::from("TOKEN"), OsString::from("unrelated")),
        ]);
        assert_eq!(
            options,
            vec![
                ("AWS_ACCESS_KEY_ID".to_string(), "akid".to_string()),
                ("GOOGLE_BUCKET".to_string(), "bucket".to_string()),
                (
                    "AZURE_STORAGE_ACCOUNT_NAME".to_string(),
                    "account".to_string()
                ),
            ]
        );
    }

    #[test]
    fn remote_range_unsatisfiable_matches_status_not_body_digits() {
        let unsatisfiable = object_store::Error::Generic {
            store: "HTTP",
            source: "Server returned non-2xx status code: 416 Range Not Satisfiable".into(),
        };
        assert!(super::remote_range_unsatisfiable(&unsatisfiable));

        let other = object_store::Error::Generic {
            store: "HTTP",
            source: "Server returned non-2xx status code: 400 Bad Request: request-id-416-xyz"
                .into(),
        };
        assert!(!super::remote_range_unsatisfiable(&other));
    }

    #[test]
    fn remote_mcap_summary_uses_range_reader() {
        let (buffer, channel_id) = summary_mcap_with_channel();
        let body: &'static [u8] = Box::leak(buffer.into_boxed_slice());
        let url = serve_http(body, true);
        let mut source = crate::byte_source::open_byte_source(
            Some(Path::new(&url)),
            super::SourceOptions::default(),
        )
        .expect("remote open");
        let summary = crate::byte_source::read_summary(source.as_mut())
            .expect("remote summary read")
            .expect("summary should be present");

        assert!(summary.channels.contains_key(&channel_id));
    }

    #[test]
    fn remote_summary_uses_single_http_request_when_tail_contains_summary() {
        // The whole point of the tail prefetch: when the summary fits in the tail,
        // summary discovery must take exactly one HTTP request (no probe/HEAD/footer).
        let (buffer, channel_id) = summary_mcap_with_channel();
        let body: &'static [u8] = Box::leak(buffer.into_boxed_slice());
        let (url, requests) = serve_http_counting(body, true);
        let mut reader = super::open_remote_range_reader(Path::new(&url))
            .expect("remote open")
            .expect("range support");
        let summary_bytes =
            super::read_summary_bytes_from_remote(&mut reader, super::SourceOptions::default())
                .expect("remote summary read")
                .expect("summary should be present");
        let summary = crate::parse::slice::parse_summary_section(&summary_bytes)
            .expect("parse summary section");
        assert!(summary.channels.contains_key(&channel_id));
        assert_eq!(
            requests.load(Ordering::SeqCst),
            1,
            "summary discovery should make exactly one range request"
        );
    }

    #[test]
    fn remote_summary_reads_from_prefetched_tail_without_extra_request() {
        // A tail covering the whole file (tail_start == 0) must yield the summary
        // entirely from the prefetched bytes, with no back-fill range read.
        let (buffer, channel_id) = summary_mcap_with_channel();
        let mut reader = object_store_memory_reader_with_tail(buffer, 0);
        let summary_bytes =
            super::read_summary_bytes_from_remote(&mut reader, super::SourceOptions::default())
                .expect("summary read")
                .expect("summary should be present");
        let summary = crate::parse::slice::parse_summary_section(&summary_bytes)
            .expect("parse summary section");
        assert!(summary.channels.contains_key(&channel_id));
    }

    #[test]
    fn remote_summary_backfills_prefix_when_tail_is_short() {
        // Simulate a summary larger than the prefetched tail: the tail starts after
        // `summary_start`, so discovery must issue one back-fill read for the prefix.
        let (buffer, channel_id) = summary_mcap_with_channel();
        let footer_start = buffer.len() - crate::parse::FOOTER_RECORD_AND_END_MAGIC_LEN;
        let summary_start = u64::from_le_bytes(
            buffer[footer_start + 9..footer_start + 17]
                .try_into()
                .expect("summary_start slice"),
        );
        assert!(summary_start > 0, "test MCAP must have a summary section");
        // Place the tail boundary strictly inside the summary region so the prefix
        // `[summary_start, tail_start)` is missing from the tail but the footer is not.
        let tail_start = summary_start + 1;
        assert!(tail_start <= footer_start as u64);

        let mut reader = object_store_memory_reader_with_tail(buffer, tail_start);
        let summary_bytes =
            super::read_summary_bytes_from_remote(&mut reader, super::SourceOptions::default())
                .expect("summary read with back-fill")
                .expect("summary should be present");
        let summary = crate::parse::slice::parse_summary_section(&summary_bytes)
            .expect("parse summary section");
        assert!(summary.channels.contains_key(&channel_id));
    }

    #[test]
    fn remote_mcap_summary_uses_range_get_when_head_is_rejected() {
        let (buffer, channel_id) = summary_mcap_with_channel();
        let body: &'static [u8] = Box::leak(buffer.into_boxed_slice());
        let (url, _requests) = TestHttpServer {
            reject_head: true,
            ..TestHttpServer::new(body)
        }
        .serve();
        let mut source = crate::byte_source::open_byte_source(
            Some(Path::new(&url)),
            super::SourceOptions::default(),
        )
        .expect("remote summary should use range GET, not HEAD");
        let summary = crate::byte_source::read_summary(source.as_mut())
            .expect("remote summary read")
            .expect("summary should be present");

        assert!(summary.channels.contains_key(&channel_id));
    }

    #[test]
    fn remote_summary_recovers_when_server_rejects_suffix_ranges() {
        // A server that honors bounded ranges but rejects suffix ranges must still
        // open without a scan: the suffix request fails, we fall back to a bounded
        // probe for the size, then read a bounded tail (suffix + probe + tail = 3).
        let (buffer, channel_id) = summary_mcap_with_channel();
        let body: &'static [u8] = Box::leak(buffer.into_boxed_slice());
        let (url, requests) = serve_http_bounded_only(body);
        let mut reader = super::open_remote_range_reader(Path::new(&url))
            .expect("bounded-only server should open without a scan")
            .expect("range support");
        let summary_bytes =
            super::read_summary_bytes_from_remote(&mut reader, super::SourceOptions::default())
                .expect("summary read")
                .expect("summary should be present");
        let summary = crate::parse::slice::parse_summary_section(&summary_bytes)
            .expect("parse summary section");
        assert!(summary.channels.contains_key(&channel_id));
        assert_eq!(
            requests.load(Ordering::SeqCst),
            3,
            "expected suffix attempt + bounded probe + bounded tail"
        );
    }

    #[test]
    fn remote_mcap_without_range_support_requires_scan_opt_in() {
        let (buffer, _) = summary_mcap_with_channel();
        let body: &'static [u8] = Box::leak(buffer.into_boxed_slice());
        let url = serve_http(body, false);
        let err = super::parse_mcap_from_path(Path::new(&url), super::SourceOptions::default())
            .expect_err("non-range HTTP input should require scan opt-in");
        let message = err.to_string();
        assert!(message.contains("Remote server does not support range requests"));
        assert!(message.contains("--allow-remote-scan"));
    }

    #[test]
    fn remote_mcap_without_range_support_falls_back_with_scan_opt_in() {
        let (buffer, channel_id) = summary_mcap_with_channel();
        let body: &'static [u8] = Box::leak(buffer.into_boxed_slice());
        let url = serve_http(body, false);
        let parsed = super::parse_mcap_from_path(Path::new(&url), super::SourceOptions::new(true))
            .expect("non-range HTTP input should materialize with scan opt-in");

        assert!(parsed.channels.contains_key(&channel_id));
    }
}
