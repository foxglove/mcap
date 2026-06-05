use std::io::{IsTerminal as _, Read as _, SeekFrom};
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use binrw::BinRead;
use futures_util::TryStreamExt;
use mcap::records::{self, Record};
use mcap::sans_io::{SummaryReadEvent, SummaryReader, SummaryReaderOptions};
use memmap2::Mmap;
use object_store::{
    path::Path as ObjectStorePath, Attribute, GetOptions, GetRange, ObjectStore, ObjectStoreExt,
};
use tempfile::NamedTempFile;
use url::Url;

use crate::parse::{self, ParsedMcap};
use crate::render::human_bytes;

pub const PLEASE_REDIRECT: &str =
    "Binary output can screw up your terminal. Supply -o or redirect to a file or pipe";
pub const PLEASE_SUPPLY_FILE: &str = "please supply a file. see --help for usage details.";
const FOOTER_RECORD_AND_END_MAGIC_LEN: usize = 37;
// Size of the single range request issued from the end of a remote file to discover
// the summary section. One read proves range support (for HTTP), discovers the file
// size via `Content-Range`, and in the common case already contains the whole summary
// section (footer + summary + summary offset records). When the summary is larger than
// this, exactly one additional range request back-fills the missing prefix. 256 KiB
// comfortably covers the summaries of typical multi-hundred-MB to low-GB files while
// keeping the per-open transfer small on bandwidth-constrained links.
const REMOTE_SUMMARY_TAIL_BYTES: u64 = 256 * 1024;
// Guards remote summary discovery against corrupt or hostile footers that point
// `summary_start` near the beginning of a large file, which would otherwise turn
// an index-only operation into a near-full-file range read without opt-in.
const MAX_REMOTE_SUMMARY_BYTES_WITHOUT_SCAN: usize = 16 * 1024 * 1024;
// Bounds aggregate metadata body reads for list/multi-match metadata commands.
// Single indexed metadata/attachment records are deliberately uncapped beyond
// the remote file size because they are explicit user-selected record reads.
pub(crate) const MAX_REMOTE_METADATA_BYTES_WITHOUT_SCAN: u64 = 64 * 1024 * 1024;

pub enum InputData {
    Mapped(Mmap),
    TempMapped {
        mmap: Mmap,
        // Keep the temporary file alive for at least as long as the mmap. Fields drop in
        // declaration order, so this is dropped after `mmap`.
        #[allow(dead_code)]
        temp_file: NamedTempFile,
    },
    Buffered(Vec<u8>),
}

impl std::fmt::Debug for InputData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InputData")
            .field("len", &self.as_slice().len())
            .finish()
    }
}

impl std::ops::Deref for InputData {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl InputData {
    pub fn as_slice(&self) -> &[u8] {
        match self {
            InputData::Mapped(mmap) => mmap.as_ref(),
            InputData::TempMapped { mmap, .. } => mmap.as_ref(),
            InputData::Buffered(buf) => buf.as_slice(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SourceOptions {
    pub allow_remote_scan: bool,
}

impl SourceOptions {
    pub fn new(allow_remote_scan: bool) -> Self {
        Self { allow_remote_scan }
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

pub struct RemoteMcap {
    reader: RemoteRangeReader,
    summary: mcap::Summary,
}

impl RemoteMcap {
    pub fn summary(&self) -> &mcap::Summary {
        &self.summary
    }

    pub fn read_range(&self, offset: u64, length: usize) -> Result<Vec<u8>> {
        self.reader.read_range(offset, length)
    }
}

pub(crate) enum StreamingInput {
    Local(std::fs::File),
    Stdin(std::io::Stdin),
    RemoteMaterialized {
        file: std::fs::File,
        #[allow(dead_code)]
        temp_file: NamedTempFile,
    },
}

impl std::io::Read for StreamingInput {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            StreamingInput::Local(file) => file.read(buf),
            StreamingInput::Stdin(stdin) => stdin.read(buf),
            StreamingInput::RemoteMaterialized { file, .. } => file.read(buf),
        }
    }
}

pub fn map_file(path: &Path) -> anyhow::Result<Mmap> {
    let file =
        std::fs::File::open(path).with_context(|| format!("couldn't open '{}'", path.display()))?;
    unsafe { Mmap::map(&file) }.with_context(|| format!("couldn't map '{}'", path.display()))
}

pub fn open_seekable_mcap_source(path: &Path) -> Result<std::fs::File> {
    std::fs::File::open(path).with_context(|| format!("couldn't open '{}'", path.display()))
}

pub fn parse_mcap_from_path(path: &Path, options: SourceOptions) -> Result<ParsedMcap> {
    if is_remote_url(path) {
        match open_remote_range_reader(path)? {
            Some(mut reader) => {
                if let Some(summary) = read_summary_from_remote(&mut reader, options)
                    .map_err(|err| remote_read_error(path, err))?
                {
                    let header = read_header_from_seekable(&mut reader)?;
                    return Ok(parse::parsed_mcap_from_summary_ref(header, &summary));
                }
                if !options.allow_remote_scan {
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
    } else {
        let mut source = open_seekable_mcap_source(path)?;
        let header = read_header_from_seekable(&mut source)?;
        if let Some(summary) = read_summary_from_seekable(&mut source)? {
            return Ok(parse::parsed_mcap_from_summary_ref(header, &summary));
        }
    }

    let mcap = load_path(path, options)?;
    let parsed = parse::parse_mcap(&mcap);
    if is_remote_url(path) {
        parsed.map_err(|err| remote_read_error(path, err))
    } else {
        parsed
    }
}

pub fn load_path(path: &Path, options: SourceOptions) -> Result<InputData> {
    if is_remote_url(path) {
        let materialized = materialize_input(path, options)?;
        let mmap = map_file(materialized.path())?;
        let temp_file = materialized
            .temp_file
            .expect("remote materialized input should have a temp file");
        return Ok(InputData::TempMapped { temp_file, mmap });
    }
    Ok(InputData::Mapped(map_file(path)?))
}

pub fn load_input(file: Option<&Path>, options: SourceOptions) -> Result<InputData> {
    if let Some(path) = file {
        return load_path(path, options);
    }

    let stdin = std::io::stdin();
    if stdin.is_terminal() {
        bail!("{PLEASE_SUPPLY_FILE}");
    }

    let mut buf = Vec::new();
    stdin
        .lock()
        .read_to_end(&mut buf)
        .context("failed to read input from stdin")?;
    Ok(InputData::Buffered(buf))
}

pub(crate) fn open_streaming_input(
    file: Option<&Path>,
    options: SourceOptions,
) -> Result<StreamingInput> {
    if let Some(path) = file {
        if is_remote_url(path) {
            // Remote clients are async, while recover's reader pipeline is synchronous.
            // Materializing keeps the recovery path simple and consistently gated by
            // `--allow-remote-scan` in `materialize_input`.
            let materialized = materialize_input(path, options)?;
            let file = open_seekable_mcap_source(materialized.path())?;
            let temp_file = materialized
                .temp_file
                .expect("remote streaming input should have a temp file");
            return Ok(StreamingInput::RemoteMaterialized { file, temp_file });
        }
        return Ok(StreamingInput::Local(open_seekable_mcap_source(path)?));
    }

    let stdin = std::io::stdin();
    if stdin.is_terminal() {
        bail!("{PLEASE_SUPPLY_FILE}");
    }
    Ok(StreamingInput::Stdin(stdin))
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
    read_remote_input_to_writer(path, temp_file.as_file_mut())?;
    std::io::Write::flush(temp_file.as_file_mut())
        .context("failed to flush temporary remote input file")?;
    Ok(MaterializedInput {
        temp_file: Some(temp_file),
        local_path: None,
    })
}

pub fn try_open_remote_mcap(path: &Path, options: SourceOptions) -> Result<Option<RemoteMcap>> {
    if !is_remote_url(path) {
        return Ok(None);
    }
    let Some(mut reader) = open_remote_range_reader(path)? else {
        if !options.allow_remote_scan {
            bail!(
                "{}: remote server does not support range requests; {}",
                redacted_display(path),
                remote_scan_opt_in_suffix()
            );
        }
        return Ok(None);
    };
    let Some(summary) = read_summary_from_remote(&mut reader, options)
        .map_err(|err| remote_read_error(path, err))?
    else {
        if !options.allow_remote_scan {
            bail!(
                "failed to read {}\nRemote file has no summary section; reading without one requires opt-in; {}",
                redacted_display(path),
                remote_scan_opt_in_suffix()
            );
        }
        return Ok(None);
    };
    Ok(Some(RemoteMcap { reader, summary }))
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
}

impl ObjectStoreSource {
    fn open(path: &Path) -> Result<Self> {
        Self::open_remote(RemoteUrl::parse(path)?)
    }

    fn open_remote(remote_url: RemoteUrl) -> Result<Self> {
        let (store, object_path) =
            object_store::parse_url_opts(&remote_url.url, remote_url.options()).with_context(
                || {
                    format!(
                        "failed to configure remote store for {}",
                        remote_url.display_url
                    )
                },
            )?;
        Ok(Self {
            runtime: object_store_runtime()?,
            store: Arc::from(store),
            path: object_path,
            display_url: remote_url.display_url,
        })
    }

    fn stat(&self) -> Result<object_store::ObjectMeta> {
        self.runtime
            .block_on(self.store.head(&self.path))
            .map_err(|err| concise_remote_stat_error(&self.display_url, err))
    }

    fn head_size(&self) -> Result<u64> {
        Ok(self.stat()?.size)
    }

    /// Read a bounded byte range and return its bytes, validating the content
    /// encoding. The range is assumed to be valid (non-empty, within the object).
    fn get_range(&self, range: std::ops::Range<u64>) -> Result<Vec<u8>> {
        let response = self
            .runtime
            .block_on(self.store.get_opts(
                &self.path,
                GetOptions {
                    range: Some(GetRange::Bounded(range)),
                    ..GetOptions::default()
                },
            ))
            .map_err(|err| {
                concise_remote_operation_error("fetching range from", &self.display_url, err)
            })?;
        validate_identity_content_encoding(&response.attributes, &self.display_url)?;
        let bytes = self
            .runtime
            .block_on(response.bytes())
            .with_context(|| format!("failed to read range from {}", self.display_url))?;
        Ok(bytes.to_vec())
    }

    /// Probe bounded range support with a one-byte request, returning the object
    /// size parsed from `Content-Range`. `Ok(None)` means the server does not honor
    /// range requests at all. Used as a fallback for HTTP servers that accept bounded
    /// ranges but reject suffix ranges, and to learn the size without a HEAD (which
    /// some HTTP servers reject).
    fn probe_bounded_range_size(&self) -> Result<Option<u64>> {
        match self.runtime.block_on(self.store.get_opts(
            &self.path,
            GetOptions {
                range: Some(GetRange::Bounded(0..1)),
                ..GetOptions::default()
            },
        )) {
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
            match self.runtime.block_on(self.store.get_opts(
                &self.path,
                GetOptions {
                    range: Some(GetRange::Suffix(tail_bytes)),
                    ..GetOptions::default()
                },
            )) {
                Ok(response) => {
                    validate_identity_content_encoding(&response.attributes, &self.display_url)?;
                    // Relies on object_store parsing a numeric total from
                    // `Content-Range` (for example `bytes 9-99/100`). A `*` total
                    // fails object_store's parse and surfaces as a fetch error rather
                    // than a bogus size.
                    let size = response.meta.size;
                    let bytes = self
                        .runtime
                        .block_on(response.bytes())
                        .with_context(|| format!("failed to read range from {}", self.display_url))?
                        .to_vec();
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
            },
            kind: RemoteUrlKind::CloudSuffix,
            size,
            offset: 0,
            tail: Some(tail),
        })
    }

    fn read_range(&self, offset: u64, length: usize) -> Result<Vec<u8>> {
        if length == 0 || offset >= self.size {
            return Ok(Vec::new());
        }
        let end = offset
            .checked_add(length as u64)
            .map(|end| end.min(self.size))
            .ok_or_else(|| anyhow::anyhow!("remote range overflow"))?;
        self.source.get_range(offset..end)
    }

    fn size(&self) -> u64 {
        self.size
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

fn open_remote_range_reader(path: &Path) -> Result<Option<RemoteRangeReader>> {
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

fn read_remote_input_to_writer(path: &Path, writer: &mut impl std::io::Write) -> Result<()> {
    let source = ObjectStoreSource::open(path)?;
    eprintln!("Warning: reading entire remote file {}", source.display_url);

    source.runtime.block_on(async {
        let response = source.store.get(&source.path).await.map_err(|err| {
            concise_remote_operation_error("reading remote input from", &source.display_url, err)
        })?;
        validate_identity_content_encoding(&response.attributes, &source.display_url)?;
        let mut stream = response.into_stream();
        while let Some(bytes) = stream
            .try_next()
            .await
            .with_context(|| format!("failed to read remote input {}", source.display_url))?
        {
            std::io::Write::write_all(writer, bytes.as_ref())
                .with_context(|| format!("failed to write remote input {}", source.display_url))?;
        }
        Ok::<(), anyhow::Error>(())
    })?;

    Ok(())
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

pub(crate) fn require_remote_metadata_budget(
    total_bytes: u64,
    options: SourceOptions,
    description: &str,
) -> Result<()> {
    if options.allow_remote_scan || total_bytes <= MAX_REMOTE_METADATA_BYTES_WITHOUT_SCAN {
        return Ok(());
    }
    bail!(
        "remote {description} would read {} (exceeds {} cap without --allow-remote-scan); {}",
        human_bytes(total_bytes),
        human_bytes(MAX_REMOTE_METADATA_BYTES_WITHOUT_SCAN),
        remote_scan_opt_in_suffix()
    );
}

fn require_remote_scan_allowed(path: &Path, options: SourceOptions) -> Result<()> {
    if options.allow_remote_scan {
        return Ok(());
    }
    let display = redacted_display(path);
    bail!(
        "remote input {display} requires opt-in because this command must download or scan remote data; {}",
        remote_scan_opt_in_suffix()
    );
}

fn read_summary_from_remote(
    reader: &mut RemoteRangeReader,
    options: SourceOptions,
) -> Result<Option<mcap::Summary>> {
    let file_size = reader.size();
    let tail_len = FOOTER_RECORD_AND_END_MAGIC_LEN as u64;
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
    let footer_bytes = &tail.bytes[tail.bytes.len() - FOOTER_RECORD_AND_END_MAGIC_LEN..];
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
    if &footer_bytes[FOOTER_RECORD_AND_END_MAGIC_LEN - mcap::MAGIC.len()..] != mcap::MAGIC {
        return Err(classify_remote_summary_error(
            reader,
            mcap::McapError::BadMagic.into(),
        ));
    }

    let mut cursor =
        std::io::Cursor::new(&footer_bytes[9..FOOTER_RECORD_AND_END_MAGIC_LEN - mcap::MAGIC.len()]);
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
    if summary_len > MAX_REMOTE_SUMMARY_BYTES_WITHOUT_SCAN && !options.allow_remote_scan {
        bail!(
            "remote summary section is {} (exceeds {} cap without --allow-remote-scan); {}",
            human_bytes(summary_len as u64),
            human_bytes(MAX_REMOTE_SUMMARY_BYTES_WITHOUT_SCAN as u64),
            remote_scan_opt_in_suffix()
        );
    }

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
    parse::parse_summary_section(&summary_bytes)
        .map(Some)
        .map_err(|err| classify_remote_summary_error(reader, err))
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

fn read_summary_from_seekable(
    reader: &mut (impl std::io::Read + std::io::Seek),
) -> Result<Option<mcap::Summary>> {
    let file_size = reader.seek(SeekFrom::End(0))?;
    let mut summary_reader =
        SummaryReader::new_with_options(SummaryReaderOptions::default().with_file_size(file_size));
    while let Some(event) = summary_reader.next_event() {
        match event? {
            SummaryReadEvent::ReadRequest(n) => {
                let read = reader.read(summary_reader.insert(n))?;
                summary_reader.notify_read(read);
            }
            SummaryReadEvent::SeekRequest(to) => {
                let pos = reader.seek(to)?;
                summary_reader.notify_seeked(pos);
            }
        }
    }
    Ok(summary_reader.finish())
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
    use std::net::TcpListener;
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;

    use super::load_path;
    use crate::render::human_bytes;
    use mcap::records;
    use object_store::ObjectStoreExt;

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

    fn serve_http_with_headers(
        body: &'static [u8],
        supports_ranges: bool,
        extra_headers: &'static [(&'static str, &'static str)],
    ) -> String {
        serve_http_with_options(body, supports_ranges, extra_headers, false, false, false).0
    }

    // Like `serve_http` but also returns a counter of HTTP requests received, so tests
    // can assert how many round trips an operation makes. Each request uses a fresh
    // connection (`Connection: close`), so connections accepted == requests.
    fn serve_http_counting(
        body: &'static [u8],
        supports_ranges: bool,
    ) -> (String, Arc<AtomicUsize>) {
        serve_http_with_options(body, supports_ranges, &[], false, false, false)
    }

    // A server that honors bounded ranges (`bytes=S-E`) but rejects suffix ranges
    // (`bytes=-N`) with `416`, like HTTP servers/proxies that omit the suffix form.
    fn serve_http_bounded_only(body: &'static [u8]) -> (String, Arc<AtomicUsize>) {
        serve_http_with_options(body, true, &[], false, false, true)
    }

    fn serve_http_status_with_range_body(
        range_body: &'static [u8],
        status_code: u16,
        reason: &'static str,
        status_body: &'static [u8],
    ) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
        let addr = listener.local_addr().expect("test server addr");
        thread::spawn(move || {
            for stream in listener.incoming().take(8) {
                let mut stream = stream.expect("accept test connection");
                let mut request = [0u8; 4096];
                let read = stream.read(&mut request).expect("read request");
                let request = String::from_utf8_lossy(&request[..read]);
                let is_head = request.starts_with("HEAD ");
                let has_range = request.lines().any(|line| {
                    line.starts_with("Range: bytes=") || line.starts_with("range: bytes=")
                });
                if has_range {
                    let end = range_body.len().saturating_sub(1);
                    let response = format!(
                        "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes 0-{end}/{}\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n",
                        range_body.len(),
                        range_body.len()
                    );
                    stream
                        .write_all(response.as_bytes())
                        .expect("write range headers");
                    if !is_head {
                        stream.write_all(range_body).expect("write range body");
                    }
                } else {
                    let response = format!(
                        "HTTP/1.1 {status_code} {reason}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        status_body.len()
                    );
                    stream
                        .write_all(response.as_bytes())
                        .expect("write status headers");
                    if !is_head {
                        stream.write_all(status_body).expect("write status body");
                    }
                }
            }
        });
        format!("http://{addr}/demo.mcap")
    }

    fn serve_http_status(status_code: u16, reason: &'static str, body: &'static [u8]) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
        let addr = listener.local_addr().expect("test server addr");
        thread::spawn(move || {
            for stream in listener.incoming().take(8) {
                let mut stream = stream.expect("accept test connection");
                let mut request = [0u8; 4096];
                let read = stream.read(&mut request).expect("read request");
                let request = String::from_utf8_lossy(&request[..read]);
                let is_head = request.starts_with("HEAD ");
                let response = format!(
                    "HTTP/1.1 {status_code} {reason}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                stream
                    .write_all(response.as_bytes())
                    .expect("write status headers");
                if !is_head {
                    stream.write_all(body).expect("write status body");
                }
            }
        });
        format!("http://{addr}/demo.mcap")
    }

    fn serve_http_with_options(
        body: &'static [u8],
        supports_ranges: bool,
        extra_headers: &'static [(&'static str, &'static str)],
        reject_head: bool,
        // Emit `Content-Range: bytes <start>-<end>/*` (unknown total) instead of a numeric total.
        unknown_range_total: bool,
        // Reject suffix ranges (`bytes=-N`) with `416` while still honoring bounded
        // ranges, like HTTP servers that do not implement the suffix form.
        reject_suffix: bool,
    ) -> (String, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
        let addr = listener.local_addr().expect("test server addr");
        let request_count = Arc::new(AtomicUsize::new(0));
        let server_request_count = request_count.clone();
        thread::spawn(move || {
            for stream in listener.incoming().take(64) {
                let mut stream = stream.expect("accept test connection");
                server_request_count.fetch_add(1, Ordering::SeqCst);
                let mut request = [0u8; 4096];
                let read = stream.read(&mut request).expect("read request");
                let request = String::from_utf8_lossy(&request[..read]);
                let is_head = request.starts_with("HEAD ");
                if reject_head && is_head {
                    stream
                        .write_all(b"HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n")
                        .expect("write HEAD rejection");
                    continue;
                }
                let range_spec = request
                    .lines()
                    .find_map(|line| line.strip_prefix("Range: bytes="))
                    .or_else(|| {
                        request
                            .lines()
                            .find_map(|line| line.strip_prefix("range: bytes="))
                    });
                if reject_suffix
                    && range_spec.is_some_and(|spec| spec.trim_start().starts_with('-'))
                {
                    stream
                        .write_all(
                            format!(
                                "HTTP/1.1 416 Range Not Satisfiable\r\nContent-Range: bytes */{}\r\nConnection: close\r\n\r\n",
                                body.len()
                            )
                            .as_bytes(),
                        )
                        .expect("write 416");
                    continue;
                }
                let requested_range =
                    range_spec
                        .and_then(|range| range.split_once('-'))
                        .and_then(|(start, end)| {
                            // Supports `S-E` (bounded), `-N` (suffix), and `S-` (open ended)
                            // forms, resolving each to an inclusive (start, end) over the body.
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
                if let (true, Some((start, end))) = (supports_ranges, requested_range) {
                    let end = end.min(body.len().saturating_sub(1));
                    let start = start.min(end);
                    let content = &body[start..=end];
                    let extra_headers = extra_headers
                        .iter()
                        .map(|(name, value)| format!("{name}: {value}\r\n"))
                        .collect::<String>();
                    let total = if unknown_range_total {
                        "*".to_string()
                    } else {
                        body.len().to_string()
                    };
                    let response = format!(
                        "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{end}/{total}\r\nAccept-Ranges: bytes\r\n{extra_headers}Connection: close\r\n\r\n",
                        content.len(),
                    );
                    stream
                        .write_all(response.as_bytes())
                        .expect("write headers");
                    if !is_head {
                        stream.write_all(content).expect("write range body");
                    }
                } else {
                    let accept_ranges = if supports_ranges { "bytes" } else { "none" };
                    let extra_headers = extra_headers
                        .iter()
                        .map(|(name, value)| format!("{name}: {value}\r\n"))
                        .collect::<String>();
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: {accept_ranges}\r\n{extra_headers}Connection: close\r\n\r\n",
                        body.len()
                    );
                    stream
                        .write_all(response.as_bytes())
                        .expect("write headers");
                    if !is_head {
                        stream.write_all(body).expect("write body");
                    }
                }
            }
        });
        (format!("http://{addr}/demo.mcap"), request_count)
    }

    #[test]
    fn remote_errors_redact_query_strings() {
        let url = "http://127.0.0.1:1/demo.mcap?X-Amz-Signature=secret-token";
        let err = load_path(Path::new(url), super::SourceOptions::default())
            .expect_err("remote scan rejection should report redacted URL");
        assert!(!err.to_string().contains("secret-token"));
        assert!(!err.to_string().contains("X-Amz-Signature"));
    }

    #[test]
    fn remote_errors_redact_userinfo() {
        let url = "http://AKIA:secret@127.0.0.1:1/demo.mcap";
        let err = load_path(Path::new(url), super::SourceOptions::default())
            .expect_err("remote scan rejection should report redacted URL");
        assert!(!err.to_string().contains("AKIA"));
        assert!(!err.to_string().contains("secret"));
        assert!(err.to_string().contains("http://127.0.0.1:1/demo.mcap"));
    }

    #[test]
    fn remote_http_input_requires_remote_scan_opt_in() {
        let url = serve_http(b"hello remote", true);
        let err = load_path(Path::new(&url), super::SourceOptions::default())
            .expect_err("remote full read should require opt-in");
        assert!(err.to_string().contains("--allow-remote-scan"));
    }

    #[test]
    fn remote_object_store_input_requires_remote_scan_opt_in_before_network() {
        let err = load_path(
            Path::new("s3://bucket/demo.mcap?X-Amz-Signature=secret-token"),
            super::SourceOptions::default(),
        )
        .expect_err("cloud remote full read should require opt-in");
        assert!(err.to_string().contains("--allow-remote-scan"));
        assert!(!err.to_string().contains("secret-token"));
        assert!(!err.to_string().contains("X-Amz-Signature"));
    }

    #[test]
    fn remote_http_input_reads_entire_file() {
        let url = serve_http(b"hello remote", true);
        let input =
            load_path(Path::new(&url), super::SourceOptions::new(true)).expect("remote read");
        assert_eq!(input.as_slice(), b"hello remote");
    }

    #[test]
    fn remote_http_input_rejects_gzip_content_encoding() {
        let url = serve_http_with_headers(b"hello remote", false, &[("Content-Encoding", "gzip")]);
        let err = load_path(Path::new(&url), super::SourceOptions::new(true))
            .expect_err("gzip-encoded remote read should fail");
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
        let err =
            match super::try_open_remote_mcap(Path::new(&url), super::SourceOptions::default()) {
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
        let (url, _requests) = serve_http_with_options(body, true, &[], false, true, false);
        let err =
            match super::try_open_remote_mcap(Path::new(&url), super::SourceOptions::default()) {
                Ok(_) => panic!("unknown range total should surface as an error, not a bogus size"),
                Err(err) => err,
            };
        let message = format!("{err:#}");
        assert!(message.contains("Failed while fetching range from"));
    }

    #[test]
    fn remote_summary_read_requires_scan_for_oversized_summary_section() {
        let len = super::MAX_REMOTE_SUMMARY_BYTES_WITHOUT_SCAN
            + super::FOOTER_RECORD_AND_END_MAGIC_LEN
            + mcap::MAGIC.len()
            + 1;
        let mut body = vec![0u8; len];
        body[..mcap::MAGIC.len()].copy_from_slice(mcap::MAGIC);
        let footer_start = len - super::FOOTER_RECORD_AND_END_MAGIC_LEN;
        body[footer_start] = records::op::FOOTER;
        body[footer_start + 1..footer_start + 9].copy_from_slice(&20u64.to_le_bytes());
        body[footer_start + 9..footer_start + 17]
            .copy_from_slice(&(mcap::MAGIC.len() as u64).to_le_bytes());
        body[footer_start + 17..footer_start + 25].copy_from_slice(&0u64.to_le_bytes());
        body[footer_start + 25..footer_start + 29].copy_from_slice(&0u32.to_le_bytes());
        body[len - mcap::MAGIC.len()..].copy_from_slice(mcap::MAGIC);

        let url = serve_http(Box::leak(body.into_boxed_slice()), true);
        let err =
            match super::try_open_remote_mcap(Path::new(&url), super::SourceOptions::default()) {
                Ok(_) => panic!("oversized remote summary should require scan opt-in"),
                Err(err) => err,
            };
        let message = format!("{err:#}");
        assert!(message.contains("Remote summary section"));
        assert!(message.contains("--allow-remote-scan"));
    }

    #[test]
    fn remote_metadata_budget_requires_scan_for_oversized_total() {
        let err = super::require_remote_metadata_budget(
            super::MAX_REMOTE_METADATA_BYTES_WITHOUT_SCAN + 1,
            super::SourceOptions::default(),
            "metadata records",
        )
        .expect_err("oversized metadata range total should require scan opt-in");
        assert!(err.to_string().contains("metadata records"));
        assert!(err
            .to_string()
            .contains(&human_bytes(super::MAX_REMOTE_METADATA_BYTES_WITHOUT_SCAN)));
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
        let source =
            super::ObjectStoreSource::open(Path::new("https://example.com/demo.mcap?token=secret"))
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
    fn remote_mcap_summary_uses_range_reader() {
        let (buffer, channel_id) = summary_mcap_with_channel();
        let body: &'static [u8] = Box::leak(buffer.into_boxed_slice());
        let url = serve_http(body, true);
        let remote = super::try_open_remote_mcap(Path::new(&url), super::SourceOptions::default())
            .expect("remote summary read")
            .expect("summary should be present");

        assert!(remote.summary().channels.contains_key(&channel_id));
    }

    #[test]
    fn remote_summary_uses_single_http_request_when_tail_contains_summary() {
        // The whole point of the tail prefetch: when the summary fits in the tail,
        // summary discovery must take exactly one HTTP request (no probe/HEAD/footer).
        let (buffer, channel_id) = summary_mcap_with_channel();
        let body: &'static [u8] = Box::leak(buffer.into_boxed_slice());
        let (url, requests) = serve_http_counting(body, true);
        let remote = super::try_open_remote_mcap(Path::new(&url), super::SourceOptions::default())
            .expect("remote summary read")
            .expect("summary should be present");
        assert!(remote.summary().channels.contains_key(&channel_id));
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
        let summary = super::read_summary_from_remote(&mut reader, super::SourceOptions::default())
            .expect("summary read")
            .expect("summary should be present");
        assert!(summary.channels.contains_key(&channel_id));
    }

    #[test]
    fn remote_summary_backfills_prefix_when_tail_is_short() {
        // Simulate a summary larger than the prefetched tail: the tail starts after
        // `summary_start`, so discovery must issue one back-fill read for the prefix.
        let (buffer, channel_id) = summary_mcap_with_channel();
        let footer_start = buffer.len() - super::FOOTER_RECORD_AND_END_MAGIC_LEN;
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
        let summary = super::read_summary_from_remote(&mut reader, super::SourceOptions::default())
            .expect("summary read with back-fill")
            .expect("summary should be present");
        assert!(summary.channels.contains_key(&channel_id));
    }

    #[test]
    fn remote_mcap_summary_uses_range_get_when_head_is_rejected() {
        let (buffer, channel_id) = summary_mcap_with_channel();
        let body: &'static [u8] = Box::leak(buffer.into_boxed_slice());
        let (url, _requests) = serve_http_with_options(body, true, &[], true, false, false);
        let remote = super::try_open_remote_mcap(Path::new(&url), super::SourceOptions::default())
            .expect("remote summary should use range GET, not HEAD")
            .expect("summary should be present");

        assert!(remote.summary().channels.contains_key(&channel_id));
    }

    #[test]
    fn remote_summary_recovers_when_server_rejects_suffix_ranges() {
        // A server that honors bounded ranges but rejects suffix ranges must still
        // open without a scan: the suffix request fails, we fall back to a bounded
        // probe for the size, then read a bounded tail (suffix + probe + tail = 3).
        let (buffer, channel_id) = summary_mcap_with_channel();
        let body: &'static [u8] = Box::leak(buffer.into_boxed_slice());
        let (url, requests) = serve_http_bounded_only(body);
        let remote = super::try_open_remote_mcap(Path::new(&url), super::SourceOptions::default())
            .expect("bounded-only server should open without a scan")
            .expect("summary should be present");
        assert!(remote.summary().channels.contains_key(&channel_id));
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
