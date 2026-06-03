use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::Write as _;
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

pub const PLEASE_REDIRECT: &str =
    "Binary output can screw up your terminal. Supply -o or redirect to a file or pipe";
pub const PLEASE_SUPPLY_FILE: &str = "please supply a file. see --help for usage details.";
const FOOTER_RECORD_AND_END_MAGIC_LEN: usize = 37;
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SourceOptions {
    pub allow_remote_scan: bool,
}

impl SourceOptions {
    pub fn new(allow_remote_scan: bool) -> Self {
        Self { allow_remote_scan }
    }
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedSchema {
    pub header: records::SchemaHeader,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ParsedMcap {
    pub header: Option<records::Header>,
    pub statistics: Option<records::Statistics>,
    pub channels: std::collections::BTreeMap<u16, records::Channel>,
    pub schemas: std::collections::BTreeMap<u16, ParsedSchema>,
    pub chunk_indexes: Vec<records::ChunkIndex>,
    pub attachment_indexes: Vec<records::AttachmentIndex>,
    pub metadata_indexes: Vec<records::MetadataIndex>,
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
                if let Some(summary) =
                    read_summary_from_remote(&reader, options).with_context(|| {
                        format!(
                            "failed to read remote summary from {}",
                            redacted_display(path)
                        )
                    })?
                {
                    let header = read_header_from_seekable(&mut reader)?;
                    return Ok(parsed_mcap_from_summary_ref(header, &summary));
                }
                if !options.allow_remote_scan {
                    bail!(
                        "{}: remote file has no summary section; reading without one requires opt-in; {}",
                        redacted_display(path),
                        remote_scan_opt_in_suffix()
                    );
                }
            }
            None if !options.allow_remote_scan => {
                bail!(
                    "{}: remote server does not support range requests; {}",
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
            return Ok(parsed_mcap_from_summary_ref(header, &summary));
        }
    }

    let mcap = load_path(path, options)?;
    parse_mcap(&mcap)
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
    let Some(reader) = open_remote_range_reader(path)? else {
        if !options.allow_remote_scan {
            bail!(
                "{}: remote server does not support range requests; {}",
                redacted_display(path),
                remote_scan_opt_in_suffix()
            );
        }
        return Ok(None);
    };
    let Some(summary) = read_summary_from_remote(&reader, options).with_context(|| {
        format!(
            "failed to read remote summary from {}",
            redacted_display(path)
        )
    })?
    else {
        if !options.allow_remote_scan {
            bail!(
                "{}: remote file has no summary section; reading without one requires opt-in; {}",
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
    Http,
    ObjectStore,
}

impl RemoteUrlKind {
    fn from_scheme(scheme: &str) -> Option<Self> {
        match scheme.to_ascii_lowercase().as_str() {
            "http" | "https" => Some(Self::Http),
            "s3" | "s3a" | "gs" | "az" | "adl" | "azure" | "abfs" | "abfss" => {
                Some(Self::ObjectStore)
            }
            _ => None,
        }
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
            RemoteUrlKind::ObjectStore => object_store_options_from_env_vars(vars),
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

    fn head_size(&self) -> Result<u64> {
        Ok(self
            .runtime
            .block_on(self.store.head(&self.path))
            .with_context(|| format!("failed to stat {}", self.display_url))?
            .size)
    }

    fn probe_http_range_size(&self) -> Result<Option<u64>> {
        let response = match self.runtime.block_on(self.store.get_opts(
            &self.path,
            GetOptions {
                range: Some(GetRange::Bounded(0..1)),
                ..GetOptions::default()
            },
        )) {
            Ok(response) => response,
            Err(err) if remote_range_not_supported(&err) => return Ok(None),
            Err(err) => {
                return Err(err)
                    .with_context(|| format!("failed to fetch range from {}", self.display_url));
            }
        };
        validate_identity_content_encoding(&response.attributes, &self.display_url)?;
        // Relies on object_store parsing a numeric total from `Content-Range`
        // (for example `bytes 0-0/1234`); servers returning a `*` total are unsupported.
        Ok(Some(response.meta.size))
    }
}

pub struct RemoteRangeReader {
    source: ObjectStoreSource,
    size: u64,
    offset: u64,
}

impl RemoteRangeReader {
    fn open(path: &Path) -> Result<Option<Self>> {
        let remote_url = RemoteUrl::parse(path)?;
        let kind = remote_url.kind;
        let source = ObjectStoreSource::open_remote(remote_url)?;
        let Some(size) = (match kind {
            RemoteUrlKind::Http => source.probe_http_range_size()?,
            RemoteUrlKind::ObjectStore => Some(source.head_size()?),
        }) else {
            return Ok(None);
        };
        Ok(Some(Self {
            source,
            size,
            offset: 0,
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
            size,
            offset: 0,
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
        let response = self
            .source
            .runtime
            .block_on(self.source.store.get_opts(
                &self.source.path,
                GetOptions {
                    range: Some(GetRange::Bounded(offset..end)),
                    ..GetOptions::default()
                },
            ))
            .with_context(|| format!("failed to fetch range from {}", self.source.display_url))?;
        validate_identity_content_encoding(&response.attributes, &self.source.display_url)?;
        let bytes = self
            .source
            .runtime
            .block_on(response.bytes())
            .with_context(|| format!("failed to read range from {}", self.source.display_url))?;
        Ok(bytes.to_vec())
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
        let response = source
            .store
            .get(&source.path)
            .await
            .with_context(|| format!("failed to read remote input {}", source.display_url))?;
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
    reader: &RemoteRangeReader,
    options: SourceOptions,
) -> Result<Option<mcap::Summary>> {
    let file_size = reader.size();
    let tail_len = FOOTER_RECORD_AND_END_MAGIC_LEN as u64;
    if file_size < tail_len + mcap::MAGIC.len() as u64 {
        return Err(mcap::McapError::UnexpectedEof.into());
    }

    let footer_start = file_size - tail_len;
    let tail = reader.read_range(footer_start, FOOTER_RECORD_AND_END_MAGIC_LEN)?;
    if tail.len() != FOOTER_RECORD_AND_END_MAGIC_LEN {
        return Err(mcap::McapError::UnexpectedEof.into());
    }
    if tail[0] != records::op::FOOTER {
        return Err(mcap::McapError::BadFooter.into());
    }
    let record_len = u64::from_le_bytes(tail[1..9].try_into().expect("footer length slice"));
    if record_len != 20 {
        return Err(mcap::McapError::BadFooter.into());
    }
    if &tail[FOOTER_RECORD_AND_END_MAGIC_LEN - mcap::MAGIC.len()..] != mcap::MAGIC {
        return Err(mcap::McapError::BadMagic.into());
    }

    let mut cursor =
        std::io::Cursor::new(&tail[9..FOOTER_RECORD_AND_END_MAGIC_LEN - mcap::MAGIC.len()]);
    let footer = records::Footer::read_le(&mut cursor)?;
    if footer.summary_start == 0 {
        return Ok(None);
    }
    if footer.summary_start > footer_start {
        return Err(mcap::McapError::UnexpectedEof.into());
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
    let summary_bytes = reader.read_range(footer.summary_start, summary_len)?;
    if summary_bytes.len() != summary_len {
        return Err(mcap::McapError::UnexpectedEof.into());
    }
    Ok(Some(parse_summary_section(&summary_bytes)?))
}

// TODO: keep this in sync with mcap::sans_io::SummaryReader and mcap::read::ChannelAccumulator.
// A future mcap crate range-summary API should replace this CLI-local parser.
fn parse_summary_section(summary: &[u8]) -> Result<mcap::Summary> {
    let mut out = mcap::Summary::default();
    let mut schemas = HashMap::<u16, Arc<mcap::Schema<'static>>>::new();

    for record in mcap::read::LinearReader::sans_magic(summary) {
        match record? {
            Record::AttachmentIndex(index) => out.attachment_indexes.push(index),
            Record::MetadataIndex(index) => out.metadata_indexes.push(index),
            Record::Statistics(statistics) => out.stats = Some(statistics),
            Record::ChunkIndex(index) => out.chunk_indexes.push(index),
            Record::Schema { header, data } => {
                if header.id == 0 {
                    return Err(mcap::McapError::InvalidSchemaId.into());
                }
                let schema = Arc::new(mcap::Schema {
                    id: header.id,
                    name: header.name,
                    encoding: header.encoding,
                    data: Cow::Owned(data.into_owned()),
                });
                match schemas.entry(schema.id) {
                    std::collections::hash_map::Entry::Occupied(entry) => {
                        let existing = entry.get();
                        if existing.name != schema.name
                            || existing.encoding != schema.encoding
                            || existing.data.as_ref() != schema.data.as_ref()
                        {
                            return Err(
                                mcap::McapError::ConflictingSchemas(schema.name.clone()).into()
                            );
                        }
                    }
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        entry.insert(schema);
                    }
                }
            }
            Record::Channel(channel) => {
                let schema = if channel.schema_id == 0 {
                    None
                } else {
                    Some(schemas.get(&channel.schema_id).cloned().ok_or_else(|| {
                        mcap::McapError::UnknownSchema(channel.topic.clone(), channel.schema_id)
                    })?)
                };
                let resolved = Arc::new(mcap::Channel {
                    id: channel.id,
                    topic: channel.topic,
                    schema,
                    message_encoding: channel.message_encoding,
                    metadata: channel.metadata,
                });
                match out.channels.entry(resolved.id) {
                    std::collections::hash_map::Entry::Occupied(entry) => {
                        let existing = entry.get();
                        if existing.topic != resolved.topic
                            || existing.schema.as_ref().map(|schema| schema.id)
                                != resolved.schema.as_ref().map(|schema| schema.id)
                            || existing.message_encoding != resolved.message_encoding
                            || existing.metadata != resolved.metadata
                        {
                            return Err(mcap::McapError::ConflictingChannels(
                                resolved.topic.clone(),
                            )
                            .into());
                        }
                    }
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        entry.insert(resolved);
                    }
                }
            }
            _ => {}
        }
    }
    out.schemas = schemas;
    Ok(out)
}

// TODO: keep these exact-record parsers in sync with mcap::read::metadata and
// mcap::read::attachment. They duplicate the mcap crate helpers so remote range callers can
// parse owned records without holding a full-file byte slice alive.
pub(crate) fn parse_metadata_record(bytes: &[u8]) -> Result<mcap::records::Metadata> {
    let mut reader = mcap::read::LinearReader::sans_magic(bytes);
    let metadata = match reader.next().ok_or(mcap::McapError::BadIndex)?? {
        mcap::records::Record::Metadata(metadata) => metadata,
        _ => return Err(mcap::McapError::BadIndex.into()),
    };
    if reader.next().is_some() {
        return Err(mcap::McapError::BadIndex.into());
    }
    Ok(metadata)
}

pub(crate) fn parse_attachment_record(bytes: &[u8]) -> Result<mcap::Attachment<'static>> {
    let mut reader = mcap::read::LinearReader::sans_magic(bytes);
    let attachment = match reader.next().ok_or(mcap::McapError::BadIndex)?? {
        mcap::records::Record::Attachment { header, data, .. } => mcap::Attachment {
            log_time: header.log_time,
            create_time: header.create_time,
            name: header.name,
            media_type: header.media_type,
            data: Cow::Owned(data.into_owned()),
        },
        _ => return Err(mcap::McapError::BadIndex.into()),
    };
    if reader.next().is_some() {
        return Err(mcap::McapError::BadIndex.into());
    }
    Ok(attachment)
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

fn parsed_mcap_from_summary_ref(
    header: Option<records::Header>,
    summary: &mcap::Summary,
) -> ParsedMcap {
    let mut out = ParsedMcap {
        header,
        statistics: summary.stats.clone(),
        channels: std::collections::BTreeMap::new(),
        schemas: std::collections::BTreeMap::new(),
        chunk_indexes: summary.chunk_indexes.clone(),
        attachment_indexes: summary.attachment_indexes.clone(),
        metadata_indexes: summary.metadata_indexes.clone(),
    };

    for schema in summary.schemas.values() {
        let schema = schema.as_ref();
        out.schemas.insert(
            schema.id,
            ParsedSchema {
                header: records::SchemaHeader {
                    id: schema.id,
                    name: schema.name.clone(),
                    encoding: schema.encoding.clone(),
                },
                data: schema.data.clone().into_owned(),
            },
        );
    }

    for channel in summary.channels.values() {
        let channel = channel.as_ref();
        out.channels.insert(
            channel.id,
            records::Channel {
                id: channel.id,
                schema_id: channel.schema.as_ref().map(|schema| schema.id).unwrap_or(0),
                topic: channel.topic.clone(),
                message_encoding: channel.message_encoding.clone(),
                metadata: channel.metadata.clone(),
            },
        );
    }

    out
}

pub fn parse_mcap(mcap: &[u8]) -> Result<ParsedMcap> {
    let header = read_header(mcap)?;
    if let Some(parsed_from_summary) = parse_mcap_from_summary(mcap, header.clone())? {
        return Ok(parsed_from_summary);
    }

    eprintln!(
        "Warning: summary section not available; full scan may be slow. Run `mcap doctor` for details."
    );
    parse_mcap_linear(mcap, header)
}

pub(crate) fn read_header(mcap: &[u8]) -> Result<Option<records::Header>> {
    let mut reader = mcap::read::LinearReader::new(mcap)?;
    match reader.next() {
        Some(Ok(Record::Header(header))) => Ok(Some(header)),
        Some(Ok(_)) | None => Ok(None),
        Some(Err(err)) => Err(err.into()),
    }
}

fn parse_mcap_from_summary(
    mcap: &[u8],
    header: Option<records::Header>,
) -> Result<Option<ParsedMcap>> {
    let Some(summary) = mcap::Summary::read(mcap)? else {
        return Ok(None);
    };
    Ok(Some(parsed_mcap_from_summary_ref(header, &summary)))
}

fn parse_mcap_linear(mcap: &[u8], header: Option<records::Header>) -> Result<ParsedMcap> {
    let mut out = ParsedMcap {
        header,
        ..ParsedMcap::default()
    };
    for record in mcap::read::LinearReader::new(mcap)? {
        let record = record?;
        if let Record::Chunk { header, data } = record {
            for nested_record in mcap::read::ChunkReader::new(header, data.as_ref())? {
                collect_record(&mut out, nested_record?)?;
            }
        } else {
            collect_record(&mut out, record)?;
        }
    }

    Ok(out)
}

fn collect_record(out: &mut ParsedMcap, record: Record<'_>) -> Result<()> {
    match record {
        Record::Header(header) => {
            if let Some(existing) = &out.header {
                if existing != &header {
                    bail!("conflicting MCAP header records");
                }
            } else {
                out.header = Some(header);
            }
        }
        Record::Statistics(statistics) => {
            out.statistics = Some(statistics);
        }
        Record::Channel(channel) => {
            if let Some(existing) = out.channels.get(&channel.id) {
                if existing != &channel {
                    bail!("conflicting channel definition for id {}", channel.id);
                }
            } else {
                out.channels.insert(channel.id, channel);
            }
        }
        Record::Schema { header, data } => {
            let schema = ParsedSchema {
                header,
                data: data.into_owned(),
            };
            if let Some(existing) = out.schemas.get(&schema.header.id) {
                if existing != &schema {
                    bail!("conflicting schema definition for id {}", schema.header.id);
                }
            } else {
                out.schemas.insert(schema.header.id, schema);
            }
        }
        Record::ChunkIndex(index) => out.chunk_indexes.push(index),
        Record::AttachmentIndex(index) => out.attachment_indexes.push(index),
        Record::MetadataIndex(index) => out.metadata_indexes.push(index),
        _ => {}
    }
    Ok(())
}

pub fn decimal_time(t: u64) -> String {
    format!("{}.{:09}", t / 1_000_000_000, t % 1_000_000_000)
}

pub fn raw_time(t: u64) -> String {
    t.to_string()
}

pub fn write_raw_time(writer: &mut impl std::io::Write, t: u64) -> std::io::Result<()> {
    write!(writer, "{t}")
}

pub fn formatted_time(t: u64) -> String {
    let seconds = (t / 1_000_000_000) as i64;
    let nanos = (t % 1_000_000_000) as u32;
    match chrono::DateTime::from_timestamp(seconds, nanos) {
        Some(dt) => format!("{} ({})", format_rfc3339_trimmed(dt), decimal_time(t)),
        None => decimal_time(t),
    }
}

pub fn human_bytes(num_bytes: u64) -> String {
    let prefixes = ["B", "KiB", "MiB", "GiB"];
    for (index, prefix) in prefixes.iter().enumerate() {
        let displayed = num_bytes as f64 / 1024f64.powi(index as i32);
        if displayed <= 1024.0 {
            return format!("{displayed:.2} {prefix}");
        }
    }

    let last = prefixes.len() - 1;
    let displayed = num_bytes as f64 / 1024f64.powi(last as i32);
    format!("{displayed:.2} {}", prefixes[last])
}

pub fn parse_output_compression(value: &str) -> Result<Option<mcap::Compression>> {
    match value {
        "zstd" => Ok(Some(mcap::Compression::Zstd)),
        "lz4" => Ok(Some(mcap::Compression::Lz4)),
        "none" | "" => Ok(None),
        _ => bail!(
            "unrecognized compression format '{value}': valid options are 'lz4', 'zstd', or 'none'"
        ),
    }
}

fn format_rfc3339_trimmed(dt: chrono::DateTime<chrono::Utc>) -> String {
    let rendered = dt.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
    let Some(without_z) = rendered.strip_suffix('Z') else {
        return rendered;
    };

    let Some((prefix, fractional)) = without_z.split_once('.') else {
        return rendered;
    };

    let trimmed = fractional.trim_end_matches('0');
    if trimmed.is_empty() {
        format!("{prefix}Z")
    } else {
        format!("{prefix}.{trimmed}Z")
    }
}

pub fn format_table(rows: &[Vec<String>]) -> String {
    if rows.is_empty() {
        return String::new();
    }

    let mut widths = vec![0usize; rows[0].len()];
    for row in rows {
        for (idx, value) in row.iter().enumerate() {
            widths[idx] = widths[idx].max(value.chars().count());
        }
    }

    let mut out = String::new();
    let last_col_idx = rows[0].len().saturating_sub(1);
    for row in rows {
        let mut line = String::new();
        for (idx, value) in row.iter().enumerate() {
            if idx > 0 {
                line.push('\t');
            }
            if idx == last_col_idx {
                line.push_str(value);
            } else {
                let width = widths[idx];
                let _ = write!(&mut line, "{value:<width$}");
            }
        }
        let _ = writeln!(&mut out, "{line}");
    }
    out
}

pub fn print_table(rows: &[Vec<String>]) {
    let rendered = format_table(rows);
    if rendered.is_empty() {
        return;
    }
    print!("{rendered}");
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::net::TcpListener;
    use std::path::Path;
    use std::sync::Arc;
    use std::thread;

    use super::{
        decimal_time, format_table, formatted_time, human_bytes, load_path, parse_mcap,
        parse_mcap_from_summary, print_table, write_raw_time,
    };
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
        serve_http_with_options(body, supports_ranges, extra_headers, false)
    }

    fn serve_http_with_options(
        body: &'static [u8],
        supports_ranges: bool,
        extra_headers: &'static [(&'static str, &'static str)],
        reject_head: bool,
    ) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
        let addr = listener.local_addr().expect("test server addr");
        thread::spawn(move || {
            for stream in listener.incoming().take(64) {
                let mut stream = stream.expect("accept test connection");
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
                let requested_range = request
                    .lines()
                    .find_map(|line| line.strip_prefix("Range: bytes="))
                    .or_else(|| {
                        request
                            .lines()
                            .find_map(|line| line.strip_prefix("range: bytes="))
                    })
                    .and_then(|range| range.split_once('-'))
                    .and_then(|(start, end)| {
                        Some((start.parse::<usize>().ok()?, end.parse::<usize>().ok()?))
                    });
                if let (true, Some((start, end))) = (supports_ranges, requested_range) {
                    let end = end.min(body.len().saturating_sub(1));
                    let start = start.min(end);
                    let content = &body[start..=end];
                    let extra_headers = extra_headers
                        .iter()
                        .map(|(name, value)| format!("{name}: {value}\r\n"))
                        .collect::<String>();
                    let response = format!(
                        "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{end}/{}\r\nAccept-Ranges: bytes\r\n{extra_headers}Connection: close\r\n\r\n",
                        content.len(),
                        body.len(),
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
        format!("http://{addr}/demo.mcap")
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
        assert!(message.contains("remote summary section"));
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
        assert!(err.to_string().contains(&super::human_bytes(
            super::MAX_REMOTE_METADATA_BYTES_WITHOUT_SCAN
        )));
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
    fn remote_mcap_summary_uses_range_get_when_head_is_rejected() {
        let (buffer, channel_id) = summary_mcap_with_channel();
        let body: &'static [u8] = Box::leak(buffer.into_boxed_slice());
        let url = serve_http_with_options(body, true, &[], true);
        let remote = super::try_open_remote_mcap(Path::new(&url), super::SourceOptions::default())
            .expect("remote summary should use range GET, not HEAD")
            .expect("summary should be present");

        assert!(remote.summary().channels.contains_key(&channel_id));
    }

    #[test]
    fn remote_mcap_without_range_support_requires_scan_opt_in() {
        let (buffer, _) = summary_mcap_with_channel();
        let body: &'static [u8] = Box::leak(buffer.into_boxed_slice());
        let url = serve_http(body, false);
        let err = super::parse_mcap_from_path(Path::new(&url), super::SourceOptions::default())
            .expect_err("non-range HTTP input should require scan opt-in");
        let message = err.to_string();
        assert!(message.contains("remote server does not support range requests"));
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

    #[test]
    fn table_printer_handles_empty_input() {
        print_table(&[]);
        assert!(format_table(&[]).is_empty());
    }

    #[test]
    fn table_formatter_aligns_columns() {
        let rows = vec![
            vec!["id".to_string(), "topic".to_string()],
            vec!["7".to_string(), "/foo".to_string()],
            vec!["12".to_string(), "/barbaz".to_string()],
        ];
        let rendered = format_table(&rows);
        let lines: Vec<&str> = rendered.lines().collect();
        assert_eq!(lines.len(), 3);
        assert!(lines[0].starts_with("id"));
        assert!(lines[1].contains('\t'));
        assert!(lines[2].contains("/barbaz"));
    }

    #[test]
    fn formatted_time_includes_rfc3339_and_decimal() {
        assert_eq!(
            formatted_time(1_000_000_000),
            "1970-01-01T00:00:01Z (1.000000000)"
        );
        assert_eq!(decimal_time(1_234_567_890), "1.234567890");
        assert_eq!(
            formatted_time(1_234_567_890),
            "1970-01-01T00:00:01.23456789Z (1.234567890)"
        );
    }

    #[test]
    fn raw_time_is_unformatted_nanoseconds() {
        assert_eq!(super::raw_time(1_234_567_890), "1234567890");
    }

    #[test]
    fn write_raw_time_writes_unformatted_nanoseconds() {
        let mut out = Vec::new();
        write_raw_time(&mut out, 1_234_567_890).expect("should write raw time");
        assert_eq!(
            String::from_utf8(out).expect("raw time output should be utf8"),
            "1234567890"
        );
    }

    #[test]
    fn table_formatter_omits_trailing_whitespace() {
        let rows = vec![
            vec!["col1".to_string(), "col2".to_string()],
            vec!["a".to_string(), "b".to_string()],
        ];
        let rendered = format_table(&rows);
        for line in rendered.lines() {
            assert!(!line.ends_with(' '));
            assert!(!line.ends_with('\t'));
        }
    }

    #[test]
    fn human_bytes_scales_units() {
        assert_eq!(human_bytes(2), "2.00 B");
        assert_eq!(human_bytes(2 * 1024), "2.00 KiB");
    }

    #[test]
    fn parse_output_compression_supports_known_values() {
        assert!(matches!(
            super::parse_output_compression("zstd").expect("zstd should parse"),
            Some(mcap::Compression::Zstd)
        ));
        assert!(matches!(
            super::parse_output_compression("lz4").expect("lz4 should parse"),
            Some(mcap::Compression::Lz4)
        ));
        assert!(super::parse_output_compression("none")
            .expect("none should parse")
            .is_none());
        assert!(super::parse_output_compression("")
            .expect("empty should parse")
            .is_none());
    }

    #[test]
    fn parse_output_compression_rejects_unknown_values() {
        let err =
            super::parse_output_compression("snappy").expect_err("unknown compression should fail");
        assert!(err
            .to_string()
            .contains("unrecognized compression format 'snappy'"));
    }

    #[test]
    fn parse_mcap_collects_channels_and_schemas() {
        let mut buffer = Vec::new();
        let (schema_id, channel_id) = {
            let mut writer = mcap::Writer::new(std::io::Cursor::new(&mut buffer)).expect("writer");
            let schema_id = writer
                .add_schema("demo_schema", "jsonschema", br#"{"type":"object"}"#)
                .expect("schema");
            let channel_id = writer
                .add_channel(schema_id, "/demo", "json", &BTreeMap::new())
                .expect("channel");
            writer
                .write_to_known_channel(
                    &records::MessageHeader {
                        channel_id,
                        sequence: 1,
                        log_time: 10,
                        publish_time: 11,
                    },
                    br#"{"k":"v"}"#,
                )
                .expect("write message");
            writer.finish().expect("finish writer");
            (schema_id, channel_id)
        };

        let parsed = parse_mcap(&buffer).expect("parse mcap");
        assert!(parsed.header.is_some());
        assert!(parsed.channels.contains_key(&channel_id));
        assert!(parsed.schemas.contains_key(&schema_id));
    }

    #[test]
    fn parse_mcap_falls_back_for_summaryless_files() {
        let mut buffer = Vec::new();
        let (schema_id, channel_id) = {
            let mut writer = mcap::WriteOptions::new()
                .emit_summary_records(false)
                .emit_summary_offsets(false)
                .create(std::io::Cursor::new(&mut buffer))
                .expect("writer");
            let schema_id = writer
                .add_schema("demo_schema", "jsonschema", br#"{"type":"object"}"#)
                .expect("schema");
            let channel_id = writer
                .add_channel(schema_id, "/demo", "json", &BTreeMap::new())
                .expect("channel");
            writer
                .write_to_known_channel(
                    &records::MessageHeader {
                        channel_id,
                        sequence: 1,
                        log_time: 10,
                        publish_time: 11,
                    },
                    br#"{"k":"v"}"#,
                )
                .expect("write message");
            writer.finish().expect("finish writer");
            (schema_id, channel_id)
        };

        let parsed = parse_mcap(&buffer).expect("parse mcap");
        assert!(parsed.header.is_some());
        assert!(parsed.channels.contains_key(&channel_id));
        assert!(parsed.schemas.contains_key(&schema_id));
    }

    #[test]
    fn parse_mcap_from_summary_accepts_empty_summary() {
        let mut buffer = Vec::new();
        {
            let mut writer = mcap::Writer::new(std::io::Cursor::new(&mut buffer)).expect("writer");
            writer.finish().expect("finish writer");
        }

        let parsed = parse_mcap_from_summary(&buffer, None).expect("parse from summary");
        assert!(parsed.is_some());
        let parsed = parsed.expect("parsed summary output");
        if let Some(stats) = &parsed.statistics {
            assert_eq!(stats.message_count, 0);
            assert_eq!(stats.channel_count, 0);
            assert_eq!(stats.attachment_count, 0);
            assert_eq!(stats.metadata_count, 0);
        }
        assert!(parsed.channels.is_empty());
        assert!(parsed.schemas.is_empty());
        assert!(parsed.chunk_indexes.is_empty());
        assert!(parsed.attachment_indexes.is_empty());
        assert!(parsed.metadata_indexes.is_empty());
    }
}
