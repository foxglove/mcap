//! Random-access and sequential byte sources for sans-io MCAP reads.
//!
//! Local files use seek+read (no mmap). Remote URLs prefer HTTP range requests.
//! Stdin is streamed when opened with [`AccessMode::Sequential`], or spooled to a
//! tempfile when opened with [`AccessMode::Random`] (needed for summary / multi-pass).

mod drivers;

pub use drivers::{for_each_linear_record, read_header, read_summary, service_indexed_chunk};

use std::fs::File;
use std::io::{IsTerminal as _, Read, Seek as _, SeekFrom, Write as _};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use tempfile::NamedTempFile;

use crate::source::{
    is_remote_url, open_remote_range_reader, read_remote_input_to_writer, redacted_display,
    remote_or_local_extension, remote_scan_opt_in_suffix, require_remote_scan_allowed,
    RemoteRangeReader, SourceOptions, PLEASE_SUPPLY_FILE,
};

/// How the caller will access bytes from the source.
///
/// Call sites know this up front:
/// - Summary / indexed chunk fetches need [`AccessMode::Random`] (remote: HTTP ranges).
/// - A single forward [`LinearReader`] pass can use [`AccessMode::Sequential`]: stdin streams
///   without a tempfile, and remotes materialize once (avoids many tiny range GETs).
/// - Multi-pass linear rewrite still opens [`AccessMode::Random`] for the indexed attempt, then
///   spools a remote source before the linear passes (see rewrite helpers).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AccessMode {
    /// May seek and re-read (summary, indexes, second passes). Stdin is spooled.
    /// Remotes keep range requests when the server supports them.
    #[default]
    Random,
    /// One forward pass from offset 0. Stdin streams without a tempfile.
    /// Remotes always spool to a local tempfile (one transfer).
    Sequential,
}

/// Byte-oriented input with optional random access.
pub trait ByteSource {
    /// File size in bytes, or `None` when unknown (streaming stdin).
    fn size(&self) -> Result<Option<u64>>;
    fn is_remote(&self) -> bool;
    fn display_name(&self) -> String;
    /// Whether random access is available (`false` for pure stdin streams).
    fn is_seekable(&self) -> bool;

    /// Read up to `dest.len()` bytes at `offset` into `dest`.
    ///
    /// Returns the number of bytes read (0 at EOF). Prefer this over [`Self::read_at`] in
    /// sans-io event loops so the reader can fill its insert buffer without an intermediate
    /// allocation.
    fn read_into(&mut self, offset: u64, dest: &mut [u8]) -> Result<usize>;

    /// Read `[offset, offset+len)` into a new buffer. `len` may be clamped to EOF.
    ///
    /// Hot paths should call [`Self::read_into`] instead to reuse caller buffers.
    fn read_at(&mut self, offset: u64, len: usize) -> Result<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let mut buf = vec![0u8; len];
        let n = self.read_into(offset, &mut buf)?;
        buf.truncate(n);
        Ok(buf)
    }
}

/// Local file opened for seek+read (not memory-mapped).
///
/// Small [`ByteSource::read_into`] calls are served from a readahead window so sans-io
/// `ReadRequest`s of a few bytes do not each become an `lseek`+`read` syscall. Large reads
/// bypass the window and go straight into the caller buffer.
pub struct LocalFileSource {
    file: File,
    path: PathBuf,
    size: u64,
    // Keeps a spool tempfile alive for stdin / non-range remote fallbacks.
    _temp_file: Option<NamedTempFile>,
    /// File offset of `cache[0]`.
    cache_start: u64,
    /// Valid readahead bytes; empty means no cached window.
    cache: Vec<u8>,
    /// Known OS file cursor after the last seek/read, used to skip redundant seeks.
    file_pos: Option<u64>,
}

/// Bytes pulled from disk on a cache miss. Sized to absorb many tiny LinearReader requests.
const LOCAL_READAHEAD: usize = 256 * 1024;

impl LocalFileSource {
    pub fn open_path(path: &Path) -> Result<Self> {
        let file =
            File::open(path).with_context(|| format!("couldn't open '{}'", path.display()))?;
        let size = file
            .metadata()
            .with_context(|| format!("couldn't stat '{}'", path.display()))?
            .len();
        Ok(Self {
            file,
            path: path.to_path_buf(),
            size,
            _temp_file: None,
            cache_start: 0,
            cache: Vec::new(),
            file_pos: None,
        })
    }

    fn from_temp_file(mut temp_file: NamedTempFile, display_path: PathBuf) -> Result<Self> {
        temp_file
            .as_file_mut()
            .flush()
            .context("failed to flush temporary input file")?;
        let file = temp_file
            .reopen()
            .context("failed to reopen temporary input file")?;
        let size = file
            .metadata()
            .context("failed to stat temporary input file")?
            .len();
        Ok(Self {
            file,
            path: display_path,
            size,
            _temp_file: Some(temp_file),
            cache_start: 0,
            cache: Vec::new(),
            file_pos: None,
        })
    }

    fn cache_end(&self) -> u64 {
        self.cache_start.saturating_add(self.cache.len() as u64)
    }

    fn seek_to(&mut self, offset: u64) -> Result<()> {
        if self.file_pos == Some(offset) {
            return Ok(());
        }
        self.file
            .seek(SeekFrom::Start(offset))
            .context("failed to seek in local file")?;
        self.file_pos = Some(offset);
        Ok(())
    }

    fn fill_cache_at(&mut self, offset: u64) -> Result<()> {
        if offset >= self.size {
            self.cache.clear();
            self.cache_start = offset;
            return Ok(());
        }
        let available = (self.size - offset) as usize;
        let to_read = LOCAL_READAHEAD.min(available);
        self.seek_to(offset)?;
        self.cache.resize(to_read, 0);
        self.file
            .read_exact(&mut self.cache)
            .context("failed to read from local file")?;
        self.cache_start = offset;
        self.file_pos = Some(offset.saturating_add(to_read as u64));
        Ok(())
    }

    fn read_direct_into(&mut self, offset: u64, dest: &mut [u8]) -> Result<usize> {
        if dest.is_empty() || offset >= self.size {
            return Ok(0);
        }
        let available = (self.size - offset) as usize;
        let to_read = dest.len().min(available);
        self.seek_to(offset)?;
        self.file
            .read_exact(&mut dest[..to_read])
            .context("failed to read from local file")?;
        self.file_pos = Some(offset.saturating_add(to_read as u64));
        // Direct reads invalidate the window — a later random jump must not see stale bytes.
        self.cache.clear();
        Ok(to_read)
    }
}

impl ByteSource for LocalFileSource {
    fn size(&self) -> Result<Option<u64>> {
        Ok(Some(self.size))
    }

    fn is_remote(&self) -> bool {
        false
    }

    fn display_name(&self) -> String {
        self.path.display().to_string()
    }

    fn is_seekable(&self) -> bool {
        true
    }

    fn read_into(&mut self, offset: u64, dest: &mut [u8]) -> Result<usize> {
        if dest.is_empty() || offset >= self.size {
            return Ok(0);
        }
        let total = dest.len().min((self.size - offset) as usize);
        // Large requests skip the readahead window (chunk payloads, summary sections).
        if total >= LOCAL_READAHEAD {
            return self.read_direct_into(offset, &mut dest[..total]);
        }

        let mut filled = 0usize;
        while filled < total {
            let at = offset.saturating_add(filled as u64);
            if at < self.cache_start || at >= self.cache_end() {
                self.fill_cache_at(at)?;
                if self.cache.is_empty() {
                    break;
                }
            }
            let cache_off = (at - self.cache_start) as usize;
            let n = (total - filled).min(self.cache.len() - cache_off);
            dest[filled..filled + n].copy_from_slice(&self.cache[cache_off..cache_off + n]);
            filled += n;
        }
        Ok(filled)
    }
}

/// Remote object that supports byte-range reads.
pub struct RemoteRangeSource {
    inner: RemoteRangeReader,
}

impl RemoteRangeSource {
    pub(crate) fn new(inner: RemoteRangeReader) -> Self {
        Self { inner }
    }
}

impl ByteSource for RemoteRangeSource {
    fn size(&self) -> Result<Option<u64>> {
        Ok(Some(self.inner.size()))
    }

    fn is_remote(&self) -> bool {
        true
    }

    fn display_name(&self) -> String {
        self.inner.display_url().to_string()
    }

    fn is_seekable(&self) -> bool {
        true
    }

    fn read_at(&mut self, offset: u64, len: usize) -> Result<Vec<u8>> {
        // Avoid the trait-default path (zeroed Vec + copy of read_range's buffer).
        self.inner.read_range(offset, len)
    }

    fn read_into(&mut self, offset: u64, dest: &mut [u8]) -> Result<usize> {
        let data = self.inner.read_range(offset, dest.len())?;
        let n = data.len();
        dest[..n].copy_from_slice(&data);
        Ok(n)
    }
}

/// Forward-only byte source (stdin pipes, or any [`Read`] stream).
///
/// [`ByteSource::read_into`] accepts only `offset ==` the current cursor. Seeking backwards
/// or skipping ahead is an error — callers that need that must open with [`AccessMode::Random`].
pub struct SequentialSource {
    reader: Box<dyn Read>,
    cursor: u64,
    name: String,
}

impl SequentialSource {
    pub fn new(reader: impl Read + 'static, name: impl Into<String>) -> Self {
        Self {
            reader: Box::new(reader),
            cursor: 0,
            name: name.into(),
        }
    }

    fn open_stdin() -> Result<Self> {
        let stdin = std::io::stdin();
        if stdin.is_terminal() {
            bail!("{PLEASE_SUPPLY_FILE}");
        }
        // BufReader coalesces the many tiny LinearReader ReadRequests on a pipe.
        Ok(Self::new(
            std::io::BufReader::with_capacity(LOCAL_READAHEAD, stdin),
            "<stdin>",
        ))
    }
}

impl ByteSource for SequentialSource {
    fn size(&self) -> Result<Option<u64>> {
        Ok(None)
    }

    fn is_remote(&self) -> bool {
        false
    }

    fn display_name(&self) -> String {
        self.name.clone()
    }

    fn is_seekable(&self) -> bool {
        false
    }

    fn read_into(&mut self, offset: u64, dest: &mut [u8]) -> Result<usize> {
        if offset != self.cursor {
            bail!(
                "{} only supports sequential reads (requested offset {offset}, cursor {})",
                self.name,
                self.cursor
            );
        }
        let n = self
            .reader
            .read(dest)
            .with_context(|| format!("failed to read from {}", self.name))?;
        self.cursor = self.cursor.saturating_add(n as u64);
        Ok(n)
    }
}

/// In-memory bytes, mainly for unit tests.
#[cfg(test)]
pub struct MemorySource {
    data: Vec<u8>,
}

#[cfg(test)]
impl MemorySource {
    pub fn new(data: impl Into<Vec<u8>>) -> Self {
        Self { data: data.into() }
    }
}

#[cfg(test)]
impl ByteSource for MemorySource {
    fn size(&self) -> Result<Option<u64>> {
        Ok(Some(self.data.len() as u64))
    }

    fn is_remote(&self) -> bool {
        false
    }

    fn display_name(&self) -> String {
        "<memory>".to_string()
    }

    fn is_seekable(&self) -> bool {
        true
    }

    fn read_into(&mut self, offset: u64, dest: &mut [u8]) -> Result<usize> {
        if dest.is_empty() || offset as usize >= self.data.len() {
            return Ok(0);
        }
        let start = offset as usize;
        let end = start.saturating_add(dest.len()).min(self.data.len());
        let n = end - start;
        dest[..n].copy_from_slice(&self.data[start..end]);
        Ok(n)
    }
}

/// Open a path, remote URL, or stdin as a [`ByteSource`] with [`AccessMode::Random`].
///
/// Prefer [`open_byte_source_with_access`] when the command only needs a linear scan so stdin
/// can stream without spooling.
pub fn open_byte_source(
    path: Option<&Path>,
    options: SourceOptions,
) -> Result<Box<dyn ByteSource>> {
    open_byte_source_with_access(path, options, AccessMode::Random)
}

/// Open a path, remote URL, or stdin with an explicit access mode.
///
/// - [`AccessMode::Random`] + stdin → spool to a tempfile (seek+read).
/// - [`AccessMode::Sequential`] + stdin → stream via [`SequentialSource`] (no tempfile).
/// - [`AccessMode::Random`] + remote → HTTP range requests when available; otherwise
///   `--allow-remote-scan` and spool.
/// - [`AccessMode::Sequential`] + remote → always spool (one streaming GET), requiring
///   `--allow-remote-scan`.
/// - Local paths always open as seekable files.
pub fn open_byte_source_with_access(
    path: Option<&Path>,
    options: SourceOptions,
    access: AccessMode,
) -> Result<Box<dyn ByteSource>> {
    let Some(path) = path else {
        return match access {
            AccessMode::Random => Ok(Box::new(spool_stdin_to_local()?)),
            AccessMode::Sequential => Ok(Box::new(SequentialSource::open_stdin()?)),
        };
    };

    if is_remote_url(path) {
        return open_remote_byte_source(path, options, access);
    }

    Ok(Box::new(LocalFileSource::open_path(path)?))
}

fn open_remote_byte_source(
    path: &Path,
    options: SourceOptions,
    access: AccessMode,
) -> Result<Box<dyn ByteSource>> {
    // Sequential remote work materializes once. Range-reading a LinearReader issues one HTTP
    // request per tiny field read; a single streaming GET into a tempfile is cheaper and matches
    // main's remote policy for recover / non-indexed scans.
    if matches!(access, AccessMode::Sequential) {
        return Ok(Box::new(spool_remote_to_local(path, options)?));
    }

    match open_remote_range_reader(path)? {
        Some(reader) => Ok(Box::new(RemoteRangeSource::new(reader))),
        None if !options.allow_remote_scan => {
            bail!(
                "failed to read {}\nRemote server does not support range requests; {}",
                redacted_display(path),
                remote_scan_opt_in_suffix()
            );
        }
        None => Ok(Box::new(spool_remote_to_local(path, options)?)),
    }
}

/// Spool a remote object to a local tempfile via one streaming download.
pub(crate) fn spool_remote_to_local(
    path: &Path,
    options: SourceOptions,
) -> Result<LocalFileSource> {
    require_remote_scan_allowed(path, options)?;
    let suffix = remote_or_local_extension(path)
        .filter(|extension| !extension.is_empty())
        .map(|extension| format!(".{extension}"));
    let mut builder = tempfile::Builder::new();
    builder.prefix("mcap-cli-remote-bytesource-");
    if let Some(suffix) = suffix.as_deref() {
        builder.suffix(suffix);
    }
    let mut temp_file = builder
        .tempfile()
        .context("failed to create temporary remote input file")?;
    read_remote_input_to_writer(path, temp_file.as_file_mut())?;
    LocalFileSource::from_temp_file(temp_file, PathBuf::from(redacted_display(path)))
}

/// Copy an already-open remote [`ByteSource`] into a local tempfile (one full read).
///
/// Used when a multi-pass linear path discovers it has a remote range source and must avoid
/// re-transferring the object on each pass. Prefer [`spool_remote_to_local`] when the URL is still
/// available so the download can be a single streaming GET.
pub(crate) fn spool_from_byte_source(source: &mut dyn ByteSource) -> Result<LocalFileSource> {
    let size = source
        .size()?
        .context("cannot spool a remote source with unknown size")?;
    let mut temp_file = tempfile::Builder::new()
        .prefix("mcap-cli-remote-spool-")
        .tempfile()
        .context("failed to create temporary file for remote spool")?;
    let mut offset = 0u64;
    let mut buf = vec![0u8; 1024 * 1024];
    while offset < size {
        let want = ((size - offset) as usize).min(buf.len());
        let n = source.read_into(offset, &mut buf[..want])?;
        if n == 0 {
            bail!(
                "remote spool hit EOF at offset {offset} before expected size {size} ({})",
                source.display_name()
            );
        }
        temp_file
            .as_file_mut()
            .write_all(&buf[..n])
            .context("failed to write remote spool tempfile")?;
        offset = offset.saturating_add(n as u64);
    }
    LocalFileSource::from_temp_file(temp_file, PathBuf::from(source.display_name()))
}

fn spool_stdin_to_local() -> Result<LocalFileSource> {
    let stdin = std::io::stdin();
    if stdin.is_terminal() {
        bail!("{PLEASE_SUPPLY_FILE}");
    }
    let mut temp_file = tempfile::Builder::new()
        .prefix("mcap-cli-stdin-bytesource-")
        .tempfile()
        .context("failed to create temporary file for stdin input")?;
    std::io::copy(&mut stdin.lock(), temp_file.as_file_mut())
        .context("failed to read input from stdin")?;
    LocalFileSource::from_temp_file(temp_file, PathBuf::from("<stdin>"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::io::Cursor;

    fn write_summary_mcap() -> Vec<u8> {
        let mut buffer = Vec::new();
        {
            let mut writer = mcap::Writer::new(Cursor::new(&mut buffer)).expect("writer");
            let schema_id = writer
                .add_schema("demo_schema", "jsonschema", br#"{"type":"object"}"#)
                .expect("schema");
            let channel_id = writer
                .add_channel(schema_id, "/demo", "json", &BTreeMap::new())
                .expect("channel");
            writer
                .write_to_known_channel(
                    &mcap::records::MessageHeader {
                        channel_id,
                        sequence: 1,
                        log_time: 10,
                        publish_time: 10,
                    },
                    br#"{"ok":true}"#,
                )
                .expect("message");
            writer.finish().expect("finish");
        }
        buffer
    }

    #[test]
    fn memory_source_read_at_clamps_to_eof() {
        let mut source = MemorySource::new(vec![1, 2, 3, 4, 5]);
        assert_eq!(source.read_at(3, 10).expect("read"), vec![4, 5]);
        assert!(source.read_at(5, 1).expect("past eof").is_empty());
        assert!(source.read_at(100, 1).expect("far past eof").is_empty());
    }

    #[test]
    fn memory_source_read_into_reuses_caller_buffer() {
        let mut source = MemorySource::new(vec![10, 20, 30, 40]);
        let mut buf = vec![0u8; 8];
        let n = source.read_into(1, &mut buf).expect("read_into");
        assert_eq!(n, 3);
        assert_eq!(&buf[..n], &[20, 30, 40]);
    }

    #[test]
    fn memory_source_summary_reader_finds_channel() {
        let bytes = write_summary_mcap();
        let mut source = MemorySource::new(bytes);
        let summary = read_summary(&mut source)
            .expect("summary read")
            .expect("summary should exist");
        assert!(summary.channels.values().any(|ch| ch.topic == "/demo"));
        assert!(!summary.chunk_indexes.is_empty());
    }

    #[test]
    fn local_file_source_matches_memory_summary() {
        let bytes = write_summary_mcap();
        let dir = tempfile::TempDir::new().expect("temp dir");
        let path = dir.path().join("demo.mcap");
        std::fs::write(&path, &bytes).expect("write fixture");

        let mut local = LocalFileSource::open_path(&path).expect("open local");
        assert!(local.is_seekable());
        assert!(!local.is_remote());
        assert_eq!(local.size().expect("size"), Some(bytes.len() as u64));

        let summary = read_summary(&mut local)
            .expect("summary read")
            .expect("summary should exist");
        assert!(summary.channels.values().any(|ch| ch.topic == "/demo"));
    }

    #[test]
    fn local_file_readahead_serves_sequential_tiny_reads() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let path = dir.path().join("seq.bin");
        let bytes: Vec<u8> = (0..10_000u32).flat_map(|i| i.to_le_bytes()).collect();
        std::fs::write(&path, &bytes).expect("write");

        let mut local = LocalFileSource::open_path(&path).expect("open");
        let mut offset = 0u64;
        let mut out = Vec::new();
        while offset < bytes.len() as u64 {
            let mut buf = [0u8; 9];
            let n = local.read_into(offset, &mut buf).expect("read");
            assert!(n > 0);
            out.extend_from_slice(&buf[..n]);
            offset += n as u64;
        }
        assert_eq!(out, bytes);
        // After a sequential scan the window should be warm near EOF.
        assert!(!local.cache.is_empty() || local.file_pos.is_some());
    }

    #[test]
    fn local_file_readahead_survives_random_jumps() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let path = dir.path().join("rand.bin");
        let bytes: Vec<u8> = (0..4096u16).flat_map(|i| i.to_le_bytes()).collect();
        std::fs::write(&path, &bytes).expect("write");

        let mut local = LocalFileSource::open_path(&path).expect("open");
        for &offset in &[0u64, 100, 3000, 50, 7000, 0] {
            let mut buf = [0u8; 16];
            let n = local.read_into(offset, &mut buf).expect("read");
            let start = offset as usize;
            let end = (start + n).min(bytes.len());
            assert_eq!(&buf[..end - start], &bytes[start..end]);
        }
    }

    #[test]
    fn for_each_linear_record_visits_opcodes() {
        let bytes = write_summary_mcap();
        let mut source = MemorySource::new(bytes);
        let mut opcodes = Vec::new();
        for_each_linear_record(
            &mut source,
            mcap::sans_io::LinearReaderOptions::default(),
            |opcode, _data| {
                opcodes.push(opcode);
                Ok(())
            },
        )
        .expect("linear scan");
        assert!(
            opcodes.contains(&mcap::records::op::HEADER),
            "expected header opcode in {opcodes:?}"
        );
        assert!(
            opcodes.contains(&mcap::records::op::FOOTER),
            "expected footer opcode in {opcodes:?}"
        );
    }

    #[test]
    fn sequential_source_supports_linear_scan_without_seek() {
        let bytes = write_summary_mcap();
        let mut source = SequentialSource::new(Cursor::new(bytes), "<cursor>");
        assert!(!source.is_seekable());
        assert_eq!(source.size().expect("size"), None);
        assert!(
            read_summary(&mut source)
                .expect("summary on sequential")
                .is_none(),
            "summary requires seek; sequential sources should skip it"
        );

        let mut source = SequentialSource::new(Cursor::new(write_summary_mcap()), "<cursor>");
        let mut opcodes = Vec::new();
        for_each_linear_record(
            &mut source,
            mcap::sans_io::LinearReaderOptions::default(),
            |opcode, _| {
                opcodes.push(opcode);
                Ok(())
            },
        )
        .expect("sequential linear scan");
        assert!(opcodes.contains(&mcap::records::op::HEADER));
    }

    #[test]
    fn sequential_source_rejects_non_monotonic_reads() {
        let mut source = SequentialSource::new(Cursor::new(vec![1, 2, 3, 4]), "<cursor>");
        let mut buf = [0u8; 2];
        assert_eq!(source.read_into(0, &mut buf).expect("first"), 2);
        let err = source.read_into(0, &mut buf).expect_err("rewind must fail");
        assert!(
            err.to_string().contains("sequential"),
            "unexpected error: {err:#}"
        );
    }

    #[test]
    fn service_indexed_chunk_feeds_messages() {
        let bytes = write_summary_mcap();
        let mut source = MemorySource::new(bytes.clone());
        let summary = read_summary(&mut source)
            .expect("summary read")
            .expect("summary should exist");
        let mut reader = mcap::sans_io::IndexedReader::new(&summary).expect("indexed reader");
        let mut messages = 0usize;
        while let Some(event) = reader.next_event() {
            match event.expect("indexed event") {
                mcap::sans_io::IndexedReadEvent::ReadChunkRequest { offset, length } => {
                    service_indexed_chunk(&mut reader, &mut source, offset, length)
                        .expect("service chunk");
                }
                mcap::sans_io::IndexedReadEvent::Message { .. } => {
                    messages += 1;
                }
            }
        }
        assert_eq!(messages, 1);
    }
}
