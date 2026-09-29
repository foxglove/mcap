//! Random-access byte sources for sans-io MCAP reads: local files via seek+read, remote URLs
//! via HTTP range requests, and stdin spooled to a temp file (see [`open_byte_source`]).

mod drivers;

pub use drivers::{
    for_each_linear_record, read_header, read_summary, service_indexed_chunk,
    try_for_each_linear_record,
};

use std::fs::File;
use std::io::{BufReader, IsTerminal as _, Read as _, Seek as _, SeekFrom, Write as _};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use tempfile::NamedTempFile;

use crate::source::{
    is_remote_url, open_remote_range_reader, read_remote_input_to_writer, redacted_display,
    remote_or_local_extension, remote_scan_opt_in_suffix, require_remote_scan_allowed,
    RemoteRangeReader, SourceOptions, PLEASE_SUPPLY_FILE,
};

/// Byte-oriented input with optional random access.
pub trait ByteSource {
    /// File size in bytes, or `None` when unknown (streaming inputs without a known length).
    fn size(&self) -> Result<Option<u64>>;
    fn is_remote(&self) -> bool;
    fn display_name(&self) -> String;
    /// Whether random access is available.
    fn is_seekable(&self) -> bool;

    /// Read up to `dest.len()` bytes at `offset` into `dest`, returning the count (0 at EOF).
    /// Sans-io drivers use this to fill `reader.insert(need)` without an extra allocation.
    fn read_into(&mut self, offset: u64, dest: &mut [u8]) -> Result<usize>;

    /// Read `[offset, offset+len)` into a new buffer, clamped to EOF. Hot paths should prefer
    /// [`Self::read_into`].
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

/// Read-ahead buffer for sequential reads. Sans-io readers request one record at a time, so
/// without it a scan costs a seek and a read per record. Small because `merge` opens one source
/// per input.
const LOCAL_FILE_BUFFER_BYTES: usize = 64 * 1024;

/// Local file opened for seek+read (not memory-mapped).
pub struct LocalFileSource {
    reader: BufReader<File>,
    /// Offset the next sequential read starts at; `None` after an I/O error left it unknown.
    pos: Option<u64>,
    path: PathBuf,
    size: u64,
    // Keeps a spool tempfile alive for stdin / non-range remote fallbacks.
    _temp_file: Option<NamedTempFile>,
}

impl LocalFileSource {
    pub(crate) fn open_path(path: &Path) -> Result<Self> {
        let file =
            File::open(path).with_context(|| format!("couldn't open '{}'", path.display()))?;
        let size = file
            .metadata()
            .with_context(|| format!("couldn't stat '{}'", path.display()))?
            .len();
        Ok(Self {
            reader: BufReader::with_capacity(LOCAL_FILE_BUFFER_BYTES, file),
            pos: Some(0),
            path: path.to_path_buf(),
            size,
            _temp_file: None,
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
            reader: BufReader::with_capacity(LOCAL_FILE_BUFFER_BYTES, file),
            pos: Some(0),
            path: display_path,
            size,
            _temp_file: Some(temp_file),
        })
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
        let available = (self.size - offset) as usize;
        let to_read = dest.len().min(available);
        let dest = &mut dest[..to_read];
        let result = if self.pos == Some(offset) {
            // Sequential: read through the buffer, one syscall per fill instead of per record.
            self.reader.read_exact(dest)
        } else {
            // Random access: seek (discarding the buffer) and read straight into `dest`, so a small
            // footer or index read does not pull in a full buffer of read-ahead.
            self.reader
                .seek(SeekFrom::Start(offset))
                .and_then(|_| self.reader.get_mut().read_exact(dest))
        };
        match result {
            Ok(()) => {
                self.pos = Some(offset + to_read as u64);
                Ok(to_read)
            }
            Err(err) => {
                self.pos = None;
                Err(err).with_context(|| format!("failed to read local file at offset {offset}"))
            }
        }
    }
}

/// Read-ahead window for sequential remote reads. Each range read is one HTTP request and the
/// sans-io readers ask for a record's prefix and body separately, so an unbuffered scan costs
/// about two requests per record. Sized like the tail prefetched at open, so memory is unchanged.
pub(crate) const REMOTE_READ_AHEAD_BYTES: usize = 256 * 1024;

/// Remote object that supports byte-range reads.
pub struct RemoteRangeSource {
    inner: RemoteRangeReader,
    /// Bytes `[window_start, window_start + window.len())`, filled on `read_into` misses and seeded
    /// with the tail prefetched at open, so summary reads usually need no request.
    window: Vec<u8>,
    window_start: u64,
    read_ahead_bytes: usize,
}

impl RemoteRangeSource {
    pub(crate) fn new(mut inner: RemoteRangeReader) -> Self {
        let (window_start, window) = inner.take_tail().unwrap_or_default();
        Self {
            inner,
            window,
            window_start,
            read_ahead_bytes: REMOTE_READ_AHEAD_BYTES,
        }
    }

    /// Shrinks the window so tests can exercise refills on small fixtures.
    #[cfg(test)]
    pub(crate) fn set_read_ahead_bytes(&mut self, bytes: usize) {
        self.read_ahead_bytes = bytes;
    }

    fn window_end(&self) -> u64 {
        self.window_start + self.window.len() as u64
    }

    /// The window's bytes for `[offset, offset + len)` if fully held, or what remains when the
    /// window reaches EOF.
    fn window_slice(&self, offset: u64, len: usize) -> Option<&[u8]> {
        if offset < self.window_start || offset >= self.window_end() {
            return None;
        }
        let start = (offset - self.window_start) as usize;
        let available = self.window.len() - start;
        if available >= len || self.window_end() >= self.inner.size() {
            Some(&self.window[start..start + available.min(len)])
        } else {
            None
        }
    }

    fn fill_window(&mut self, offset: u64, len: usize) -> Result<()> {
        self.window = self
            .inner
            .read_range(offset, len.max(self.read_ahead_bytes))?;
        self.window_start = offset;
        Ok(())
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

    fn read_into(&mut self, offset: u64, dest: &mut [u8]) -> Result<usize> {
        if dest.is_empty() || offset >= self.inner.size() {
            return Ok(0);
        }
        if self.window_slice(offset, dest.len()).is_none() {
            self.fill_window(offset, dest.len())?;
        }
        // The window now starts at `offset`, so only a short server response yields fewer bytes
        // than asked; return what arrived and let the reader ask again.
        let start = (offset - self.window_start) as usize;
        let n = (self.window.len() - start).min(dest.len());
        if n == 0 {
            bail!(
                "remote range read at offset {offset} returned no data for {}",
                self.display_name()
            );
        }
        dest[..n].copy_from_slice(&self.window[start..start + n]);
        Ok(n)
    }

    fn read_at(&mut self, offset: u64, len: usize) -> Result<Vec<u8>> {
        if let Some(slice) = self.window_slice(offset, len) {
            return Ok(slice.to_vec());
        }
        // Chunk and attachment payloads are fetched exactly and bypass the window, so a single
        // indexed read never over-fetches.
        self.inner.read_range(offset, len)
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

/// Open a local path, remote URL, or stdin (`None`, spooled to a tempfile) as a [`ByteSource`].
/// Remotes use range requests when available; otherwise they need `--allow-remote-scan` and are
/// downloaded to a tempfile.
pub fn open_byte_source(
    path: Option<&Path>,
    options: SourceOptions,
) -> Result<Box<dyn ByteSource>> {
    let Some(path) = path else {
        return Ok(Box::new(spool_stdin_to_local()?));
    };

    if is_remote_url(path) {
        return open_remote_byte_source(path, options);
    }

    Ok(Box::new(LocalFileSource::open_path(path)?))
}

fn open_remote_byte_source(path: &Path, options: SourceOptions) -> Result<Box<dyn ByteSource>> {
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

fn spool_remote_to_local(path: &Path, options: SourceOptions) -> Result<LocalFileSource> {
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
    fn local_file_source_read_into_tracks_position_across_access_patterns() {
        let expected = (0..(3 * LOCAL_FILE_BUFFER_BYTES + 123))
            .map(|i| (i * 7 % 251) as u8)
            .collect::<Vec<u8>>();
        let mut temp = NamedTempFile::new().expect("temp file");
        temp.write_all(&expected).expect("write temp file");
        let mut source = LocalFileSource::open_path(temp.path()).expect("open local");
        let size = expected.len();

        let mut check = |offset: usize, len: usize, want: usize| {
            let mut dest = vec![0u8; len];
            let n = source
                .read_into(offset as u64, &mut dest)
                .expect("read_into");
            assert_eq!(n, want, "read {len} bytes at offset {offset}");
            let start = offset.min(size);
            assert_eq!(
                &dest[..n],
                &expected[start..start + n],
                "bytes at offset {offset}"
            );
        };

        // Sequential reads, first through a direct read then through the read-ahead buffer.
        check(0, 10, 10);
        check(10, 100, 100);
        check(110, 5, 5);
        // Backward re-read of bytes the buffer already holds.
        check(20, 50, 50);
        // Sequential read that spans a buffer refill boundary.
        check(
            70,
            LOCAL_FILE_BUFFER_BYTES + 10,
            LOCAL_FILE_BUFFER_BYTES + 10,
        );
        // Forward jump past the buffered window, then continue sequentially from there.
        check(2 * LOCAL_FILE_BUFFER_BYTES + 7, 33, 33);
        check(2 * LOCAL_FILE_BUFFER_BYTES + 40, 33, 33);
        // Reads that touch EOF clamp, and reads at or past EOF return nothing.
        check(size - 5, 100, 5);
        check(size, 10, 0);
        check(size + 10, 10, 0);
        check(0, 0, 0);
        // Sequential continuation still works after the EOF clamp left `pos` at the end.
        check(3, 8, 8);
    }

    #[test]
    fn memory_source_read_into_clamps_to_eof() {
        let mut source = MemorySource::new(vec![1, 2, 3, 4, 5]);
        let mut dest = [0u8; 10];
        assert_eq!(source.read_into(3, &mut dest).expect("read"), 2);
        assert_eq!(&dest[..2], &[4, 5]);
        assert_eq!(source.read_into(5, &mut dest[..1]).expect("past eof"), 0);
        assert_eq!(source.read_at(3, 10).expect("read_at"), vec![4, 5]);
    }

    #[test]
    fn memory_source_summary_reader_finds_channel() {
        let bytes = write_summary_mcap();
        let mut source = MemorySource::new(bytes);
        let summary = read_summary(&mut source, SourceOptions::default())
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

        let summary = read_summary(&mut local, SourceOptions::default())
            .expect("summary read")
            .expect("summary should exist");
        assert!(summary.channels.values().any(|ch| ch.topic == "/demo"));
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
    fn service_indexed_chunk_feeds_messages() {
        let bytes = write_summary_mcap();
        let mut source = MemorySource::new(bytes.clone());
        let summary = read_summary(&mut source, SourceOptions::default())
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
