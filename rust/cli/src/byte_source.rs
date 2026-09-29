//! Byte sources for sans-io MCAP reads: local files via seek+read and remote URLs via HTTP
//! range requests (see [`open_byte_source`]), plus a forward-only [`SequentialSource`] for
//! stdin and other streams that commands read exactly once, in order.

mod drivers;

pub use drivers::{
    for_each_linear_record, read_header, read_summary, service_indexed_chunk,
    try_for_each_linear_record,
};

use std::fs::File;
use std::io::{BufReader, IsTerminal as _, Read, Seek as _, SeekFrom, Write as _};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use tempfile::NamedTempFile;

use crate::source::{
    is_remote_url, materialize_input, open_remote_range_reader, redacted_display,
    remote_scan_opt_in_suffix, require_remote_scan_allowed, RemoteRangeReader, SourceOptions,
    PLEASE_SUPPLY_FILE,
};

/// Byte-oriented input with optional random access.
pub trait ByteSource {
    /// File size in bytes, or `None` when unknown (streaming inputs without a known length).
    fn size(&self) -> Option<u64>;
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
        // Clamp before allocating: `len` often comes from a record length field in the file,
        // which a damaged file can set near u64::MAX.
        let len = match self.size() {
            Some(size) => {
                len.min(usize::try_from(size.saturating_sub(offset)).unwrap_or(usize::MAX))
            }
            None => len,
        };
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
    fn size(&self) -> Option<u64> {
        Some(self.size)
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
        let available = usize::try_from(self.size - offset).unwrap_or(usize::MAX);
        let to_read = dest.len().min(available);
        let dest = &mut dest[..to_read];
        let result = if self.pos == Some(offset) {
            // Sequential: read through the buffer, one syscall per buffer fill instead of one per
            // call.
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
/// about two requests per record. Object stores charge tens of milliseconds per request before
/// the first byte arrives, so the window has to be large enough that transfer time, not request
/// latency, dominates a scan. 8 MiB is the byte-range size AWS recommends for S3 and the part
/// size its own clients default to; measurements in Durner et al., "Exploiting Cloud Object
/// Storage for High-Performance Analytics" (PVLDB 2023) put the throughput plateau at 8-16 MiB.
pub(crate) const REMOTE_READ_AHEAD_BYTES: usize = 8 * 1024 * 1024;

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
    fn size(&self) -> Option<u64> {
        Some(self.inner.size())
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

/// Forward-only stream such as stdin. Serves reads at the current position straight from the
/// reader and refuses any other offset, so a command that only scans in order neither buffers
/// nor spools the input.
pub struct SequentialSource<R> {
    reader: R,
    pos: u64,
    name: String,
}

impl SequentialSource<std::io::StdinLock<'static>> {
    /// Holds the stdin lock for the life of the source, so the two reads per record do not each
    /// take and release the mutex. Nothing else in the process reads stdin.
    pub fn stdin() -> Result<Self> {
        let stdin = std::io::stdin();
        if stdin.is_terminal() {
            bail!("{PLEASE_SUPPLY_FILE}");
        }
        Ok(Self::new(stdin.lock(), "<stdin>"))
    }
}

impl<R: Read> SequentialSource<R> {
    pub fn new(reader: R, name: impl Into<String>) -> Self {
        Self {
            reader,
            pos: 0,
            name: name.into(),
        }
    }
}

impl<R: Read> ByteSource for SequentialSource<R> {
    fn size(&self) -> Option<u64> {
        None
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
        if offset != self.pos {
            bail!(
                "{} is not seekable: read at offset {offset} requested at position {}",
                self.name,
                self.pos
            );
        }
        if dest.is_empty() {
            return Ok(0);
        }
        let n = loop {
            match self.reader.read(dest) {
                Ok(n) => break n,
                Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(err) => {
                    return Err(err).with_context(|| {
                        format!("failed to read {} at offset {offset}", self.name)
                    })
                }
            }
        };
        self.pos += n as u64;
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
    fn size(&self) -> Option<u64> {
        Some(self.data.len() as u64)
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

/// Open a local path or remote URL as a random-access [`ByteSource`]. Remotes use range
/// requests when available; otherwise they need `--allow-remote-scan` and are downloaded to a
/// tempfile.
pub fn open_byte_source(path: &Path, options: SourceOptions) -> Result<Box<dyn ByteSource>> {
    if is_remote_url(path) {
        return open_remote_byte_source(path, options);
    }

    Ok(Box::new(LocalFileSource::open_path(path)?))
}

/// Opens `path` for a command that always reads the whole file. A remote URL needs the
/// `--allow-remote-scan` opt-in, checked here before any request goes out, so a refused command
/// never contacts the server.
pub fn open_byte_source_for_scan(
    path: &Path,
    options: SourceOptions,
) -> Result<Box<dyn ByteSource>> {
    if is_remote_url(path) {
        require_remote_scan_allowed(path, options)?;
    }
    open_byte_source(path, options)
}

/// Spools stdin to a tempfile and opens it as a random-access [`ByteSource`], for commands that
/// may seek. Commands that only scan forward should use [`SequentialSource::stdin`] instead.
pub fn open_stdin_byte_source() -> Result<Box<dyn ByteSource>> {
    let stdin = std::io::stdin();
    if stdin.is_terminal() {
        bail!("{PLEASE_SUPPLY_FILE}");
    }
    let mut temp_file = tempfile::Builder::new()
        .prefix("mcap-cli-stdin-input-")
        .tempfile()
        .context("failed to create temporary file for stdin input")?;
    std::io::copy(&mut stdin.lock(), temp_file.as_file_mut())
        .context("failed to read input from stdin")?;
    Ok(Box::new(LocalFileSource::from_temp_file(
        temp_file,
        PathBuf::from("<stdin>"),
    )?))
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

fn spool_remote_to_local(path: &Path, options: SourceOptions) -> Result<LocalFileSource> {
    let temp_file = materialize_input(path, options)?
        .into_temp_file()
        .context("remote input was not downloaded to a temporary file")?;
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

        // A length from a corrupt record field must be clamped before anything is allocated.
        let tail = source
            .read_at(size as u64 - 5, usize::MAX)
            .expect("huge read_at clamps to EOF");
        assert_eq!(tail, &expected[size - 5..]);
    }

    #[test]
    fn sequential_source_serves_in_order_reads_and_refuses_seeks() {
        let mut source = SequentialSource::new(Cursor::new(vec![1u8, 2, 3, 4, 5]), "<pipe>");
        assert_eq!(source.size(), None);
        assert_eq!(source.display_name(), "<pipe>");

        let mut dest = [0u8; 2];
        assert_eq!(source.read_into(0, &mut dest).expect("first read"), 2);
        assert_eq!(dest, [1, 2]);
        assert_eq!(source.read_at(2, 2).expect("second read"), vec![3, 4]);

        let err = source
            .read_into(0, &mut dest)
            .expect_err("re-reading an earlier offset must fail");
        assert!(format!("{err:#}").contains("not seekable"), "{err:#}");
        let err = source
            .read_into(10, &mut dest)
            .expect_err("skipping ahead must fail");
        assert!(format!("{err:#}").contains("not seekable"), "{err:#}");

        // The failed reads did not advance the position.
        assert_eq!(source.read_into(4, &mut dest).expect("last read"), 1);
        assert_eq!(dest[0], 5);
        assert_eq!(source.read_into(5, &mut dest).expect("at eof"), 0);
    }

    #[test]
    fn sequential_source_reports_reader_errors_with_its_name() {
        struct Broken;
        impl Read for Broken {
            fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
                Err(std::io::Error::other("pipe exploded"))
            }
        }
        let mut source = SequentialSource::new(Broken, "<pipe>");
        let err = source.read_at(0, 4).expect_err("reader error propagates");
        let message = format!("{err:#}");
        assert!(message.contains("failed to read <pipe>"), "{message}");
        assert!(message.contains("pipe exploded"), "{message}");
    }

    #[test]
    fn memory_source_read_into_clamps_to_eof() {
        let mut source = MemorySource::new(vec![1, 2, 3, 4, 5]);
        let mut dest = [0u8; 10];
        assert_eq!(source.read_into(3, &mut dest).expect("read"), 2);
        assert_eq!(&dest[..2], &[4, 5]);
        assert_eq!(source.read_into(5, &mut dest[..1]).expect("past eof"), 0);
        assert_eq!(source.read_at(3, 10).expect("read_at"), vec![4, 5]);
        // A length from a corrupt record field must be clamped before anything is allocated.
        assert_eq!(
            source.read_at(3, usize::MAX).expect("huge read_at"),
            vec![4, 5]
        );
        assert!(source.read_at(5, usize::MAX).expect("past eof").is_empty());
    }

    /// Walks every top-level record through `read_into`, the way the sans-io readers do.
    fn record_sizes(source: &mut dyn ByteSource) -> Vec<(u8, usize)> {
        let mut reader = mcap::sans_io::LinearReader::new();
        let mut pos = 0u64;
        let mut records = Vec::new();
        while let Some(event) = reader.next_event() {
            match event.expect("linear reader event") {
                mcap::sans_io::LinearReadEvent::ReadRequest(need) => {
                    let buf = reader.insert(need);
                    let n = source.read_into(pos, buf).expect("read_into");
                    reader.notify_read(n);
                    pos += n as u64;
                }
                mcap::sans_io::LinearReadEvent::Record { opcode, data } => {
                    records.push((opcode, data.len()));
                }
            }
        }
        records
    }

    #[test]
    fn local_file_source_matches_memory_linear_scan() {
        let bytes = write_summary_mcap();
        let dir = tempfile::TempDir::new().expect("temp dir");
        let path = dir.path().join("demo.mcap");
        std::fs::write(&path, &bytes).expect("write fixture");

        let mut local = LocalFileSource::open_path(&path).expect("open local");
        assert_eq!(local.size(), Some(bytes.len() as u64));

        let mut memory = MemorySource::new(bytes);
        let expected = record_sizes(&mut memory);
        assert!(expected
            .iter()
            .any(|(opcode, _)| *opcode == mcap::records::op::MESSAGE));
        assert_eq!(record_sizes(&mut local), expected);
    }

    #[test]
    fn memory_source_linear_scan_visits_opcodes() {
        let bytes = write_summary_mcap();
        let mut source = MemorySource::new(bytes);
        let opcodes: Vec<u8> = record_sizes(&mut source)
            .into_iter()
            .map(|(opcode, _)| opcode)
            .collect();
        assert_eq!(opcodes.first(), Some(&mcap::records::op::HEADER));
        assert_eq!(opcodes.last(), Some(&mcap::records::op::FOOTER));
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
        assert_eq!(local.size(), Some(bytes.len() as u64));

        let summary = read_summary(&mut local)
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
        assert_eq!(opcodes.first(), Some(&mcap::records::op::HEADER));
        assert_eq!(opcodes.last(), Some(&mcap::records::op::FOOTER));
    }

    #[test]
    fn service_indexed_chunk_feeds_messages() {
        let bytes = write_summary_mcap();
        let mut source = MemorySource::new(bytes);
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
