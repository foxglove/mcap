//! Random-access byte sources for sans-io MCAP reads.
//!
//! Local files use seek+read (no mmap). Remote URLs prefer HTTP range requests.
//! Stdin is spooled to a temp file when opened through [`open_byte_source`].

mod drivers;

pub use drivers::{for_each_linear_record, read_header, read_summary, service_indexed_chunk};

use std::fs::File;
use std::io::{IsTerminal as _, Read as _, Seek as _, SeekFrom, Write as _};
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
    /// File size in bytes, or `None` when unknown (pure stdin without spooling).
    fn size(&self) -> Result<Option<u64>>;
    fn is_remote(&self) -> bool;
    fn display_name(&self) -> String;
    /// Read `[offset, offset+len)` bytes. `len` may be clamped to EOF.
    fn read_at(&mut self, offset: u64, len: usize) -> Result<Vec<u8>>;
    /// Whether random access is available (`false` for pure stdin).
    fn is_seekable(&self) -> bool;
}

/// Local file opened for seek+read (not memory-mapped).
pub struct LocalFileSource {
    file: File,
    path: PathBuf,
    size: u64,
    // Keeps a spool tempfile alive for stdin / non-range remote fallbacks.
    _temp_file: Option<NamedTempFile>,
}

impl LocalFileSource {
    fn open_path(path: &Path) -> Result<Self> {
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

    fn read_at(&mut self, offset: u64, len: usize) -> Result<Vec<u8>> {
        read_file_at(&mut self.file, self.size, offset, len)
    }

    fn is_seekable(&self) -> bool {
        true
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

    fn read_at(&mut self, offset: u64, len: usize) -> Result<Vec<u8>> {
        self.inner.read_range(offset, len)
    }

    fn is_seekable(&self) -> bool {
        true
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

    fn read_at(&mut self, offset: u64, len: usize) -> Result<Vec<u8>> {
        if len == 0 || offset as usize >= self.data.len() {
            return Ok(Vec::new());
        }
        let start = offset as usize;
        let end = start.saturating_add(len).min(self.data.len());
        Ok(self.data[start..end].to_vec())
    }

    fn is_seekable(&self) -> bool {
        true
    }
}

/// Open a path, remote URL, or stdin as a [`ByteSource`].
///
/// - `None` spools stdin to a tempfile (seek+read, no mmap).
/// - Remote URLs use range requests when available.
/// - Non-range remotes require `--allow-remote-scan` and fall back to a full download into a tempfile.
/// - Local paths use seek+read (no mmap).
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

fn read_file_at(file: &mut File, size: u64, offset: u64, len: usize) -> Result<Vec<u8>> {
    if len == 0 || offset >= size {
        return Ok(Vec::new());
    }
    let available = (size - offset) as usize;
    let to_read = len.min(available);
    file.seek(SeekFrom::Start(offset))
        .context("failed to seek in local file")?;
    let mut buf = vec![0u8; to_read];
    file.read_exact(&mut buf)
        .context("failed to read from local file")?;
    Ok(buf)
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
