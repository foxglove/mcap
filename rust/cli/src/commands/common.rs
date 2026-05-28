use std::fmt::Write as _;
use std::io::{IsTerminal as _, Read as _, SeekFrom};
use std::path::Path;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use mcap::records::{self, Record};
use mcap::sans_io::{SummaryReadEvent, SummaryReader, SummaryReaderOptions};
use memmap2::Mmap;
use tempfile::NamedTempFile;
use ureq::Agent;

use crate::context::CommandContext;

pub const PLEASE_REDIRECT: &str =
    "Binary output can screw up your terminal. Supply -o or redirect to a file or pipe";
pub const PLEASE_SUPPLY_FILE: &str = "please supply a file. see --help for usage details.";
const REMOTE_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const REMOTE_RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);
const REMOTE_BODY_TIMEOUT: Duration = Duration::from_secs(60);

pub enum InputData {
    Mapped(Mmap),
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
            InputData::Buffered(buf) => buf.as_slice(),
        }
    }
}

pub struct MaterializedInput {
    temp_file: Option<NamedTempFile>,
    local_path: Option<std::path::PathBuf>,
}

pub struct RemoteMcap {
    reader: HttpRangeReader,
    summary: mcap::Summary,
    parsed: ParsedMcap,
}

impl RemoteMcap {
    pub fn parsed(&self) -> &ParsedMcap {
        &self.parsed
    }

    pub fn summary(&self) -> &mcap::Summary {
        &self.summary
    }

    pub fn read_range(&self, offset: u64, length: usize) -> Result<Vec<u8>> {
        self.reader.read_range(offset, length)
    }
}

pub struct HttpRangeReader {
    agent: Agent,
    url: String,
    display_url: String,
    size: u64,
    offset: u64,
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

pub fn load_path(ctx: &CommandContext, path: &Path) -> Result<InputData> {
    if is_http_url(path) {
        return Ok(InputData::Buffered(read_remote_input(ctx, path)?));
    }
    Ok(InputData::Mapped(map_file(path)?))
}

pub fn load_input(ctx: &CommandContext, file: Option<&Path>) -> Result<InputData> {
    if let Some(path) = file {
        return load_path(ctx, path);
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

pub fn materialize_input(ctx: &CommandContext, path: &Path) -> Result<MaterializedInput> {
    if !is_http_url(path) {
        return Ok(MaterializedInput {
            temp_file: None,
            local_path: Some(path.to_path_buf()),
        });
    }

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
    read_remote_input_to_writer(ctx, path, temp_file.as_file_mut())?;
    std::io::Write::flush(temp_file.as_file_mut())
        .context("failed to flush temporary remote input file")?;
    Ok(MaterializedInput {
        temp_file: Some(temp_file),
        local_path: None,
    })
}

pub fn try_open_remote_mcap(path: &Path) -> Result<Option<RemoteMcap>> {
    if !is_http_url(path) {
        return Ok(None);
    }
    let Some(mut reader) = HttpRangeReader::open(path)? else {
        return Ok(None);
    };
    let header = read_header_from_seekable(&mut reader)?;
    let Some(summary) = read_summary_from_seekable(&mut reader)? else {
        return Ok(None);
    };
    let parsed = parsed_mcap_from_summary_ref(header, &summary);
    Ok(Some(RemoteMcap {
        reader,
        summary,
        parsed,
    }))
}

pub fn is_http_url(path: &Path) -> bool {
    let Some(text) = path.to_str() else {
        return false;
    };
    let Some((scheme, _)) = text.split_once("://") else {
        return false;
    };
    scheme.eq_ignore_ascii_case("http") || scheme.eq_ignore_ascii_case("https")
}

pub fn remote_or_local_extension(path: &Path) -> Option<String> {
    if is_http_url(path) {
        let text = remote_url_without_fragment_or_query(path.to_str()?);
        return Path::new(text)
            .extension()
            .and_then(|extension| extension.to_str())
            .map(str::to_string);
    }
    path.extension()
        .and_then(|extension| extension.to_str())
        .map(str::to_string)
}

fn read_remote_input(ctx: &CommandContext, path: &Path) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    read_remote_input_to_writer(ctx, path, &mut bytes)?;
    Ok(bytes)
}

impl HttpRangeReader {
    fn open(path: &Path) -> Result<Option<Self>> {
        let url = path.to_str().ok_or_else(|| {
            anyhow::anyhow!("remote URL is not valid UTF-8: '{}'", path.display())
        })?;
        let display_url = redact_url(url);
        let agent = remote_agent();
        let Some(size) = probe_range_size(&agent, url, &display_url)? else {
            return Ok(None);
        };
        Ok(Some(Self {
            agent,
            url: url.to_string(),
            display_url,
            size,
            offset: 0,
        }))
    }

    fn read_range(&self, offset: u64, length: usize) -> Result<Vec<u8>> {
        if length == 0 || offset >= self.size {
            return Ok(Vec::new());
        }
        let requested_end = offset
            .checked_add(length as u64)
            .and_then(|end| end.checked_sub(1))
            .ok_or_else(|| anyhow::anyhow!("remote range overflow"))?;
        let end = requested_end.min(self.size - 1);
        let range = format!("bytes={offset}-{end}");
        let mut response = self
            .agent
            .get(&self.url)
            .header("Range", &range)
            .call()
            .with_context(|| format!("failed to fetch range from {}", self.display_url))?;
        let status = response.status();
        if status.as_u16() != 206 {
            bail!(
                "failed to fetch range from {}: HTTP {status}",
                self.display_url
            );
        }
        let mut bytes = Vec::with_capacity(length.min((end - offset + 1) as usize));
        std::io::copy(&mut response.body_mut().as_reader(), &mut bytes)
            .with_context(|| format!("failed to read range from {}", self.display_url))?;
        Ok(bytes)
    }
}

impl std::io::Read for HttpRangeReader {
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

impl std::io::Seek for HttpRangeReader {
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

fn probe_range_size(agent: &Agent, url: &str, display_url: &str) -> Result<Option<u64>> {
    let response = agent
        .get(url)
        .header("Range", "bytes=0-0")
        .call()
        .with_context(|| format!("failed to fetch {display_url}"))?;
    match response.status() {
        status if status.as_u16() == 206 => Ok(response
            .headers()
            .get("content-range")
            .and_then(|value| value.to_str().ok())
            .and_then(parse_content_range_len)),
        status if status.is_success() => Ok(None),
        status => bail!("failed to fetch {display_url}: HTTP {status}"),
    }
}

fn read_remote_input_to_writer(
    ctx: &CommandContext,
    path: &Path,
    writer: &mut impl std::io::Write,
) -> Result<()> {
    let url = path
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("remote URL is not valid UTF-8: '{}'", path.display()))?;
    let display_url = redact_url(url);
    let agent = remote_agent();
    if ctx.verbose() == 0 {
        eprintln!("Warning: reading entire remote file {display_url}");
    } else {
        eprintln!("Warning: reading entire remote file {display_url} (range path unavailable)");
    }

    let mut response = agent
        .get(url)
        .call()
        .with_context(|| format!("failed to read remote input {display_url}"))?;
    let status = response.status();
    if !status.is_success() {
        bail!("failed to read remote input {display_url}: HTTP {status}");
    }

    copy_response(&mut response, writer, &display_url)?;
    Ok(())
}

fn remote_agent() -> Agent {
    Agent::config_builder()
        .http_status_as_error(false)
        .timeout_connect(Some(REMOTE_CONNECT_TIMEOUT))
        .timeout_recv_response(Some(REMOTE_RESPONSE_TIMEOUT))
        .timeout_recv_body(Some(REMOTE_BODY_TIMEOUT))
        .build()
        .into()
}

fn redact_url(url: &str) -> String {
    remote_url_without_fragment_or_query(url).to_string()
}

fn remote_url_without_fragment_or_query(url: &str) -> &str {
    url.split('#')
        .next()
        .unwrap_or(url)
        .split('?')
        .next()
        .unwrap_or(url)
}

fn copy_response(
    response: &mut ureq::http::Response<ureq::Body>,
    writer: &mut impl std::io::Write,
    display_url: &str,
) -> Result<()> {
    std::io::copy(&mut response.body_mut().as_reader(), writer)
        .with_context(|| format!("failed to read remote input {display_url}"))?;
    Ok(())
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

fn parse_content_range_len(value: &str) -> Option<u64> {
    let (_, total) = value.rsplit_once('/')?;
    if total == "*" {
        return None;
    }
    total.parse().ok()
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
    use std::io::{Read, SeekFrom, Write};
    use std::net::TcpListener;
    use std::path::Path;
    use std::thread;

    use super::{
        decimal_time, format_table, formatted_time, human_bytes, load_path, parse_mcap,
        parse_mcap_from_summary, print_table, write_raw_time,
    };
    use mcap::records;

    fn serve_http(body: &'static [u8], supports_ranges: bool) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
        let addr = listener.local_addr().expect("test server addr");
        thread::spawn(move || {
            for stream in listener.incoming().take(64) {
                let mut stream = stream.expect("accept test connection");
                let mut request = [0u8; 4096];
                let read = stream.read(&mut request).expect("read request");
                let request = String::from_utf8_lossy(&request[..read]);
                let is_head = request.starts_with("HEAD ");
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
                    let response = format!(
                        "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{end}/{}\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n",
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
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: {accept_ranges}\r\nConnection: close\r\n\r\n",
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
    fn table_printer_handles_empty_input() {
        print_table(&[]);
        assert!(format_table(&[]).is_empty());
    }

    #[test]
    fn remote_errors_redact_query_strings() {
        let url = "http://127.0.0.1:1/demo.mcap?X-Amz-Signature=secret-token";
        let err = load_path(&crate::context::CommandContext::default(), Path::new(url))
            .expect_err("connection failure should report redacted URL");
        assert!(!err.to_string().contains("secret-token"));
        assert!(!err.to_string().contains("X-Amz-Signature"));
    }

    #[test]
    fn remote_http_input_reads_entire_file() {
        let url = serve_http(b"hello remote", true);
        let input = load_path(&crate::context::CommandContext::default(), Path::new(&url))
            .expect("remote read");
        assert_eq!(input.as_slice(), b"hello remote");
    }

    #[test]
    fn remote_mcap_summary_uses_range_reader() {
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
        let body: &'static [u8] = Box::leak(buffer.into_boxed_slice());
        let url = serve_http(body, true);
        let remote = super::try_open_remote_mcap(Path::new(&url))
            .expect("remote summary read")
            .expect("summary should be present");

        assert!(remote.parsed().header.is_some());
        assert!(remote.parsed().channels.contains_key(&channel_id));
    }

    #[test]
    fn http_url_scheme_is_case_insensitive() {
        assert!(super::is_http_url(Path::new(
            "HTTP://example.com/demo.mcap"
        )));
        assert!(super::is_http_url(Path::new(
            "Https://example.com/demo.mcap"
        )));
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
    }

    #[test]
    fn http_range_reader_allows_seek_past_end() {
        let mut reader = super::HttpRangeReader {
            agent: super::remote_agent(),
            url: "http://127.0.0.1:1/demo.mcap".to_string(),
            display_url: "http://127.0.0.1:1/demo.mcap".to_string(),
            size: 10,
            offset: 0,
        };

        assert_eq!(
            std::io::Seek::seek(&mut reader, SeekFrom::End(1)).unwrap(),
            11
        );
        let mut byte = [0_u8; 1];
        assert_eq!(std::io::Read::read(&mut reader, &mut byte).unwrap(), 0);
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
