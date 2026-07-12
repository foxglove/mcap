use std::path::PathBuf;
use std::sync::LazyLock;

use anyhow::{Context, Result};
use clap::{ArgAction, Parser, Subcommand};
use clap_complete::Shell;
use log::warn;

use crate::logsetup;

pub(crate) static VERSION: LazyLock<String> = LazyLock::new(|| {
    // `GIT_SHORT_SHA` is the abbreviated commit (set by `build.rs`), or `unknown` when
    // the build had no provenance (no git checkout and not a `git archive` tarball).
    format!(
        "{} ({}) mcap-rust/{}",
        env!("CARGO_PKG_VERSION"),
        env!("GIT_SHORT_SHA"),
        mcap::VERSION
    )
});

/// The `Header.library` stamped on every MCAP this CLI authors. The CLI re-encodes the bytes, so it
/// is the writer and names itself, pairing its own version with the underlying `mcap` crate's
/// `mcap::LIBRARY_IDENTIFIER` (the two are versioned independently). The source file's library is not
/// carried forward; lineage, if ever needed, belongs in a metadata record rather than this field.
/// (`add` is exempt: it appends to an existing file without rewriting the header.)
pub(crate) static LIBRARY_IDENTIFIER: LazyLock<String> = LazyLock::new(|| {
    format!(
        "mcap-cli/{} {}",
        env!("CARGO_PKG_VERSION"),
        mcap::LIBRARY_IDENTIFIER
    )
});

#[derive(Parser, Debug, PartialEq, Eq)]
#[command(
    name = "mcap",
    bin_name = "mcap",
    version = VERSION.as_str(),
)]
pub struct Args {
    #[arg(
        short,
        long,
        value_enum,
        default_value_t = logsetup::Color::Auto,
        global = true
    )]
    pub color: logsetup::Color,

    /// Allow commands to download/scan remote inputs
    #[arg(long, default_value_t = false, global = true)]
    pub allow_remote_scan: bool,

    #[command(subcommand)]
    pub command: Command,

    /// Verbosity (-v, -vv, -vvv, etc.)
    #[arg(short, long, action = ArgAction::Count, global = true)]
    pub verbose: u8,
}

#[derive(Subcommand, Debug, PartialEq, Eq)]
pub enum Command {
    /// Add records to an existing MCAP file
    Add(AddCommand),
    /// Concatenate the messages in one or more MCAP files to stdout
    Cat(CatCommand),
    /// Generate shell completion scripts
    ///
    /// To load completions in the current shell session:
    ///   bash:       source <(mcap completion bash)
    ///   zsh:        source <(mcap completion zsh)
    ///   fish:       mcap completion fish | source
    ///   powershell: mcap completion powershell | Out-String | Invoke-Expression
    #[command(verbatim_doc_comment)]
    Completion(CompletionCommand),
    /// Create a compressed copy of an MCAP file
    Compress(CompressCommand),
    /// Convert supported input files to MCAP
    #[command(
        long_about = "Convert supported input files to MCAP.\n\nSupported inputs:\n  .bag  ROS 1 bag\n  .db3  ROS 2 SQLite db3"
    )]
    Convert(ConvertCommand),
    /// Create an uncompressed copy of an MCAP file
    Decompress(DecompressCommand),
    /// Check an MCAP file structure
    Doctor(DoctorCommand),
    /// Compute byte usage statistics for MCAP records
    Du(DuCommand),
    /// Copy filtered MCAP data to a new file
    Filter(FilterCommand),
    /// Get a record from an MCAP file
    Get(GetCommand),
    /// Report statistics about an MCAP file
    Info(InfoCommand),
    /// List records of an MCAP file
    List(ListCommand),
    /// Merge MCAP files
    Merge(MergeCommand),
    /// Recover data from a potentially corrupt MCAP file
    Recover(RecoverCommand),
    /// Read an MCAP file and write messages sorted by log time
    Sort(SortCommand),
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
pub struct CompletionCommand {
    /// Shell to generate a completion script for
    #[arg(value_enum)]
    pub shell: Shell,
}

/// Options shared by every rewrite-based command (`filter`, `compress`, `decompress`, `sort`).
/// Each command flattens these and adds only the knobs that apply to it, so the common definitions
/// (and their help text) live in one place.
#[derive(clap::Args, Debug, PartialEq, Eq)]
pub struct CommonRewriteArgs {
    /// Input MCAP file path. If omitted, reads from stdin.
    pub file: Option<PathBuf>,

    /// Output file path. If omitted, writes to stdout.
    #[arg(short = 'o', long = "output")]
    pub output: Option<PathBuf>,

    /// Deprecated: use --output.
    #[arg(long = "output-file", hide = true)]
    pub output_file: Option<PathBuf>,

    /// Target uncompressed chunk size for output
    #[arg(long = "chunk-size", default_value_t = mcap::WriteOptions::DEFAULT_CHUNK_SIZE)]
    pub chunk_size: u64,

    /// Disable all output CRC fields
    #[arg(long = "no-crc", default_value_t = false)]
    pub no_crc: bool,
}

impl CommonRewriteArgs {
    /// The resolved output path: the `-o/--output` value, falling back to the deprecated
    /// `--output-file` alias. `None` means write to stdout.
    pub(crate) fn output(&self) -> Option<PathBuf> {
        self.output.clone().or_else(|| self.output_file.clone())
    }

    /// Warns about any deprecated shared flags that were supplied. Called by every rewrite
    /// command handler.
    pub(crate) fn warn_deprecations(&self) {
        if self.output_file.is_some() {
            warn!("--output-file is deprecated; use --output instead");
        }
    }
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
pub struct CompressCommand {
    #[command(flatten)]
    pub common: CommonRewriteArgs,

    /// Compression algorithm for output file: zstd, lz4, or none
    #[arg(long = "compression", value_enum, default_value = "zstd")]
    pub compression: CompressionFormat,

    /// Message order in the output: preserve (keep the input order) or log_time
    #[arg(long = "order", value_enum, default_value = "preserve")]
    pub order: MessageOrder,
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
pub struct DecompressCommand {
    #[command(flatten)]
    pub common: CommonRewriteArgs,

    /// Message order in the output: preserve (keep the input order) or log_time
    #[arg(long = "order", value_enum, default_value = "preserve")]
    pub order: MessageOrder,
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
#[command(arg_required_else_help = true)]
pub struct AddCommand {
    #[command(subcommand)]
    pub command: AddSubcommand,
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
pub struct CatCommand {
    /// One or more local paths to MCAP files. If omitted, reads from stdin.
    pub files: Vec<PathBuf>,

    /// Comma-separated list of topics to include
    #[arg(long = "topics", default_value = "")]
    pub topics: String,

    /// Include messages at or after this time (seconds)
    #[arg(
        long = "start-secs",
        default_value_t = 0,
        conflicts_with = "start_nsecs"
    )]
    pub start_secs: u64,

    /// Include messages at or after this time (nanoseconds)
    #[arg(long = "start-nsecs", default_value_t = 0)]
    pub start_nsecs: u64,

    /// Include messages before this time (seconds)
    #[arg(long = "end-secs", default_value_t = 0, conflicts_with = "end_nsecs")]
    pub end_secs: u64,

    /// Include messages before this time (nanoseconds)
    #[arg(long = "end-nsecs", default_value_t = 0)]
    pub end_nsecs: u64,

    /// Print messages as JSON. Supported message encodings: ros1, protobuf, and json.
    #[arg(long = "json", default_value_t = false)]
    pub json: bool,
}

#[derive(Subcommand, Debug, PartialEq, Eq)]
pub enum AddSubcommand {
    /// Add an attachment to an MCAP file
    Attachment(AddAttachmentCommand),
    /// Add metadata to an MCAP file
    Metadata(AddMetadataCommand),
}

/// Add an attachment to an MCAP file.
///
/// WARNING: this command rewrites the MCAP file in place and can leave it
/// corrupted if interrupted (for example process kill or disk full).
#[derive(clap::Args, Debug, PartialEq, Eq)]
pub struct AddAttachmentCommand {
    /// Local path to the MCAP file
    pub file: PathBuf,

    /// Filename of attachment to add
    #[arg(short = 'f', long = "file")]
    pub attachment_file: PathBuf,

    /// Name of attachment to add (defaults to attachment file path)
    #[arg(short = 'n', long = "name")]
    pub name: Option<String>,

    /// Content type of attachment
    #[arg(long = "content-type", default_value = "application/octet-stream")]
    pub content_type: String,

    /// Attachment log time in nanoseconds or RFC3339 format
    #[arg(long = "log-time")]
    pub log_time: Option<String>,

    /// Attachment creation time in nanoseconds or RFC3339 format
    #[arg(long = "creation-time")]
    pub creation_time: Option<String>,
}

/// Add metadata to an MCAP file.
///
/// WARNING: this command rewrites the MCAP file in place and can leave it
/// corrupted if interrupted (for example process kill or disk full).
#[derive(clap::Args, Debug, PartialEq, Eq)]
pub struct AddMetadataCommand {
    /// Local path to the MCAP file
    pub file: PathBuf,

    /// Name of metadata record to add
    #[arg(short = 'n', long = "name")]
    pub name: String,

    /// Key-value pair in key=value format
    #[arg(short = 'k', long = "key")]
    pub key_values: Vec<String>,
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
#[command(arg_required_else_help = true)]
pub struct GetCommand {
    #[command(subcommand)]
    pub command: GetSubcommand,
}

#[derive(Subcommand, Debug, PartialEq, Eq)]
pub enum GetSubcommand {
    /// Get an attachment by name or offset
    Attachment(GetAttachmentCommand),
    /// Get metadata by name
    Metadata(GetMetadataCommand),
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
pub struct GetAttachmentCommand {
    /// Local path to the MCAP file
    pub file: PathBuf,

    /// Name of attachment to extract
    #[arg(short = 'n', long = "name")]
    pub name: String,

    /// Offset of attachment to extract
    #[arg(long = "offset")]
    pub offset: Option<u64>,

    /// Location to write attachment bytes
    #[arg(short = 'o', long = "output")]
    pub output: Option<PathBuf>,
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
pub struct GetMetadataCommand {
    /// Local path to the MCAP file
    pub file: PathBuf,

    /// Name of metadata record to get
    #[arg(short = 'n', long = "name")]
    pub name: String,
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
#[command(arg_required_else_help = true)]
pub struct ListCommand {
    #[command(subcommand)]
    pub command: ListSubcommand,
}

#[derive(Subcommand, Debug, PartialEq, Eq)]
pub enum ListSubcommand {
    /// List attachments in an MCAP file
    Attachments(ListAttachmentsCommand),
    /// List channels in an MCAP file
    Channels(ListChannelsCommand),
    /// List chunks in an MCAP file
    Chunks(ListChunksCommand),
    /// List metadata in an MCAP file
    Metadata(ListMetadataCommand),
    /// List schemas in an MCAP file
    Schemas(ListSchemasCommand),
}

#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionFormat {
    Zstd,
    Lz4,
    None,
}

impl CompressionFormat {
    /// Convert to the library's compression enum, where `None` means uncompressed.
    pub fn to_compression(self) -> Option<mcap::Compression> {
        match self {
            CompressionFormat::Zstd => Some(mcap::Compression::Zstd),
            CompressionFormat::Lz4 => Some(mcap::Compression::Lz4),
            CompressionFormat::None => None,
        }
    }
}

/// Output message order for the rewrite commands (`filter`, `compress`, `decompress`).
#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MessageOrder {
    /// Keep the input's stored message order.
    #[default]
    #[value(name = "preserve")]
    Preserve,
    /// Sort messages by log time.
    #[value(name = "log_time", alias = "log-time")]
    LogTime,
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
pub struct ConvertCommand {
    /// Local path to the input file
    pub input: PathBuf,

    /// Local path for the destination MCAP file
    pub output: PathBuf,

    /// Compression algorithm for output file: zstd, lz4, or none
    #[arg(long, value_enum, default_value = "zstd")]
    pub compression: CompressionFormat,

    /// Target uncompressed chunk size in bytes
    #[arg(long, default_value_t = mcap::WriteOptions::DEFAULT_CHUNK_SIZE)]
    pub chunk_size: u64,

    /// Disable all output CRC fields
    #[arg(long = "no-crc", default_value_t = false)]
    pub no_crc: bool,

    /// Write records outside of chunks
    #[arg(long = "no-chunks", default_value_t = false)]
    pub no_chunks: bool,
}

#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoalesceChannels {
    Auto,
    Force,
    None,
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
#[command(arg_required_else_help = true)]
pub struct MergeCommand {
    /// One or more local paths to MCAP files
    #[arg(required = true)]
    pub files: Vec<PathBuf>,

    /// Output file path. If omitted, writes to stdout.
    #[arg(short = 'o', long = "output")]
    pub output: Option<PathBuf>,

    /// Deprecated: use --output.
    #[arg(long = "output-file", hide = true)]
    pub output_file: Option<PathBuf>,

    /// Compression algorithm for output file: zstd, lz4, or none
    #[arg(long, value_enum, default_value = "zstd")]
    pub compression: CompressionFormat,

    /// Target uncompressed chunk size in bytes
    #[arg(long, default_value_t = mcap::WriteOptions::DEFAULT_CHUNK_SIZE)]
    pub chunk_size: u64,

    /// Disable all output CRC fields
    #[arg(long = "no-crc", default_value_t = false)]
    pub no_crc: bool,

    /// Write records outside of chunks
    #[arg(long = "no-chunks", default_value_t = false)]
    pub no_chunks: bool,

    /// Allow duplicate-named metadata records in output.
    ///
    /// Identical metadata records are still deduplicated by content.
    #[arg(long, default_value_t = false)]
    pub allow_duplicate_metadata: bool,

    /// Channel coalescing behavior:
    /// - auto: coalesce channels with matching topic, schema, encoding, and
    ///   metadata
    /// - force: same as auto but ignores metadata
    /// - none: do not coalesce channels
    ///
    /// Note: coalescing channels may produce non-monotonic or colliding
    /// sequence values within a coalesced output channel.
    #[arg(long, value_enum, default_value = "auto")]
    pub coalesce_channels: CoalesceChannels,
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
pub struct FileCommand {
    /// Local path to the MCAP file
    pub file: PathBuf,
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
pub struct DuCommand {
    /// Fast approximation using message indexes (skips decompression, may over-count if non-message records are interleaved in chunks)
    #[arg(long, default_value_t = false)]
    pub approximate: bool,

    /// Local path to the MCAP file
    pub file: PathBuf,
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
pub struct FilterCommand {
    #[command(flatten)]
    pub common: CommonRewriteArgs,

    /// Include topics matching this regex (repeatable)
    #[arg(short = 'y', long = "include-topic-regex")]
    pub include_topic_regex: Vec<String>,

    /// Exclude topics matching this regex (repeatable)
    #[arg(short = 'n', long = "exclude-topic-regex")]
    pub exclude_topic_regex: Vec<String>,

    /// Include the last pre-start message for matching topics (repeatable)
    #[arg(short = 'l', long = "last-per-channel-topic-regex")]
    pub last_per_channel_topic_regex: Vec<String>,

    /// Include messages at or after this time (nanos or RFC3339)
    #[arg(short = 'S', long = "start")]
    pub start: Option<String>,

    /// Include messages at or after this time (seconds)
    #[arg(
        short = 's',
        long = "start-secs",
        default_value_t = 0,
        conflicts_with = "start_nsecs"
    )]
    pub start_secs: u64,

    /// Deprecated: include messages at or after this time (nanoseconds)
    #[arg(long = "start-nsecs", default_value_t = 0)]
    pub start_nsecs: u64,

    /// Include messages before this time (nanos or RFC3339)
    #[arg(short = 'E', long = "end")]
    pub end: Option<String>,

    /// Include messages before this time (seconds)
    #[arg(
        short = 'e',
        long = "end-secs",
        default_value_t = 0,
        conflicts_with = "end_nsecs"
    )]
    pub end_secs: u64,

    /// Deprecated: include messages before this time (nanoseconds)
    #[arg(long = "end-nsecs", default_value_t = 0)]
    pub end_nsecs: u64,

    /// Exclude metadata records from the output (metadata is included by default)
    #[arg(long = "exclude-metadata", default_value_t = false)]
    pub exclude_metadata: bool,

    /// Exclude attachments from the output (attachments are included by default)
    #[arg(long = "exclude-attachments", default_value_t = false)]
    pub exclude_attachments: bool,

    /// Deprecated no-op: metadata is included by default. Use --exclude-metadata to drop it.
    #[arg(long = "include-metadata", default_value_t = false, hide = true)]
    pub include_metadata: bool,

    /// Deprecated no-op: attachments are included by default. Use --exclude-attachments to drop them.
    #[arg(long = "include-attachments", default_value_t = false, hide = true)]
    pub include_attachments: bool,

    /// Compression algorithm for output file: zstd, lz4, or none (default: zstd)
    #[arg(long = "compression", value_enum)]
    pub compression: Option<CompressionFormat>,

    /// Deprecated: use --compression. Ignored when --compression is set.
    #[arg(long = "output-compression", value_enum, hide = true)]
    pub output_compression: Option<CompressionFormat>,

    /// Write records outside of chunks
    #[arg(long = "no-chunks", default_value_t = false)]
    pub no_chunks: bool,

    /// Message order in the output: preserve (keep the input order) or log_time
    #[arg(long = "order", value_enum, default_value = "preserve")]
    pub order: MessageOrder,
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
pub struct DoctorCommand {
    /// Require that messages have a monotonic log time
    #[arg(long, default_value_t = false)]
    pub strict_message_order: bool,

    /// Local path to the MCAP file
    pub file: PathBuf,
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
pub struct RecoverCommand {
    /// Input MCAP file path. If omitted, reads from stdin.
    pub file: Option<PathBuf>,

    /// Output MCAP file path. If omitted, writes to stdout.
    #[arg(short = 'o', long = "output")]
    pub output: Option<PathBuf>,

    /// Target uncompressed chunk size for output MCAP
    #[arg(long = "chunk-size", default_value_t = mcap::WriteOptions::DEFAULT_CHUNK_SIZE)]
    pub chunk_size: u64,

    /// Compression algorithm for output file: zstd, lz4, none, or preserve
    ///
    /// `preserve` (the default) keeps the input file's compression (uncompressed if the input is
    /// unchunked).
    #[arg(long = "compression", default_value = "preserve")]
    pub compression: String,
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
pub struct SortCommand {
    #[command(flatten)]
    pub common: CommonRewriteArgs,

    /// Compression algorithm for output file: zstd, lz4, or none
    #[arg(long, value_enum, default_value = "zstd")]
    pub compression: CompressionFormat,

    /// Write records outside of chunks
    #[arg(long = "no-chunks", default_value_t = false)]
    pub no_chunks: bool,

    /// Message order in the output: preserve (keep the input order) or log_time.
    ///
    /// `sort` defaults to log_time; it accepts the same flag as the other rewrite commands so it
    /// can be overridden (and future modes such as publish_time will apply here too).
    #[arg(long = "order", value_enum, default_value = "log_time")]
    pub order: MessageOrder,
}

pub type InfoCommand = FileCommand;
pub type ListAttachmentsCommand = FileCommand;
pub type ListChannelsCommand = FileCommand;
pub type ListChunksCommand = FileCommand;
pub type ListMetadataCommand = FileCommand;
pub type ListSchemasCommand = FileCommand;

/// Parse a CLI-supplied timestamp as either integer nanoseconds or an RFC3339 string.
///
/// Shared by commands that accept timestamps on the command line (for example
/// `add attachment` and `filter`).
pub(crate) fn parse_timestamp_or_nanos(value: &str) -> Result<u64> {
    if let Ok(nanos) = value.parse::<u64>() {
        return Ok(nanos);
    }

    let parsed = chrono::DateTime::parse_from_rfc3339(value)
        .with_context(|| format!("failed to parse timestamp '{value}'"))?;
    let seconds = parsed.timestamp();
    anyhow::ensure!(seconds >= 0, "timestamp is before unix epoch: '{value}'");
    let seconds = seconds as u64;
    let nanos = parsed.timestamp_subsec_nanos() as u64;
    seconds
        .checked_mul(1_000_000_000)
        .and_then(|v| v.checked_add(nanos))
        .with_context(|| format!("timestamp is out of range: '{value}'"))
}

#[cfg(test)]
mod tests {
    use super::parse_timestamp_or_nanos;

    #[test]
    fn library_identifier_pairs_cli_and_crate_identifiers() {
        let library = super::LIBRARY_IDENTIFIER.as_str();
        assert!(library.starts_with("mcap-cli/"));
        assert!(library.ends_with(mcap::LIBRARY_IDENTIFIER));
        assert!(library.contains(" mcap-rust/"));
    }

    #[test]
    fn parses_nanos_or_rfc3339() {
        assert_eq!(parse_timestamp_or_nanos("123").expect("nanos"), 123);
        let ts = parse_timestamp_or_nanos("2023-07-25T15:27:30.132545471Z").expect("rfc3339");
        assert_eq!(ts, 1_690_298_850_132_545_471);
    }

    #[test]
    fn rejects_invalid_timestamp() {
        let err = parse_timestamp_or_nanos("not-a-time").expect_err("invalid time should fail");
        assert!(err.to_string().contains("failed to parse timestamp"));
    }
}
