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
    about = "A command line tool for inspecting and manipulating MCAP files.",
    after_help = help_footer(),
)]
pub struct Args {
    /// When to color log output on stderr
    #[arg(
        short,
        long,
        value_enum,
        default_value_t = logsetup::Color::Auto,
        global = true
    )]
    pub color: logsetup::Color,

    /// Allow whole-file scans or downloads of remote inputs.
    ///
    /// Applies to http(s):// and object-store URLs (s3://, s3a://, gs://, az://, abfs://). Small
    /// bounded indexed reads work without this flag.
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
    /// Add an attachment or metadata record to an existing MCAP file (modifies the file in place).
    Add(AddCommand),
    /// Concatenate the messages in one or more MCAP files to stdout.
    ///
    /// By default prints one line per message (log time, topic, schema name, and a short byte
    /// preview). Use --json to print one JSON object per message instead.
    Cat(CatCommand),
    /// Generate shell completion scripts.
    ///
    /// To load completions in the current shell session:
    ///   bash:       source <(mcap completion bash)
    ///   zsh:        source <(mcap completion zsh)
    ///   fish:       mcap completion fish | source
    ///   powershell: mcap completion powershell | Out-String | Invoke-Expression
    #[command(verbatim_doc_comment, about = "Generate shell completion scripts")]
    Completion(CompletionCommand),
    /// Create a compressed copy of an MCAP file.
    ///
    /// Equivalent to running `mcap filter --compression=zstd`.
    Compress(CompressCommand),
    /// Convert supported input files to MCAP.
    ///
    /// Reads from an input path or URL and writes to an output path; does not use stdin/stdout.
    ///
    /// Supported inputs:
    ///   .bag  ROS 1 bag
    ///   .db3  ROS 2 SQLite db3
    #[command(
        verbatim_doc_comment,
        about = "Convert supported files (ROS 1 .bag, ROS 2 .db3) to MCAP"
    )]
    Convert(ConvertCommand),
    /// Create an uncompressed copy of an MCAP file.
    ///
    /// Equivalent to running `mcap filter --compression=none`.
    Decompress(DecompressCommand),
    /// Check an MCAP file structure.
    ///
    /// Prints diagnostics to stderr; exits non-zero if any structural errors are found.
    Doctor(DoctorCommand),
    /// Compute byte usage statistics for MCAP records.
    Du(DuCommand),
    /// Copy filtered MCAP data to an output file or stdout.
    ///
    /// Copies messages (optionally filtered by topic and time range) plus metadata and
    /// attachments, and can change compression, chunking, and message order. `compress`,
    /// `decompress`, and `sort` are presets over this command.
    Filter(FilterCommand),
    /// Extract an attachment or metadata record from an MCAP file.
    Get(GetCommand),
    /// Report statistics about an MCAP file.
    Info(InfoCommand),
    /// List attachments, channels, chunks, metadata, or schemas in an MCAP file.
    List(ListCommand),
    /// Merge MCAP files.
    ///
    /// Performs a k-way merge of messages from all inputs into a single output, ordered by log time.
    Merge(MergeCommand),
    /// Recover data from a potentially corrupt MCAP file.
    ///
    /// Scans records sequentially, skipping corrupt records and stopping early on truncated or
    /// undecodable data, then writes a valid MCAP with indexes and CRCs rebuilt. Diagnostics go to
    /// stderr; exits with status 3 if any records were discarded or the scan stopped early.
    Recover(RecoverCommand),
    /// Rewrite an MCAP file with messages reordered.
    ///
    /// With default options, equivalent to `mcap filter --order=log_time`.
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
    /// Input MCAP file path or URL. If omitted, reads from stdin.
    pub file: Option<PathBuf>,

    /// Output file path. If omitted, writes to stdout.
    #[arg(short = 'o', long = "output")]
    pub output: Option<PathBuf>,

    /// Deprecated: use --output.
    #[arg(long = "output-file", hide = true)]
    pub output_file: Option<PathBuf>,

    /// Target uncompressed chunk size in bytes
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

    /// Message order in the output: preserve (keep the input order), log_time, or topic
    #[arg(long = "order", value_enum, default_value = "preserve")]
    pub order: MessageOrder,
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
pub struct DecompressCommand {
    #[command(flatten)]
    pub common: CommonRewriteArgs,

    /// Message order in the output: preserve (keep the input order), log_time, or topic
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
    /// One or more paths or URLs to MCAP files. If omitted, reads from stdin.
    pub files: Vec<PathBuf>,

    /// Comma-separated list of topics to include (exact match). If empty (the default), all topics are included.
    #[arg(long = "topics", default_value = "", hide_default_value = true)]
    pub topics: String,

    /// Include messages with log time at or after this time (seconds)
    #[arg(
        long = "start-secs",
        default_value_t = 0,
        conflicts_with = "start_nsecs"
    )]
    pub start_secs: u64,

    /// Include messages with log time at or after this time (nanoseconds)
    #[arg(long = "start-nsecs", default_value_t = 0)]
    pub start_nsecs: u64,

    /// Include messages with log time before this time (seconds); 0 (the default) means no upper bound
    #[arg(long = "end-secs", default_value_t = 0, conflicts_with = "end_nsecs")]
    pub end_secs: u64,

    /// Include messages with log time before this time (nanoseconds)
    #[arg(long = "end-nsecs", default_value_t = 0)]
    pub end_nsecs: u64,

    /// Print messages as JSON, one object per message. Supported schema encodings: ros1msg, protobuf, and jsonschema (or schemaless channels with json message encoding); other encodings error.
    #[arg(long = "json", default_value_t = false)]
    pub json: bool,
}

#[derive(Subcommand, Debug, PartialEq, Eq)]
pub enum AddSubcommand {
    /// Add an attachment to an MCAP file.
    ///
    /// Rewrites FILE in place. WARNING: interrupting this (for example a process kill or disk full)
    /// can leave FILE corrupted.
    Attachment(AddAttachmentCommand),
    /// Add metadata to an MCAP file.
    ///
    /// Rewrites FILE in place. WARNING: interrupting this (for example a process kill or disk full)
    /// can leave FILE corrupted.
    Metadata(AddMetadataCommand),
}

/// Arguments for `add attachment`.
#[derive(clap::Args, Debug, PartialEq, Eq)]
pub struct AddAttachmentCommand {
    /// Local path to the MCAP file
    pub file: PathBuf,

    /// Path to the local file to attach
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

/// Arguments for `add metadata`.
#[derive(clap::Args, Debug, PartialEq, Eq)]
pub struct AddMetadataCommand {
    /// Local path to the MCAP file
    pub file: PathBuf,

    /// Name of metadata record to add
    #[arg(short = 'n', long = "name")]
    pub name: String,

    /// Key-value pair in key=value format (repeatable)
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
    /// Get an attachment by name.
    Attachment(GetAttachmentCommand),
    /// Get metadata by name.
    Metadata(GetMetadataCommand),
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
pub struct GetAttachmentCommand {
    /// Path or URL to the MCAP file
    pub file: PathBuf,

    /// Name of attachment to extract
    #[arg(short = 'n', long = "name")]
    pub name: String,

    /// Byte offset of the attachment record (from `mcap list attachments`), used to disambiguate multiple attachments that share the same name
    #[arg(long = "offset")]
    pub offset: Option<u64>,

    /// Location to write attachment bytes. If omitted, writes to stdout (refuses if stdout is a terminal)
    #[arg(short = 'o', long = "output")]
    pub output: Option<PathBuf>,
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
pub struct GetMetadataCommand {
    /// Path or URL to the MCAP file
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
    /// List attachments in an MCAP file.
    Attachments(ListAttachmentsCommand),
    /// List channels in an MCAP file.
    Channels(ListChannelsCommand),
    /// List chunks in an MCAP file.
    Chunks(ListChunksCommand),
    /// List metadata in an MCAP file.
    Metadata(ListMetadataCommand),
    /// List schemas in an MCAP file.
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

/// Output message order for the rewrite commands (`filter`, `compress`, `decompress`, `sort`).
#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MessageOrder {
    /// Keep the input's stored message order.
    #[default]
    #[value(name = "preserve")]
    Preserve,
    /// Sort messages by ascending log time.
    #[value(name = "log_time", alias = "log-time")]
    LogTime,
    /// Group each channel's messages together (channels ordered by topic name, then channel ID),
    /// placing every channel in its own chunk(s) with its messages in ascending log time. This lets
    /// a single-topic reader fetch one contiguous byte range instead of scanning the whole file.
    /// Buffers all selected messages in memory while reordering.
    #[value(name = "topic")]
    Topic,
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
pub struct ConvertCommand {
    /// Path or URL to the input file
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

    /// Write records outside of chunks (ignores --chunk-size and --compression)
    #[arg(long = "no-chunks", default_value_t = false)]
    pub no_chunks: bool,
}

#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoalesceChannels {
    /// Coalesce channels with matching topic, schema, encoding, and metadata
    Auto,
    /// Like auto, but ignore metadata differences
    Force,
    /// Do not coalesce channels
    None,
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
#[command(arg_required_else_help = true)]
pub struct MergeCommand {
    /// One or more paths or URLs to MCAP files
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

    /// Write records outside of chunks (ignores --chunk-size and --compression)
    #[arg(long = "no-chunks", default_value_t = false)]
    pub no_chunks: bool,

    /// Allow duplicate-named metadata records in output.
    ///
    /// Identical metadata records are still deduplicated by content.
    #[arg(long, default_value_t = false)]
    pub allow_duplicate_metadata: bool,

    /// Channel coalescing behavior.
    ///
    /// Note: coalescing channels may produce non-monotonic or colliding sequence values within a
    /// coalesced output channel.
    #[arg(long, value_enum, default_value = "auto")]
    pub coalesce_channels: CoalesceChannels,
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
pub struct FileCommand {
    /// Path or URL to the MCAP file
    pub file: PathBuf,
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
pub struct DuCommand {
    /// Fast approximation using message indexes (skips decompression, may over-count if non-message records are interleaved in chunks)
    #[arg(long, default_value_t = false)]
    pub approximate: bool,

    /// Path or URL to the MCAP file
    pub file: PathBuf,
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
pub struct FilterCommand {
    #[command(flatten)]
    pub common: CommonRewriteArgs,

    /// Include only topics matching this regex, matched against the full topic name (repeatable; cannot be combined with --exclude-topic-regex)
    #[arg(short = 'y', long = "include-topic-regex")]
    pub include_topic_regex: Vec<String>,

    /// Exclude topics matching this regex, matched against the full topic name (repeatable; cannot be combined with --include-topic-regex)
    #[arg(short = 'n', long = "exclude-topic-regex")]
    pub exclude_topic_regex: Vec<String>,

    /// For topics matching this regex, also include the most recent message before the start time for each channel (captures latched/initial state). No effect when the start time is unset or 0. Requires an indexed input (errors on non-indexed files). Repeatable.
    #[arg(short = 'l', long = "last-per-channel-topic-regex")]
    pub last_per_channel_topic_regex: Vec<String>,

    /// Include messages with log time at or after this time (nanoseconds since the Unix epoch, or RFC3339). If omitted, starts at the beginning.
    #[arg(short = 'S', long = "start")]
    pub start: Option<String>,

    /// Include messages with log time at or after this time (seconds)
    #[arg(
        short = 's',
        long = "start-secs",
        default_value_t = 0,
        conflicts_with = "start_nsecs"
    )]
    pub start_secs: u64,

    /// Deprecated: include messages with log time at or after this time (nanoseconds)
    #[arg(long = "start-nsecs", default_value_t = 0, hide = true)]
    pub start_nsecs: u64,

    /// Include messages with log time before this time (nanoseconds since the Unix epoch, or RFC3339). If omitted, no upper bound.
    #[arg(short = 'E', long = "end")]
    pub end: Option<String>,

    /// Include messages with log time before this time (seconds); 0 (the default) means no upper bound
    #[arg(
        short = 'e',
        long = "end-secs",
        default_value_t = 0,
        conflicts_with = "end_nsecs"
    )]
    pub end_secs: u64,

    /// Deprecated: include messages with log time before this time (nanoseconds)
    #[arg(long = "end-nsecs", default_value_t = 0, hide = true)]
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

    /// Write records outside of chunks (ignores --chunk-size and --compression)
    #[arg(long = "no-chunks", default_value_t = false)]
    pub no_chunks: bool,

    /// Message order in the output: preserve (keep the input order), log_time, or topic
    #[arg(long = "order", value_enum, default_value = "preserve")]
    pub order: MessageOrder,
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
pub struct DoctorCommand {
    /// Treat decreasing message log times as errors (non-zero exit) instead of warnings
    #[arg(long, default_value_t = false)]
    pub strict_message_order: bool,

    /// Path or URL to the MCAP file
    pub file: PathBuf,
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
pub struct RecoverCommand {
    /// Input MCAP file path or URL. If omitted, reads from stdin.
    pub file: Option<PathBuf>,

    /// Output MCAP file path. If omitted, writes to stdout.
    #[arg(short = 'o', long = "output")]
    pub output: Option<PathBuf>,

    /// Target uncompressed chunk size in bytes
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

    /// Write records outside of chunks (ignores --chunk-size and --compression)
    #[arg(long = "no-chunks", default_value_t = false)]
    pub no_chunks: bool,

    /// Message order in the output (defaults to log_time for `sort`).
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

/// Footer shown at the bottom of the root `mcap --help`.
///
/// Built as a `StyledStr` so the heading uses clap's default section-header style
/// (bold + underline), matching the generated `Commands:`/`Options:` headings.
/// clap strips the embedded styling when color is disabled or the output isn't a terminal.
fn help_footer() -> clap::builder::StyledStr {
    use std::fmt::Write as _;

    use clap::builder::styling::Style;

    const HEADER: Style = Style::new().bold().underline();

    let mut footer = clap::builder::StyledStr::new();
    let _ = write!(
        footer,
        "{HEADER}Learn more:{HEADER:#}\n  \
         Homepage       https://mcap.dev\n  \
         Specification  https://mcap.dev/spec\n\n\
         MCAP is an open source project by Foxglove (https://foxglove.dev)."
    );
    footer
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
