use std::path::PathBuf;
use std::sync::LazyLock;

use clap::{ArgAction, Parser, Subcommand};

use crate::logsetup;

pub(crate) static VERSION: LazyLock<String> = LazyLock::new(|| {
    format!(
        "{} ({}) mcap-rust/{}",
        env!("CARGO_PKG_VERSION"),
        env!("GIT_SHA"),
        mcap::VERSION
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

    /// Config file path
    #[arg(long, global = true)]
    pub config: Option<PathBuf>,

    /// Record pprof-style profiling output
    #[arg(long, default_value_t = false, global = true)]
    pub pprof_profile: bool,

    /// Allow commands to download/scan remote inputs or fetch remote message chunk payloads
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
    /// Merge a selection of MCAP files by record timestamp
    Merge(MergeCommand),
    /// Recover data from a potentially corrupt MCAP file
    Recover(RecoverCommand),
    /// Read an MCAP file and write messages sorted by log time
    Sort(SortCommand),
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
pub struct CompressCommand {
    /// Input MCAP file path. If omitted, reads from stdin.
    pub file: Option<PathBuf>,

    /// Output file path. If omitted, writes to stdout.
    #[arg(short = 'o', long = "output")]
    pub output: Option<PathBuf>,

    /// Target uncompressed chunk size for output
    #[arg(long = "chunk-size", default_value_t = 4 * 1024 * 1024)]
    pub chunk_size: u64,

    /// Compression algorithm for output file: zstd, lz4, or none
    #[arg(long = "compression", default_value = "zstd")]
    pub compression: String,

    /// Do not chunk the output file
    #[arg(long = "unchunked", default_value_t = false)]
    pub unchunked: bool,
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
pub struct DecompressCommand {
    /// Input MCAP file path. If omitted, reads from stdin.
    pub file: Option<PathBuf>,

    /// Output file path. If omitted, writes to stdout.
    #[arg(short = 'o', long = "output")]
    pub output: Option<PathBuf>,

    /// Target uncompressed chunk size for output
    #[arg(long = "chunk-size", default_value_t = 4 * 1024 * 1024)]
    pub chunk_size: u64,
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

#[derive(clap::Args, Debug, PartialEq, Eq)]
pub struct ConvertCommand {
    /// Local path to the input file
    pub input: PathBuf,

    /// Local path for the destination MCAP file
    pub output: PathBuf,

    /// Chunk compression algorithm for output MCAP
    #[arg(long, value_enum, default_value = "zstd")]
    pub compression: CompressionFormat,

    /// Target uncompressed chunk size in bytes
    #[arg(long, default_value_t = 8 * 1024 * 1024)]
    pub chunk_size: u64,

    /// Include chunk CRC checksums in output MCAP.
    ///
    /// Accepts bare `--include-crc` and explicit `--include-crc=<bool>`.
    #[arg(
        long,
        action = ArgAction::Set,
        num_args = 0..=1,
        require_equals = true,
        default_missing_value = "true",
        default_value_t = true
    )]
    pub include_crc: bool,

    /// Enable chunked output MCAP writing.
    ///
    /// Accepts bare `--chunked` and explicit `--chunked=<bool>`.
    #[arg(
        long,
        action = ArgAction::Set,
        num_args = 0..=1,
        require_equals = true,
        default_missing_value = "true",
        default_value_t = true
    )]
    pub chunked: bool,
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
    #[arg(short = 'o', long = "output-file")]
    pub output_file: Option<PathBuf>,

    /// Chunk compression algorithm for output MCAP
    #[arg(long, value_enum, default_value = "zstd")]
    pub compression: CompressionFormat,

    /// Target uncompressed chunk size in bytes
    #[arg(long, default_value_t = 8 * 1024 * 1024)]
    pub chunk_size: u64,

    /// Include chunk CRC checksums in output MCAP.
    ///
    /// Accepts bare `--include-crc` and explicit `--include-crc=<bool>`.
    #[arg(
        long,
        action = ArgAction::Set,
        num_args = 0..=1,
        require_equals = true,
        default_missing_value = "true",
        default_value_t = true
    )]
    pub include_crc: bool,

    /// Enable chunked output MCAP writing.
    ///
    /// Accepts bare `--chunked` and explicit `--chunked=<bool>`.
    #[arg(
        long,
        action = ArgAction::Set,
        num_args = 0..=1,
        require_equals = true,
        default_missing_value = "true",
        default_value_t = true
    )]
    pub chunked: bool,

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
    /// sequence values within a coalesced output channel (matches Go CLI
    /// behavior).
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
    /// Input MCAP file path. If omitted, reads from stdin.
    pub file: Option<PathBuf>,

    /// Output file path. If omitted, writes to stdout.
    #[arg(short = 'o', long = "output")]
    pub output: Option<PathBuf>,

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

    /// Include metadata records in output
    #[arg(long = "include-metadata", default_value_t = false)]
    pub include_metadata: bool,

    /// Include attachments in output
    #[arg(long = "include-attachments", default_value_t = false)]
    pub include_attachments: bool,

    /// Compression algorithm for output file: zstd, lz4, or none
    #[arg(long = "output-compression", default_value = "zstd")]
    pub output_compression: String,

    /// Target uncompressed chunk size for output
    #[arg(long = "chunk-size", default_value_t = 4 * 1024 * 1024)]
    pub chunk_size: u64,
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
    #[arg(long = "chunk-size", default_value_t = 4 * 1024 * 1024)]
    pub chunk_size: u64,

    /// Compression for the output file: preserve, none, zstd, or lz4.
    ///
    /// `preserve` (the default) keeps the input file's compression (uncompressed if the input is
    /// unchunked).
    #[arg(long = "compression", default_value = "preserve")]
    pub compression: String,
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
pub struct SortCommand {
    /// Local path to the source MCAP file
    pub file: PathBuf,

    /// Local path for the destination sorted MCAP file
    #[arg(short = 'o', long = "output-file")]
    pub output_file: PathBuf,

    /// Chunk compression algorithm for output MCAP: zstd, lz4, or none
    #[arg(long, value_enum, default_value = "zstd")]
    pub compression: CompressionFormat,

    /// Target uncompressed chunk size in bytes
    #[arg(long, default_value_t = 4 * 1024 * 1024)]
    pub chunk_size: u64,

    /// Include chunk CRC checksums in output MCAP.
    ///
    /// Accepts bare `--include-crc` and explicit `--include-crc=<bool>`.
    #[arg(
        long,
        action = ArgAction::Set,
        num_args = 0..=1,
        require_equals = true,
        default_missing_value = "true",
        default_value_t = true
    )]
    pub include_crc: bool,

    /// Enable chunked output MCAP writing.
    ///
    /// Accepts bare `--chunked` and explicit `--chunked=<bool>`.
    #[arg(
        long,
        action = ArgAction::Set,
        num_args = 0..=1,
        require_equals = true,
        default_missing_value = "true",
        default_value_t = true
    )]
    pub chunked: bool,
}

pub type InfoCommand = FileCommand;
pub type ListAttachmentsCommand = FileCommand;
pub type ListChannelsCommand = FileCommand;
pub type ListChunksCommand = FileCommand;
pub type ListMetadataCommand = FileCommand;
pub type ListSchemasCommand = FileCommand;
