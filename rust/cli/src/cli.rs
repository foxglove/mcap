use std::path::PathBuf;

use clap::{ArgAction, Parser, Subcommand};

use crate::logsetup;

#[derive(Parser, Debug, PartialEq, Eq)]
#[command(name = "mcap", bin_name = "mcap")]
pub struct ArgsRoot {
    /// Verbosity (-v, -vv, -vvv, etc.)
    #[arg(short, long, action = ArgAction::Count, global = true)]
    pub verbose: u8,

    #[arg(
        short,
        long,
        value_enum,
        default_value_t = logsetup::Color::Auto,
        global = true
    )]
    pub color: logsetup::Color,

    /// Config file path (reserved for future implementation)
    #[arg(long, global = true)]
    pub config: Option<PathBuf>,

    /// Record pprof-style profiling output (reserved for future implementation)
    #[arg(long, default_value_t = false, global = true)]
    pub pprof_profile: bool,

    #[command(subcommand)]
    pub command: Command,
}

pub type Args = ArgsRoot;

#[derive(Subcommand, Debug, PartialEq, Eq)]
pub enum Command {
    /// Add records to an existing MCAP file
    Add(AddCommand),
    /// Concatenate the messages in one or more MCAP files to stdout
    Cat(CatArgs),
    /// Create a compressed copy of an MCAP file
    Compress(CompressArgs),
    /// Convert an input recording file to MCAP
    Convert(ConvertArgs),
    /// Create an uncompressed copy of an MCAP file
    Decompress(DecompressArgs),
    /// Check an MCAP file structure
    Doctor(DoctorArgs),
    /// Compute byte usage statistics for MCAP records
    Du(DuArgs),
    /// Copy filtered MCAP data to a new file
    Filter(FilterArgs),
    /// Get a record from an MCAP file
    Get(GetCommand),
    /// Report statistics about an MCAP file
    Info(InputFile),
    /// List records of an MCAP file
    List(ListCommand),
    /// Merge a selection of MCAP files by record timestamp
    Merge(MergeArgs),
    /// Recover data from a potentially corrupt MCAP file
    Recover(RecoverArgs),
    /// Read an MCAP file and write messages sorted by log time
    Sort(SortArgs),
    /// Output version information
    Version(VersionCommand),
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
#[command(arg_required_else_help = true)]
pub struct AddCommand {
    #[command(subcommand)]
    pub command: AddSubcommand,
}

#[derive(Subcommand, Debug, PartialEq, Eq)]
pub enum AddSubcommand {
    /// Add an attachment to an MCAP file
    Attachment(AddAttachmentArgs),
    /// Add metadata to an MCAP file
    Metadata(AddMetadataArgs),
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
    Attachment(GetAttachmentArgs),
    /// Get metadata by name
    Metadata(GetMetadataArgs),
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
    Attachments(InputFile),
    /// List channels in an MCAP file
    Channels(InputFile),
    /// List chunks in an MCAP file
    Chunks(InputFile),
    /// List metadata in an MCAP file
    Metadata(InputFile),
    /// List schemas in an MCAP file
    Schemas(InputFile),
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
pub struct InputFile {
    /// Input MCAP file path
    pub file: PathBuf,
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
pub struct CatArgs {
    /// Input MCAP files. If omitted, reads from stdin when piped.
    pub files: Vec<PathBuf>,
    /// Comma-separated list of topics to include
    #[arg(long = "topics")]
    pub topics: Option<String>,
    /// Start time (RFC3339 or integer nanoseconds)
    #[arg(long = "start")]
    pub start: Option<String>,
    /// End time (RFC3339 or integer nanoseconds)
    #[arg(long = "end")]
    pub end: Option<String>,
    /// Print messages as newline-delimited JSON
    #[arg(long = "json", default_value_t = false)]
    pub json: bool,
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
pub struct FilterArgs {
    /// Input MCAP file path (reads stdin when omitted and piped)
    pub file: Option<PathBuf>,
    /// Output MCAP file path (stdout requires redirection)
    #[arg(short = 'o', long = "output")]
    pub output: Option<PathBuf>,
    /// Include messages with topics matching regex (repeatable)
    #[arg(short = 'y', long = "include-topic-regex")]
    pub include_topic_regex: Vec<String>,
    /// Exclude messages with topics matching regex (repeatable)
    #[arg(short = 'n', long = "exclude-topic-regex")]
    pub exclude_topic_regex: Vec<String>,
    /// Include metadata records in output
    #[arg(long = "include-metadata", default_value_t = false)]
    pub include_metadata: bool,
    /// Include attachment records in output
    #[arg(long = "include-attachments", default_value_t = false)]
    pub include_attachments: bool,
    /// Start time (RFC3339 or integer nanoseconds)
    #[arg(long = "start")]
    pub start: Option<String>,
    /// End time (RFC3339 or integer nanoseconds)
    #[arg(long = "end")]
    pub end: Option<String>,
    /// Output compression (zstd, lz4, none)
    #[arg(long = "output-compression", default_value = "zstd")]
    pub output_compression: String,
    /// Chunk size for output file
    #[arg(long = "chunk-size", default_value_t = 4 * 1024 * 1024)]
    pub chunk_size: u64,
    /// Write output unchunked
    #[arg(long = "unchunked", default_value_t = false)]
    pub unchunked: bool,
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
pub struct CompressArgs {
    /// Input MCAP file path (reads stdin when omitted and piped)
    pub file: Option<PathBuf>,
    /// Output MCAP file path (stdout requires redirection)
    #[arg(short = 'o', long = "output")]
    pub output: Option<PathBuf>,
    /// Compression algorithm (zstd, lz4, none)
    #[arg(long = "compression", default_value = "zstd")]
    pub compression: String,
    /// Chunk size for output file
    #[arg(long = "chunk-size", default_value_t = 4 * 1024 * 1024)]
    pub chunk_size: u64,
    /// Write output unchunked
    #[arg(long = "unchunked", default_value_t = false)]
    pub unchunked: bool,
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
pub struct DecompressArgs {
    /// Input MCAP file path (reads stdin when omitted and piped)
    pub file: Option<PathBuf>,
    /// Output MCAP file path (stdout requires redirection)
    #[arg(short = 'o', long = "output")]
    pub output: Option<PathBuf>,
    /// Chunk size for output file
    #[arg(long = "chunk-size", default_value_t = 4 * 1024 * 1024)]
    pub chunk_size: u64,
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
pub struct ConvertArgs {
    /// Input file path (.mcap, .bag, or .db3)
    pub input: PathBuf,
    /// Output MCAP file path
    pub output: PathBuf,
    /// (ROS 2 db3 only) prefix path(s) for message definitions
    #[arg(long = "ament-prefix-path")]
    pub ament_prefix_path: Option<String>,
    /// Chunk compression algorithm (zstd, lz4, none)
    #[arg(long = "compression", default_value = "zstd")]
    pub compression: String,
    /// Chunk size target for output file
    #[arg(long = "chunk-size", default_value_t = 8 * 1024 * 1024)]
    pub chunk_size: u64,
    /// Include chunk CRC checksums in output
    #[arg(long = "include-crc", default_value_t = true, action = ArgAction::Set)]
    pub include_crc: bool,
    /// Chunk the output file
    #[arg(long = "chunked", default_value_t = true, action = ArgAction::Set)]
    pub chunked: bool,
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
pub struct DoctorArgs {
    /// Input MCAP file path
    pub file: PathBuf,
    /// Require monotonic message log time ordering
    #[arg(long = "strict-message-order", default_value_t = false)]
    pub strict_message_order: bool,
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
pub struct DuArgs {
    /// Input MCAP file path
    pub file: PathBuf,
    /// Fast approximation that skips chunk decompression
    #[arg(long = "approximate", default_value_t = false)]
    pub approximate: bool,
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
pub struct SortArgs {
    /// Input MCAP file path
    pub file: PathBuf,
    /// Output MCAP file path
    #[arg(short = 'o', long = "output-file")]
    pub output_file: PathBuf,
    /// Chunk size for output file
    #[arg(long = "chunk-size", default_value_t = 4 * 1024 * 1024)]
    pub chunk_size: u64,
    /// Compression algorithm (zstd, lz4, none)
    #[arg(long = "compression", default_value = "zstd")]
    pub compression: String,
    /// Include chunk CRCs in output
    #[arg(long = "include-crc", default_value_t = true, action = ArgAction::Set)]
    pub include_crc: bool,
    /// Create an indexed and chunk-compressed output
    #[arg(long = "chunked", default_value_t = true, action = ArgAction::Set)]
    pub chunked: bool,
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
pub struct MergeArgs {
    /// Input MCAP files to merge
    pub files: Vec<PathBuf>,
    /// Output MCAP file path
    #[arg(short = 'o', long = "output-file")]
    pub output_file: PathBuf,
    /// Chunk size for output file
    #[arg(long = "chunk-size", default_value_t = 4 * 1024 * 1024)]
    pub chunk_size: u64,
    /// Compression algorithm (zstd, lz4, none)
    #[arg(long = "compression", default_value = "zstd")]
    pub compression: String,
    /// Include chunk CRCs in output
    #[arg(long = "include-crc", default_value_t = true, action = ArgAction::Set)]
    pub include_crc: bool,
    /// Create an indexed and chunk-compressed output
    #[arg(long = "chunked", default_value_t = true, action = ArgAction::Set)]
    pub chunked: bool,
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
pub struct RecoverArgs {
    /// Input MCAP file path
    pub file: PathBuf,
    /// Output MCAP file path
    #[arg(short = 'o', long = "output")]
    pub output: PathBuf,
    /// Chunk size for recovered output
    #[arg(long = "chunk-size", default_value_t = 4 * 1024 * 1024)]
    pub chunk_size: u64,
    /// Compression algorithm (zstd, lz4, none)
    #[arg(long = "compression", default_value = "zstd")]
    pub compression: String,
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
pub struct GetAttachmentArgs {
    /// Input MCAP file path
    pub file: PathBuf,
    /// Name of attachment to extract
    #[arg(short = 'n', long = "name")]
    pub name: String,
    /// Optional attachment offset when multiple names match
    #[arg(long = "offset")]
    pub offset: Option<u64>,
    /// Optional output path (stdout requires redirection)
    #[arg(short = 'o', long = "output")]
    pub output: Option<PathBuf>,
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
pub struct GetMetadataArgs {
    /// Input MCAP file path
    pub file: PathBuf,
    /// Name of metadata record to fetch
    #[arg(short = 'n', long = "name")]
    pub name: String,
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
pub struct AddAttachmentArgs {
    /// Input MCAP file path to mutate
    pub file: PathBuf,
    /// Attachment payload file path
    #[arg(short = 'f', long = "file")]
    pub attachment_file: PathBuf,
    /// Attachment name (defaults to attachment filename)
    #[arg(short = 'n', long = "name")]
    pub name: Option<String>,
    /// Attachment media type
    #[arg(long = "content-type", default_value = "application/octet-stream")]
    pub content_type: String,
    /// Attachment log time (RFC3339 or integer nanoseconds)
    #[arg(long = "log-time")]
    pub log_time: Option<String>,
    /// Attachment creation time (RFC3339 or integer nanoseconds)
    #[arg(long = "creation-time")]
    pub creation_time: Option<String>,
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
pub struct AddMetadataArgs {
    /// Input MCAP file path to mutate
    pub file: PathBuf,
    /// Metadata record name
    #[arg(short = 'n', long = "name")]
    pub name: String,
    /// Key-value pair in the form key=value (repeatable)
    #[arg(short = 'k', long = "key", value_name = "KEY=VALUE")]
    pub key_values: Vec<String>,
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
pub struct VersionCommand {
    /// Print MCAP library version instead of CLI version
    #[arg(short = 'l', long = "library", default_value_t = false)]
    pub library: bool,
}
