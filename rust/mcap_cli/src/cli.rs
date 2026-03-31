use std::path::PathBuf;

use clap::{ArgAction, Parser, Subcommand};

use crate::logsetup;

#[derive(Parser, Debug, PartialEq, Eq)]
#[command(name = "mcap", bin_name = "mcap", version = env!("CARGO_PKG_VERSION"))]
pub struct Args {
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

    /// Config file path
    #[arg(long, global = true)]
    pub config: Option<PathBuf>,

    /// Record pprof-style profiling output
    #[arg(long, default_value_t = false, global = true)]
    pub pprof_profile: bool,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug, PartialEq, Eq)]
pub enum Command {
    /// Add records to an existing MCAP file
    Add(AddCommand),
    /// Concatenate the messages in one or more MCAP files to stdout
    Cat(CatCommand),
    /// Create a compressed copy of an MCAP file
    Compress,
    /// Convert a bag file to an MCAP file
    Convert,
    /// Create an uncompressed copy of an MCAP file
    Decompress,
    /// Check an MCAP file structure
    Doctor,
    /// Compute byte usage statistics for MCAP records
    Du(DuCommand),
    /// Copy filtered MCAP data to a new file
    Filter,
    /// Get a record from an MCAP file
    Get(GetCommand),
    /// Report statistics about an MCAP file
    Info(InfoCommand),
    /// List records of an MCAP file
    List(ListCommand),
    /// Merge a selection of MCAP files by record timestamp
    Merge,
    /// Recover data from a potentially corrupt MCAP file
    Recover,
    /// Read an MCAP file and write messages sorted by log time
    Sort,
    /// Output version information
    Version(VersionCommand),
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
#[command(arg_required_else_help = true)]
pub struct AddCommand {
    #[command(subcommand)]
    pub command: AddSubcommand,
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
#[command(arg_required_else_help = true)]
pub struct CatCommand {
    /// One or more local paths to MCAP files
    pub files: Vec<PathBuf>,
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

#[derive(clap::Args, Debug, PartialEq, Eq)]
pub struct VersionCommand {
    /// Print MCAP library version instead of CLI version
    #[arg(short = 'l', long = "library", default_value_t = false)]
    pub library: bool,
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

pub type InfoCommand = FileCommand;
pub type ListAttachmentsCommand = FileCommand;
pub type ListChannelsCommand = FileCommand;
pub type ListChunksCommand = FileCommand;
pub type ListMetadataCommand = FileCommand;
pub type ListSchemasCommand = FileCommand;
