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
    Cat,
    /// Create a compressed copy of an MCAP file
    Compress,
    /// Convert a bag file to an MCAP file
    Convert,
    /// Create an uncompressed copy of an MCAP file
    Decompress,
    /// Check an MCAP file structure
    Doctor,
    /// Compute byte usage statistics for MCAP records
    Du,
    /// Copy filtered MCAP data to a new file
    Filter,
    /// Get a record from an MCAP file
    Get(GetCommand),
    /// Report statistics about an MCAP file
    Info(InputFile),
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
