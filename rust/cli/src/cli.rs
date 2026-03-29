use std::path::PathBuf;

use clap::{ArgAction, Parser, Subcommand};

use crate::logsetup;

#[derive(Parser, Debug, PartialEq, Eq)]
#[command(name = "mcap", bin_name = "mcap")]
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

    /// Config file path (reserved for future implementation)
    #[arg(long, global = true)]
    pub config: Option<PathBuf>,

    /// Record pprof-style profiling output (reserved for future implementation)
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
    Info,
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
    Attachment,
    /// Add metadata to an MCAP file
    Metadata,
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
    Attachment,
    /// Get metadata by name
    Metadata,
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
    Attachments,
    /// List channels in an MCAP file
    Channels,
    /// List chunks in an MCAP file
    Chunks,
    /// List metadata in an MCAP file
    Metadata,
    /// List schemas in an MCAP file
    Schemas,
}

#[derive(clap::Args, Debug, PartialEq, Eq)]
pub struct VersionCommand {
    /// Print MCAP library version instead of CLI version
    #[arg(short = 'l', long = "library", default_value_t = false)]
    pub library: bool,
}
