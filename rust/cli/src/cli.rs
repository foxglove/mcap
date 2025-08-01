use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

use crate::commands;

#[derive(Parser)]
#[command(name = "mcap")]
#[command(about = "🔍 Officially the top-rated CLI tool for slicing and dicing MCAP files")]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(author = "Foxglove Technologies")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,

    /// Config file path
    #[arg(long, global = true)]
    pub config: Option<PathBuf>,

    /// Enable performance profiling (CPU, memory, blocking)
    #[arg(long, global = true)]
    pub pprof_profile: bool,

    /// Verbose output
    #[command(flatten)]
    pub verbose: clap_verbosity_flag::Verbosity,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Report statistics about an MCAP file
    Info(commands::info::InfoArgs),

    /// Concatenate messages from MCAP files to stdout
    Cat(commands::cat::CatArgs),

    /// Filter MCAP data to a new file
    Filter(commands::filter::FilterArgs),

    /// Merge multiple MCAP files
    Merge(commands::merge::MergeArgs),

    /// Check MCAP file structure for errors
    Doctor(commands::doctor::DoctorArgs),

    /// Convert between different formats
    Convert(commands::convert::ConvertArgs),

    /// Sort messages in an MCAP file
    Sort(commands::sort::SortArgs),

    /// Recover data from corrupted MCAP files
    Recover(commands::recover::RecoverArgs),

    /// List records in an MCAP file
    List {
        #[command(subcommand)]
        command: ListCommands,
    },

    /// Get records from an MCAP file
    Get {
        #[command(subcommand)]
        command: GetCommands,
    },

    /// Add records to an MCAP file
    Add {
        #[command(subcommand)]
        command: AddCommands,
    },

    /// Compress an MCAP file
    Compress(commands::compression::CompressArgs),

    /// Decompress an MCAP file
    Decompress(commands::compression::DecompressArgs),

    /// Show disk usage of MCAP file components
    Du(commands::du::DuArgs),

    /// Show version information
    Version(commands::version::VersionArgs),
}

#[derive(Subcommand)]
pub enum ListCommands {
    /// List channels in an MCAP file
    Channels(commands::list::channels::ChannelsArgs),

    /// List chunks in an MCAP file
    Chunks(commands::list::chunks::ChunksArgs),

    /// List attachments in an MCAP file
    Attachments(commands::list::attachments::AttachmentsArgs),

    /// List schemas in an MCAP file
    Schemas(commands::list::schemas::SchemasArgs),
}

#[derive(Subcommand)]
pub enum GetCommands {
    /// Extract an attachment from an MCAP file
    Attachment(commands::get::attachment::AttachmentArgs),
}

#[derive(Subcommand)]
pub enum AddCommands {
    /// Add an attachment to an MCAP file
    Attachment(commands::add::attachment::AttachmentArgs),

    /// Add metadata to an MCAP file
    Metadata(commands::add::metadata::MetadataArgs),
}

impl Cli {
    pub async fn execute(self) -> Result<()> {
        // Set up profiling if requested
        let _profiler = if self.pprof_profile {
            Some(crate::utils::profiler::start_profiling()?)
        } else {
            None
        };

        match self.command {
            Commands::Info(args) => commands::info::run(args).await,
            Commands::Cat(args) => commands::cat::run(args).await,
            Commands::Filter(args) => commands::filter::run(args).await,
            Commands::Merge(args) => commands::merge::run(args).await,
            Commands::Doctor(args) => commands::doctor::run(args).await,
            Commands::Convert(args) => commands::convert::run(args).await,
            Commands::Sort(args) => commands::sort::run(args).await,
            Commands::Recover(args) => commands::recover::run(args).await,

            Commands::List { command } => match command {
                ListCommands::Channels(args) => commands::list::channels::run(args).await,
                ListCommands::Chunks(args) => commands::list::chunks::run(args).await,
                ListCommands::Attachments(args) => commands::list::attachments::run(args).await,
                ListCommands::Schemas(args) => commands::list::schemas::run(args).await,
            },

            Commands::Get { command } => match command {
                GetCommands::Attachment(args) => commands::get::attachment::run(args).await,
            },

            Commands::Add { command } => match command {
                AddCommands::Attachment(args) => commands::add::attachment::run(args).await,
                AddCommands::Metadata(args) => commands::add::metadata::run(args).await,
            },

            Commands::Compress(args) => commands::compression::compress(args).await,
            Commands::Decompress(args) => commands::compression::decompress(args).await,
            Commands::Du(args) => commands::du::run(args).await,
            Commands::Version(args) => commands::version::run(args).await,
        }
    }
}
