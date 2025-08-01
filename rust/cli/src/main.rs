use anyhow::Result;
use clap::Parser;

mod cli;
mod commands;
mod utils;

use cli::Cli;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging based on verbosity
    env_logger::init();

    // Parse command line arguments
    let cli = Cli::parse();

    // Execute the command
    cli.execute().await
}
