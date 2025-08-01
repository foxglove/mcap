use clap::{CommandFactory, Parser};
use std::process;

mod commands;
mod error;
mod utils;

use commands::Commands;

#[derive(Parser)]
#[command(
    name = "mcap",
    version,
    about = "🔪 Officially the top-rated CLI tool for slicing and dicing MCAP files.",
    long_about = None
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    /// Configuration file (default is $HOME/.mcap.yaml)
    #[arg(long, global = true)]
    config: Option<String>,

    /// Record pprof profiles of command execution
    #[arg(long, global = true)]
    pprof_profile: bool,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let result = match cli.command {
        Some(command) => commands::execute(command).await,
        None => {
            // Print help if no subcommand is provided
            let mut cmd = Cli::command();
            cmd.print_help().unwrap();
            println!();
            Ok(())
        }
    };

    if let Err(e) = result {
        eprintln!("Error: {}", e);
        process::exit(1);
    }
}
