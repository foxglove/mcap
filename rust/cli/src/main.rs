use std::process::ExitCode;

use tracing_subscriber::EnvFilter;

mod cli;
mod error;
mod info;
mod mcap;
mod filter;
mod reader;
mod utils;

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    if let Err(e) = cli::run().await {
        eprintln!("{e}");
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}
