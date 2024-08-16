use std::process::ExitCode;

mod cli;
mod error;
mod gcs_reader;
mod info;
mod mcap;
mod traits;
mod utils;

#[tokio::main]
async fn main() -> ExitCode {
    if let Err(e) = cli::run().await {
        eprintln!("{e}");
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}
