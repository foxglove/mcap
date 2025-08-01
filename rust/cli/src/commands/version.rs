use crate::error::CliResult;

/// Execute the version command
pub async fn execute() -> CliResult<()> {
    println!("mcap {}", env!("CARGO_PKG_VERSION"));
    println!("Rust implementation of the MCAP CLI");
    println!("Homepage: {}", env!("CARGO_PKG_HOMEPAGE"));
    Ok(())
}
