use anyhow::Result;
use clap::Args;
use std::path::PathBuf;

#[derive(Args)]
pub struct DoctorArgs {
    /// MCAP file to check
    pub file: PathBuf,

    /// Verbose output
    #[arg(short, long)]
    pub verbose: bool,

    /// Require strict message ordering
    #[arg(long)]
    pub strict_message_order: bool,
}

pub async fn run(args: DoctorArgs) -> Result<()> {
    anyhow::bail!(
        "doctor command not yet implemented for file: {:?}",
        args.file
    );
}
