use anyhow::Result;
use clap::Args;

#[derive(Args)]
pub struct VersionArgs {
    /// Print MCAP library version instead of CLI version
    #[arg(short, long)]
    pub library: bool,
}

pub async fn run(args: VersionArgs) -> Result<()> {
    if args.library {
        // TODO: Find the correct way to get MCAP library version
        println!("MCAP library version: 0.23.2");
    } else {
        println!("{}", env!("CARGO_PKG_VERSION"));
    }
    Ok(())
}
