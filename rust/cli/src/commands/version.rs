use anyhow::Result;

use crate::cli::VersionCommand;

pub fn run(args: VersionCommand) -> Result<()> {
    if args.library {
        // Until wired to the mcap crate as a dependency, fall back to package version.
        // This preserves CLI shape and flag behavior for scaffolding parity.
        println!("{}", env!("CARGO_PKG_VERSION"));
    } else {
        println!("{}", env!("CARGO_PKG_VERSION"));
    }
    Ok(())
}
