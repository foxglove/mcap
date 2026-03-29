use anyhow::Result;

use crate::cli::VersionCommand;

pub fn run(args: VersionCommand) -> Result<()> {
    println!("{}", selected_version(args));
    Ok(())
}

fn selected_version(args: VersionCommand) -> &'static str {
    if args.library {
        mcap::VERSION
    } else {
        env!("CARGO_PKG_VERSION")
    }
}

#[cfg(test)]
mod tests {
    use super::selected_version;
    use crate::cli::VersionCommand;

    #[test]
    fn selects_cli_version_by_default() {
        assert_eq!(
            selected_version(VersionCommand { library: false }),
            env!("CARGO_PKG_VERSION")
        );
    }

    #[test]
    fn selects_library_version_when_requested() {
        assert_eq!(
            selected_version(VersionCommand { library: true }),
            mcap::VERSION
        );
    }
}
