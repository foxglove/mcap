use anyhow::Result;
use std::sync::OnceLock;

use crate::cli::VersionCommand;
use crate::context::CommandContext;

pub fn run(_ctx: &CommandContext, _args: VersionCommand) -> Result<()> {
    println!("{}", version_output());
    Ok(())
}

pub(crate) fn clap_version() -> &'static str {
    static VERSION: OnceLock<String> = OnceLock::new();
    VERSION
        .get_or_init(|| {
            format!(
                "cli version: {}\nmcap library version: {}",
                env!("CARGO_PKG_VERSION"),
                mcap::VERSION
            )
        })
        .as_str()
}

fn version_output() -> String {
    format!("mcap {}", clap_version())
}

#[cfg(test)]
mod tests {
    use super::{clap_version, version_output};

    #[test]
    fn includes_cli_and_library_versions() {
        assert_eq!(
            clap_version(),
            format!(
                "cli version: {}\nmcap library version: {}",
                env!("CARGO_PKG_VERSION"),
                mcap::VERSION
            )
        );
        assert_eq!(version_output(), format!("mcap {}", clap_version()));
    }
}
