use anyhow::Result;

use crate::cli::{VersionCommand, VERSION};
use crate::context::CommandContext;

pub fn run(_ctx: &CommandContext, _args: VersionCommand) -> Result<()> {
    println!("mcap {}", VERSION.as_str());
    Ok(())
}
