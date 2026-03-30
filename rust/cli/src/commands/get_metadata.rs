use anyhow::{bail, Result};

use crate::context::CommandContext;

pub fn run(_ctx: &CommandContext) -> Result<()> {
    bail!("'get metadata' is not implemented yet")
}
