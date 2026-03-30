use anyhow::{bail, Result};

use crate::context::CommandContext;

pub fn run(_ctx: &CommandContext) -> Result<()> {
    bail!("'list metadata' is not implemented yet")
}
