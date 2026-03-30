use anyhow::{bail, Result};

use crate::context::CommandContext;

pub fn run(_ctx: &CommandContext) -> Result<()> {
    bail!("'recover' is not implemented yet")
}
