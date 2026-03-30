use anyhow::Result;

use crate::commands::not_implemented;
use crate::context::CommandContext;

pub fn run(_ctx: &CommandContext) -> Result<()> {
    Err(not_implemented("add metadata"))
}
