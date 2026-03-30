use std::path::PathBuf;

use crate::logsetup::Color;

/// Global CLI execution context shared across command handlers.
#[derive(Debug, Clone)]
pub struct CommandContext {
    pub verbose: u8,
    pub color: Color,
    pub config: Option<PathBuf>,
    pub pprof_profile: bool,
}

impl Default for CommandContext {
    fn default() -> Self {
        Self {
            verbose: 0,
            color: Color::Auto,
            config: None,
            pprof_profile: false,
        }
    }
}

impl CommandContext {
    pub fn new(verbose: u8, color: Color, config: Option<PathBuf>, pprof_profile: bool) -> Self {
        Self {
            verbose,
            color,
            config,
            pprof_profile,
        }
    }
}

