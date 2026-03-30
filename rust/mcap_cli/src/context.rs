use std::path::PathBuf;

use crate::logsetup::Color;

/// Global CLI execution context shared across command handlers.
///
/// This scaffold stores global options that upcoming command implementations
/// will consume as real command behavior lands.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct CommandContext {
    verbose: u8,
    color: Color,
    config: Option<PathBuf>,
    pprof_profile: bool,
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

#[allow(dead_code)]
impl CommandContext {
    pub fn new(verbose: u8, color: Color, config: Option<PathBuf>, pprof_profile: bool) -> Self {
        Self {
            verbose,
            color,
            config,
            pprof_profile,
        }
    }

    pub fn verbose(&self) -> u8 {
        self.verbose
    }

    pub fn color(&self) -> Color {
        self.color
    }

    pub fn config(&self) -> Option<&PathBuf> {
        self.config.as_ref()
    }

    pub fn pprof_profile(&self) -> bool {
        self.pprof_profile
    }
}
