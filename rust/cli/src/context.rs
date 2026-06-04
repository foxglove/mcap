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
    allow_remote_scan: bool,
}

impl Default for CommandContext {
    fn default() -> Self {
        Self {
            verbose: 0,
            color: Color::Auto,
            allow_remote_scan: false,
        }
    }
}

#[allow(dead_code)]
impl CommandContext {
    pub fn new(verbose: u8, color: Color, allow_remote_scan: bool) -> Self {
        Self {
            verbose,
            color,
            allow_remote_scan,
        }
    }

    pub fn verbose(&self) -> u8 {
        self.verbose
    }

    pub fn color(&self) -> Color {
        self.color
    }

    pub fn allow_remote_scan(&self) -> bool {
        self.allow_remote_scan
    }
}
