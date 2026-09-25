use crate::cli::TimeFormat;
use crate::logsetup::Color;

/// Global CLI execution context shared across command handlers.
///
/// Holds the global options (verbosity, color, `allow_remote_scan`, `no_sign_request`,
/// `time_format`) threaded into every handler.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct CommandContext {
    verbose: u8,
    color: Color,
    allow_remote_scan: bool,
    no_sign_request: bool,
    time_format: TimeFormat,
}

impl Default for CommandContext {
    fn default() -> Self {
        Self {
            verbose: 0,
            color: Color::Auto,
            allow_remote_scan: false,
            no_sign_request: false,
            time_format: TimeFormat::Auto,
        }
    }
}

#[allow(dead_code)]
impl CommandContext {
    pub fn new(
        verbose: u8,
        color: Color,
        allow_remote_scan: bool,
        no_sign_request: bool,
        time_format: TimeFormat,
    ) -> Self {
        Self {
            verbose,
            color,
            allow_remote_scan,
            no_sign_request,
            time_format,
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

    pub fn no_sign_request(&self) -> bool {
        self.no_sign_request
    }

    pub fn time_format(&self) -> TimeFormat {
        self.time_format
    }
}
