use std::io::IsTerminal;

use anyhow::Context;
use simplelog::{ColorChoice, ConfigBuilder, LevelFilter, SimpleLogger, TermLogger, TerminalMode};

#[derive(clap::ValueEnum, Debug, Copy, Clone, PartialEq, Eq)]
pub enum Color {
    Auto,
    Always,
    Never,
}

/// Set up simplelog to spit messages to stderr.
pub fn init_logger(verbosity: u8, color: Color) -> anyhow::Result<()> {
    let mut builder = ConfigBuilder::new();
    // Shut a bunch of stuff off - we're just spitting to stderr.
    builder.set_location_level(LevelFilter::Trace);
    builder.set_target_level(LevelFilter::Off);
    builder.set_thread_level(LevelFilter::Off);
    builder.set_time_level(LevelFilter::Off);

    let level = match verbosity {
        0 => LevelFilter::Warn,
        1 => LevelFilter::Info,
        2 => LevelFilter::Debug,
        _ => LevelFilter::Trace,
    };

    let config = builder.build();

    let color = match color {
        Color::Always => ColorChoice::AlwaysAnsi,
        Color::Auto => {
            if std::io::stderr().is_terminal() {
                ColorChoice::Auto
            } else {
                ColorChoice::Never
            }
        }
        Color::Never => ColorChoice::Never,
    };

    TermLogger::init(level, config.clone(), TerminalMode::Stderr, color)
        .or_else(|_| SimpleLogger::init(level, config))
        .context("Couldn't init logger")
}
