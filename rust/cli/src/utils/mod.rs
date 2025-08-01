// Utility modules for shared functionality
pub mod error; // Error handling utilities
pub mod format; // Output formatting helpers
pub mod io; // File I/O operations and remote file support
pub mod mcap_ext; // MCAP library extensions
pub mod profiler;
pub mod progress; // Progress bars and reporting
pub mod table; // Table output formatting
pub mod time; // Time formatting utilities // Performance profiling

// Re-export commonly used items
pub use error::{enhance_mcap_error, from_mcap_result, McapCliError, Result};
pub use format::{human_bytes, FormatOptions, OutputFormat};
pub use io::{with_reader, FileInput};
pub use progress::ProgressReporter;
