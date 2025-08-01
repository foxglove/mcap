# MCAP CLI Rust Port Plan

## 📁 Directory Structure

```
rust/cli/
├── Cargo.toml
├── src/
│   ├── main.rs
│   ├── lib.rs
│   ├── commands/
│   │   ├── mod.rs
│   │   ├── cat.rs
│   │   ├── compress.rs
│   │   ├── convert.rs
│   │   ├── decompress.rs
│   │   ├── doctor.rs
│   │   ├── du.rs
│   │   ├── filter.rs
│   │   ├── info.rs
│   │   ├── merge.rs
│   │   ├── recover.rs
│   │   ├── sort.rs
│   │   ├── version.rs
│   │   ├── add/
│   │   │   ├── mod.rs
│   │   │   ├── attachment.rs
│   │   │   └── metadata.rs
│   │   ├── get/
│   │   │   ├── mod.rs
│   │   │   ├── attachment.rs
│   │   │   └── metadata.rs
│   │   └── list/
│   │       ├── mod.rs
│   │       ├── attachments.rs
│   │       ├── channels.rs
│   │       ├── chunks.rs
│   │       ├── metadata.rs
│   │       └── schemas.rs
│   ├── utils/
│   │   ├── mod.rs
│   │   ├── io.rs
│   │   ├── format.rs
│   │   ├── progress.rs
│   │   └── validation.rs
│   └── error.rs
└── README.md
```

## 🔧 Core Dependencies

```toml
[dependencies]
# CLI framework
clap = { version = "4.0", features = ["derive", "cargo"] }

# MCAP library (already exists)
mcap = { path = "../", features = ["zstd", "lz4"] }

# Error handling
anyhow = "1.0"
thiserror = "1.0"

# Serialization
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"

# Table formatting
tabled = "0.15"

# Progress bars
indicatif = "0.17"

# File I/O and compression
tokio = { version = "1.0", features = ["full"] }
bytes = "1.0"

# Protobuf support
prost = "0.12"
prost-types = "0.12"

# Cloud storage (if needed)
cloud-storage = { version = "0.11", optional = true }

# Time handling
chrono = { version = "0.4", features = ["serde"] }

# Config management
config = "0.14"
dirs = "5.0"

[features]
default = ["cloud"]
cloud = ["dep:cloud-storage"]
```

## 🏗️ Implementation Phases

### Phase 1: Foundation (Week 1-2)

1. **Project Setup**

   - Create `rust/cli/` directory structure
   - Set up `Cargo.toml` with dependencies
   - Implement basic CLI structure with `clap`

2. **Core Infrastructure**
   - `src/main.rs` - Entry point and argument parsing
   - `src/error.rs` - Unified error handling
   - `src/utils/mod.rs` - Shared utilities
   - `src/utils/io.rs` - File I/O helpers (local and cloud)
   - `src/utils/format.rs` - Table formatting and output utilities

### Phase 2: Basic Commands (Week 3-4)

3. **Simple Commands**
   - `version` - Version information
   - `info` - File information and statistics
   - `cat` - Message output with filtering
   - `list` subcommands - List various MCAP records

### Phase 3: Data Processing Commands (Week 5-6)

4. **Processing Commands**
   - `filter` - Message filtering and copying
   - `sort` - Message sorting by timestamp
   - `compress`/`decompress` - Compression utilities
   - `du` - Disk usage analysis

### Phase 4: Complex Operations (Week 7-8)

5. **Advanced Commands**
   - `merge` - Multi-file merging with deduplication
   - `convert` - ROS bag to MCAP conversion
   - `doctor` - File validation and health checks
   - `recover` - File recovery operations

### Phase 5: Metadata Operations (Week 9)

6. **Metadata Commands**
   - `add attachment`/`add metadata`
   - `get attachment`/`get metadata`

### Phase 6: Polish & Testing (Week 10)

7. **Final Integration**
   - Comprehensive testing
   - Performance optimization
   - Documentation
   - CI/CD integration

## 🎯 Key Implementation Details

### Command Structure Pattern

```rust
// commands/mod.rs
use clap::{Args, Subcommand};

#[derive(Subcommand)]
pub enum Commands {
    /// Display information about an MCAP file
    Info(info::InfoArgs),
    /// Output messages from an MCAP file
    Cat(cat::CatArgs),
    /// Merge multiple MCAP files
    Merge(merge::MergeArgs),
    // ... other commands

    /// Add records to an MCAP file
    Add {
        #[command(subcommand)]
        command: add::AddCommands,
    },
    /// Get records from an MCAP file
    Get {
        #[command(subcommand)]
        command: get::GetCommands,
    },
    /// List records in an MCAP file
    List {
        #[command(subcommand)]
        command: list::ListCommands,
    },
}
```

### Utility Functions

```rust
// utils/io.rs
pub async fn get_reader(path: &str) -> Result<Box<dyn AsyncRead + Unpin>> {
    if path.starts_with("gs://") {
        // Cloud storage implementation
        #[cfg(feature = "cloud")]
        return get_gcs_reader(path).await;
        #[cfg(not(feature = "cloud"))]
        return Err(anyhow!("Cloud storage not supported"));
    } else {
        // Local file
        let file = tokio::fs::File::open(path).await?;
        Ok(Box::new(file))
    }
}

// utils/format.rs
pub fn format_table(headers: Vec<&str>, rows: Vec<Vec<String>>) {
    // Use tabled for consistent table formatting
}

pub fn format_duration(nanos: u64) -> String {
    // Human-readable duration formatting
}

pub fn format_bytes(bytes: u64) -> String {
    // Human-readable byte formatting (B, KiB, MiB, etc.)
}
```

### Error Handling

```rust
// error.rs
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CliError {
    #[error("MCAP error: {0}")]
    Mcap(#[from] mcap::McapError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    #[error("File not found: {0}")]
    FileNotFound(String),
}
```

## 🔄 Migration Strategy

### Direct Go → Rust Mapping

- **Cobra commands** → **clap derive macros**
- **tablewriter** → **tabled crate**
- **progressbar** → **indicatif crate**
- **viper config** → **config crate**
- **Go's io.Reader** → **Rust's AsyncRead trait**

### Rust-Specific Improvements

1. **Type Safety**: Leverage Rust's type system for better argument validation
2. **Memory Safety**: Eliminate potential memory issues from Go implementation
3. **Performance**: Take advantage of Rust's zero-cost abstractions
4. **Error Handling**: Use `Result<T, E>` for robust error propagation
5. **Async I/O**: Use `tokio` for efficient file and network operations

## 🧪 Testing Strategy

- Unit tests for each command module
- Integration tests with sample MCAP files
- Property-based testing for file operations
- Performance benchmarks against Go implementation
- Cross-platform testing (Linux, macOS, Windows)

## 📦 Distribution

- Single binary distribution
- Optional features for cloud storage
- Integration with existing Rust MCAP ecosystem
- GitHub Actions for automated builds and releases

## 📊 Go CLI Analysis Summary

Based on analysis of the existing Go implementation:

### Root Commands

- `cat` - Output messages with filtering and JSON support
- `compress` - Compress MCAP files
- `convert` - Convert ROS bags to MCAP
- `decompress` - Decompress MCAP files
- `doctor` - Validate and diagnose MCAP files
- `du` - Analyze disk usage
- `filter` - Filter messages and copy to new file
- `info` - Display file information and statistics
- `merge` - Merge multiple MCAP files
- `recover` - Recover corrupted MCAP files
- `sort` - Sort messages by timestamp
- `version` - Show version information

### Subcommand Structure

- `add` (parent command)
  - `attachment` - Add attachments to MCAP files
  - `metadata` - Add metadata to MCAP files
- `get` (parent command)
  - `attachment` - Extract attachments from MCAP files
  - `metadata` - Extract metadata from MCAP files
- `list` (parent command)
  - `attachments` - List attachments in MCAP files
  - `channels` - List channels in MCAP files
  - `chunks` - List chunks in MCAP files
  - `metadata` - List metadata in MCAP files
  - `schemas` - List schemas in MCAP files

### Key Features to Port

1. **Remote file support** - Google Cloud Storage integration
2. **Progress bars** - For long-running operations
3. **Table formatting** - Consistent output formatting
4. **Protobuf support** - Schema parsing and message decoding
5. **Compression** - LZ4 and Zstd support
6. **Profiling** - Optional performance profiling
7. **Configuration** - YAML config file support
8. **Error handling** - Robust error reporting

This plan provides a structured approach to porting the full functionality of the Go MCAP CLI to Rust while maintaining feature parity and improving upon the original design where possible.
