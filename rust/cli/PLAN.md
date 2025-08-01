# MCAP CLI Rust Port - Implementation Plan

## Overview

This document outlines the plan to port the MCAP CLI from Go to Rust, achieving **full feature parity** while applying **selective modernization** to improve user experience where beneficial.

## Goals

- **Full Feature Parity**: Port all 27 Go CLI commands and their functionality
- **Selective Modernization**: Keep core interfaces identical, improve UX and error handling
- **Performance**: Leverage Rust's performance advantages
- **Maintainability**: Create a clean, well-structured Rust codebase

## Architecture Decisions

### CLI Framework: Clap v4 (Derive API)
- **Rationale**: Best balance of features, performance, and developer ergonomics
- **Benefits**: Automatic help generation, validation, strong typing, excellent error messages
- **Approach**: Use derive API for clean, declarative command definitions

### Project Structure
```
rust/cli/
├── Cargo.toml
├── src/
│   ├── main.rs                 # Entry point and top-level CLI definition
│   ├── cli.rs                  # Clap command definitions
│   ├── commands/               # Command implementations
│   │   ├── mod.rs
│   │   ├── info.rs
│   │   ├── cat.rs
│   │   ├── filter.rs
│   │   ├── merge.rs
│   │   ├── doctor.rs
│   │   ├── convert.rs
│   │   ├── sort.rs
│   │   ├── recover.rs
│   │   ├── list/               # List subcommands
│   │   │   ├── mod.rs
│   │   │   ├── channels.rs
│   │   │   ├── chunks.rs
│   │   │   ├── attachments.rs
│   │   │   └── schemas.rs
│   │   ├── get/                # Get subcommands
│   │   │   ├── mod.rs
│   │   │   └── attachment.rs
│   │   ├── add/                # Add subcommands
│   │   │   ├── mod.rs
│   │   │   ├── attachment.rs
│   │   │   └── metadata.rs
│   │   ├── compression.rs      # compress/decompress
│   │   ├── du.rs
│   │   └── version.rs
│   ├── utils/                  # Shared utilities
│   │   ├── mod.rs
│   │   ├── io.rs              # File I/O, remote file support
│   │   ├── progress.rs        # Progress bars and reporting
│   │   ├── format.rs          # Output formatting helpers
│   │   ├── time.rs            # Time formatting utilities
│   │   ├── table.rs           # Table output formatting
│   │   ├── error.rs           # Error handling utilities
│   │   └── mcap_ext.rs        # MCAP library extensions
│   └── tests/                 # Integration tests
├── tests/                     # Additional test files
├── benches/                   # Performance benchmarks
├── examples/                  # Usage examples
└── README.md
```

## Complete Command Matrix

### Primary Commands (8)
| Command | Description | Complexity | Priority |
|---------|-------------|------------|----------|
| `info` | Report file statistics | Medium | Phase 1 |
| `cat` | Output messages | High | Phase 1 |
| `filter` | Filter MCAP data | High | Phase 2 |
| `merge` | Merge multiple files | High | Phase 2 |
| `doctor` | Validate file structure | Medium | Phase 1 |
| `convert` | Format conversion | Medium | Phase 3 |
| `sort` | Sort messages | Medium | Phase 2 |
| `recover` | Recover corrupted data | High | Phase 4 |

### List Subcommands (4)
| Command | Description | Complexity | Priority |
|---------|-------------|------------|----------|
| `list channels` | List channels | Low | Phase 1 |
| `list chunks` | List chunks | Low | Phase 1 |
| `list attachments` | List attachments | Low | Phase 2 |
| `list schemas` | List schemas | Low | Phase 2 |

### Get Subcommands (1)
| Command | Description | Complexity | Priority |
|---------|-------------|------------|----------|
| `get attachment` | Extract attachment | Medium | Phase 3 |

### Add Subcommands (2)
| Command | Description | Complexity | Priority |
|---------|-------------|------------|----------|
| `add attachment` | Add attachment | Medium | Phase 3 |
| `add metadata` | Add metadata | Medium | Phase 3 |

### Utility Commands (12)
| Command | Description | Complexity | Priority |
|---------|-------------|------------|----------|
| `compress` | Compress MCAP | Medium | Phase 3 |
| `decompress` | Decompress MCAP | Medium | Phase 3 |
| `du` | Disk usage analysis | Low | Phase 4 |
| `version` | Version information | Low | Phase 1 |

**Total: 27 commands**

## Implementation Phases

### Phase 1: Foundation & Core Commands (4-6 weeks)
**Goal**: Basic CLI infrastructure and essential commands

**Deliverables**:
- [x] Project setup and build system
- [x] Core CLI structure with Clap
- [x] Utility modules (I/O, formatting, error handling)
- [x] Commands: `version` (working)
- [x] Commands: `info`, `doctor` (working implementations)
- [x] Commands: `list channels`, `list chunks` (working implementations)
- [x] Commands: `list attachments`, `list schemas` (stub implementations)
- [x] All 27 commands defined with proper argument structures
- [x] Basic testing infrastructure

**Success Criteria**:
- [x] CLI compiles and runs
- [x] Help system works
- [x] Basic file reading functional
- [x] Output format matches Go CLI

**Status**: ✅ **COMPLETED**

### Phase 2: Message Processing (4-6 weeks)
**Goal**: Core message handling and filtering

**Deliverables**:
- [ ] `cat` command with full feature parity
- [ ] `filter` command with topic/time filtering
- [ ] `sort` command
- [ ] `merge` command (basic version)
- [ ] `list attachments`, `list schemas`
- [ ] Progress bar integration
- [ ] JSON output support

**Success Criteria**:
- Message iteration and filtering works
- Performance comparable to Go version
- Complex filtering scenarios supported

### Phase 3: Advanced Features (4-6 weeks)
**Goal**: File manipulation and advanced operations

**Deliverables**:
- [ ] `convert` command with format support
- [ ] `compress`/`decompress` commands
- [ ] `get attachment`, `add attachment`
- [ ] `add metadata` command
- [ ] Enhanced `merge` with conflict resolution
- [ ] Remote file support (Google Cloud Storage)

**Success Criteria**:
- File format conversion working
- Attachment handling complete
- Remote file access functional

### Phase 4: Robustness & Polish (3-4 weeks)
**Goal**: Error recovery and production readiness

**Deliverables**:
- [ ] `recover` command for corrupted files
- [ ] `du` command for disk usage
- [ ] Comprehensive error handling
- [ ] Performance optimizations
- [ ] Documentation and examples
- [ ] Cross-platform testing

**Success Criteria**:
- All 27 commands implemented
- Robust error handling
- Performance benchmarks pass
- Ready for production use

## Technical Implementation Details

### Dependencies
```toml
[dependencies]
mcap = { path = "../", features = ["zstd", "lz4"] }
clap = { version = "4.0", features = ["derive", "color"] }
tokio = { version = "1.0", features = ["rt", "fs", "io-util"] }
anyhow = "1.0"
thiserror = "1.0"
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
indicatif = "0.17"      # Progress bars
comfy-table = "7.0"     # Table formatting
colored = "2.0"         # Terminal colors
regex = "1.0"
chrono = { version = "0.4", features = ["serde"] }
reqwest = { version = "0.11", features = ["stream"] }  # Remote files
google-cloud-storage = "0.15"  # GCS support
clap-verbosity-flag = "2.0"    # Standardized verbosity
```

### Error Handling Strategy
- Use `anyhow` for error propagation in main application
- Use `thiserror` for custom error types in utilities
- Provide helpful error messages with suggestions
- Support `--verbose` flag for detailed debugging

### Output Formatting
- **Human-readable**: Default colorized output
- **JSON**: `--json` flag for machine consumption
- **Tables**: Consistent formatting using `comfy-table`
- **Progress**: Progress bars for long operations

### Performance Considerations
- **Async I/O**: Use `tokio` for file operations
- **Streaming**: Process large files without loading entirely into memory
- **Parallel processing**: Use `rayon` for CPU-intensive operations
- **Memory mapping**: Use `memmap2` for large file access

### Compatibility Strategy
- **Argument compatibility**: Maintain exact flag names and behavior
- **Output compatibility**: Match formats where users depend on them
- **Selective improvements**:
  - Better error messages
  - Improved progress indication
  - Enhanced help text
  - Optional colored output

## Testing Strategy

### Unit Tests
- Test each command module independently
- Mock file I/O for deterministic testing
- Test argument parsing and validation

### Integration Tests
- Test against real MCAP files
- Compare outputs with Go CLI version
- Test edge cases and error conditions

### Compatibility Tests
- Automated comparison of outputs
- Test with diverse MCAP file types
- Regression testing against Go version

### Performance Tests
- Benchmark against Go version
- Memory usage testing
- Large file handling tests

## Quality Assurance

### Code Quality
- Use `rustfmt` for consistent formatting
- Use `clippy` for linting and best practices
- Maintain >90% test coverage
- Document all public APIs

### User Experience
- Comprehensive help text for all commands
- Consistent error message formatting
- Progress indication for long operations
- Intuitive command structure

## Migration Strategy

### Coexistence Period
- Both CLIs will coexist during development
- Rust CLI will use `mcap-rs` binary name initially
- Document migration path for users

### Feature Flags
- Optional features for advanced functionality
- Backward compatibility modes if needed
- Gradual rollout capabilities

## Success Metrics

### Functional
- [ ] All 27 commands implemented and tested
- [ ] 100% argument compatibility with Go CLI
- [ ] All output formats supported
- [ ] Cross-platform support (Linux, macOS, Windows)

### Performance
- [ ] Startup time ≤ Go version
- [ ] Memory usage ≤ 150% of Go version
- [ ] Processing speed ≥ 90% of Go version
- [ ] Binary size ≤ 20MB (release build)

### Quality
- [ ] >90% test coverage
- [ ] Zero clippy warnings
- [ ] Comprehensive documentation
- [ ] Successful integration tests

## Timeline Estimate

**Total Duration**: 15-20 weeks

- **Phase 1**: Weeks 1-6 (Foundation)
- **Phase 2**: Weeks 7-12 (Core functionality)
- **Phase 3**: Weeks 13-18 (Advanced features)
- **Phase 4**: Weeks 19-22 (Polish & production)

## Risk Mitigation

### Technical Risks
- **Large scope**: Implement incrementally, validate each phase
- **Performance**: Early benchmarking, optimize critical paths
- **Compatibility**: Automated testing against Go version

### Project Risks
- **Resource allocation**: Flexible timeline, prioritize core features
- **User adoption**: Clear migration documentation, gradual rollout
- **Maintenance burden**: Clean architecture, comprehensive tests

## Next Steps

1. **Set up project structure** (Week 1)
2. **Implement basic CLI framework** (Week 1-2)
3. **Create first command (`version`)** (Week 2)
4. **Establish testing and CI/CD** (Week 2-3)
5. **Begin Phase 1 implementation** (Week 3+)

---

This plan provides a roadmap for successfully porting the MCAP CLI to Rust while maintaining full compatibility and improving the user experience where possible.
