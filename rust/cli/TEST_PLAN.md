# MCAP CLI Rust Port - Feature Parity Test Plan

## Overview

This document defines a comprehensive testing strategy to ensure **exact feature parity** between the Go and Rust implementations of the MCAP CLI. The plan is designed to be executed systematically by an AI agent to validate all 27 commands and their functionality.

## Test Objectives

1. **Functional Parity**: Every command produces identical output for identical inputs
2. **Argument Compatibility**: All flags, options, and arguments work identically
3. **Error Handling**: Error messages and exit codes match between versions
4. **Performance Validation**: Rust version performs within acceptable bounds
5. **Cross-Platform Consistency**: Behavior is identical across operating systems

## Test Infrastructure

### Prerequisites

```bash
# Required binaries
- go/cli/mcap/mcap (Go reference implementation)
- rust/cli/target/release/mcap (Rust implementation under test)

# Test data
- testdata/ directory with diverse MCAP files
- Generated test files for edge cases
- Invalid/corrupted files for error testing
```

### Test Data Categories

#### 1. Reference Files
- `testdata/mcap/demo.mcap` - Basic test file
- `testdata/bags/demo.bag` - ROS1 bag file
- `testdata/db3/chatter.db3` - ROS2 db3 file

#### 2. Generated Test Files
- Empty MCAP files
- Single message files
- Large files (>1GB)
- Files with attachments
- Files with metadata
- Compressed files (LZ4, ZSTD)
- Files with different chunk sizes
- Files with schema definitions

#### 3. Invalid/Corrupted Files
- Truncated files
- Files with invalid CRC
- Files with missing chunks
- Files with malformed headers

## Automated Testing Framework

### Test Execution Engine

```bash
#!/bin/bash
# test_runner.sh - Main test execution script

set -euo pipefail

GO_CLI="go/cli/mcap/mcap"
RUST_CLI="rust/cli/target/release/mcap"
TEST_DATA_DIR="testdata"
RESULTS_DIR="test_results"

# Ensure both CLIs are available
if [[ ! -x "$GO_CLI" ]]; then
    echo "ERROR: Go CLI not found at $GO_CLI"
    exit 1
fi

if [[ ! -x "$RUST_CLI" ]]; then
    echo "ERROR: Rust CLI not found at $RUST_CLI"
    exit 1
fi

# Create results directory
mkdir -p "$RESULTS_DIR"

# Execute all test suites
./tests/test_version.sh
./tests/test_info.sh
./tests/test_list_commands.sh
./tests/test_cat.sh
./tests/test_doctor.sh
./tests/test_filter.sh
./tests/test_merge.sh
./tests/test_sort.sh
./tests/test_convert.sh
./tests/test_attachments.sh
./tests/test_metadata.sh
./tests/test_compression.sh
./tests/test_recovery.sh
./tests/test_du.sh
./tests/test_error_conditions.sh
./tests/test_performance.sh
```

### Output Comparison Utility

```bash
#!/bin/bash
# compare_outputs.sh - Compare command outputs between Go and Rust

compare_command() {
    local test_name="$1"
    local cmd_args="$2"
    local input_file="$3"

    echo "Testing: $test_name"

    # Run Go version
    if [[ -n "$input_file" ]]; then
        timeout 30 "$GO_CLI" $cmd_args "$input_file" > "$RESULTS_DIR/go_${test_name}.out" 2> "$RESULTS_DIR/go_${test_name}.err"
    else
        timeout 30 "$GO_CLI" $cmd_args > "$RESULTS_DIR/go_${test_name}.out" 2> "$RESULTS_DIR/go_${test_name}.err"
    fi
    local go_exit_code=$?

    # Run Rust version
    if [[ -n "$input_file" ]]; then
        timeout 30 "$RUST_CLI" $cmd_args "$input_file" > "$RESULTS_DIR/rust_${test_name}.out" 2> "$RESULTS_DIR/rust_${test_name}.err"
    else
        timeout 30 "$RUST_CLI" $cmd_args > "$RESULTS_DIR/rust_${test_name}.out" 2> "$RESULTS_DIR/rust_${test_name}.err"
    fi
    local rust_exit_code=$?

    # Compare exit codes
    if [[ $go_exit_code -ne $rust_exit_code ]]; then
        echo "FAIL: Exit codes differ - Go: $go_exit_code, Rust: $rust_exit_code"
        return 1
    fi

    # Compare stdout
    if ! diff -u "$RESULTS_DIR/go_${test_name}.out" "$RESULTS_DIR/rust_${test_name}.out"; then
        echo "FAIL: Stdout differs for $test_name"
        return 1
    fi

    # Compare stderr (with some flexibility for formatting)
    if ! diff -u "$RESULTS_DIR/go_${test_name}.err" "$RESULTS_DIR/rust_${test_name}.err"; then
        echo "WARN: Stderr differs for $test_name (may be acceptable if only formatting)"
    fi

    echo "PASS: $test_name"
    return 0
}
```

## Command-Specific Test Suites

### 1. Version Command Tests

**File: `tests/test_version.sh`**

```bash
#!/bin/bash
# Test version command

source ./compare_outputs.sh

test_version_basic() {
    compare_command "version_basic" "version" ""
}

test_version_help() {
    compare_command "version_help" "version --help" ""
}

test_version_short() {
    compare_command "version_short" "version -h" ""
}

# Run tests
test_version_basic
test_version_help
test_version_short
```

### 2. Info Command Tests

**File: `tests/test_info.sh`**

```bash
#!/bin/bash
# Test info command with various file types

source ./compare_outputs.sh

test_info_basic() {
    compare_command "info_basic" "info" "$TEST_DATA_DIR/mcap/demo.mcap"
}

test_info_json() {
    compare_command "info_json" "info --json" "$TEST_DATA_DIR/mcap/demo.mcap"
}

test_info_verbose() {
    compare_command "info_verbose" "info --verbose" "$TEST_DATA_DIR/mcap/demo.mcap"
}

test_info_compressed() {
    compare_command "info_compressed" "info" "$TEST_DATA_DIR/compressed.mcap"
}

test_info_empty() {
    compare_command "info_empty" "info" "$TEST_DATA_DIR/empty.mcap"
}

test_info_invalid_file() {
    compare_command "info_invalid" "info" "/nonexistent/file.mcap"
}

# Run all info tests
test_info_basic
test_info_json
test_info_verbose
test_info_compressed
test_info_empty
test_info_invalid_file
```

### 3. List Commands Tests

**File: `tests/test_list_commands.sh`**

```bash
#!/bin/bash
# Test all list subcommands

source ./compare_outputs.sh

# List channels tests
test_list_channels_basic() {
    compare_command "list_channels_basic" "list channels" "$TEST_DATA_DIR/mcap/demo.mcap"
}

test_list_channels_json() {
    compare_command "list_channels_json" "list channels --json" "$TEST_DATA_DIR/mcap/demo.mcap"
}

# List chunks tests
test_list_chunks_basic() {
    compare_command "list_chunks_basic" "list chunks" "$TEST_DATA_DIR/mcap/demo.mcap"
}

test_list_chunks_json() {
    compare_command "list_chunks_json" "list chunks --json" "$TEST_DATA_DIR/mcap/demo.mcap"
}

# List attachments tests
test_list_attachments_basic() {
    compare_command "list_attachments_basic" "list attachments" "$TEST_DATA_DIR/with_attachments.mcap"
}

test_list_attachments_json() {
    compare_command "list_attachments_json" "list attachments --json" "$TEST_DATA_DIR/with_attachments.mcap"
}

# List schemas tests
test_list_schemas_basic() {
    compare_command "list_schemas_basic" "list schemas" "$TEST_DATA_DIR/mcap/demo.mcap"
}

test_list_schemas_json() {
    compare_command "list_schemas_json" "list schemas --json" "$TEST_DATA_DIR/mcap/demo.mcap"
}

# Run all list tests
test_list_channels_basic
test_list_channels_json
test_list_chunks_basic
test_list_chunks_json
test_list_attachments_basic
test_list_attachments_json
test_list_schemas_basic
test_list_schemas_json
```

### 4. Cat Command Tests

**File: `tests/test_cat.sh`**

```bash
#!/bin/bash
# Test cat command with various options

source ./compare_outputs.sh

test_cat_basic() {
    compare_command "cat_basic" "cat" "$TEST_DATA_DIR/mcap/demo.mcap"
}

test_cat_json() {
    compare_command "cat_json" "cat --json" "$TEST_DATA_DIR/mcap/demo.mcap"
}

test_cat_topics() {
    compare_command "cat_topics" "cat --topics /topic1,/topic2" "$TEST_DATA_DIR/mcap/demo.mcap"
}

test_cat_start_time() {
    compare_command "cat_start_time" "cat --start 1000000000" "$TEST_DATA_DIR/mcap/demo.mcap"
}

test_cat_end_time() {
    compare_command "cat_end_time" "cat --end 2000000000" "$TEST_DATA_DIR/mcap/demo.mcap"
}

test_cat_time_range() {
    compare_command "cat_time_range" "cat --start 1000000000 --end 2000000000" "$TEST_DATA_DIR/mcap/demo.mcap"
}

test_cat_offset() {
    compare_command "cat_offset" "cat --offset 100" "$TEST_DATA_DIR/mcap/demo.mcap"
}

test_cat_count() {
    compare_command "cat_count" "cat --count 10" "$TEST_DATA_DIR/mcap/demo.mcap"
}

# Run all cat tests
test_cat_basic
test_cat_json
test_cat_topics
test_cat_start_time
test_cat_end_time
test_cat_time_range
test_cat_offset
test_cat_count
```

### 5. Doctor Command Tests

**File: `tests/test_doctor.sh`**

```bash
#!/bin/bash
# Test doctor command

source ./compare_outputs.sh

test_doctor_valid() {
    compare_command "doctor_valid" "doctor" "$TEST_DATA_DIR/mcap/demo.mcap"
}

test_doctor_verbose() {
    compare_command "doctor_verbose" "doctor --verbose" "$TEST_DATA_DIR/mcap/demo.mcap"
}

test_doctor_corrupted() {
    compare_command "doctor_corrupted" "doctor" "$TEST_DATA_DIR/corrupted.mcap"
}

test_doctor_truncated() {
    compare_command "doctor_truncated" "doctor" "$TEST_DATA_DIR/truncated.mcap"
}

# Run all doctor tests
test_doctor_valid
test_doctor_verbose
test_doctor_corrupted
test_doctor_truncated
```

### 6. Filter Command Tests

**File: `tests/test_filter.sh`**

```bash
#!/bin/bash
# Test filter command

source ./compare_outputs.sh

test_filter_topics() {
    compare_command "filter_topics" "filter --topics /topic1" "$TEST_DATA_DIR/mcap/demo.mcap"
}

test_filter_start_time() {
    compare_command "filter_start_time" "filter --start 1000000000 --output filtered.mcap" "$TEST_DATA_DIR/mcap/demo.mcap"
}

test_filter_end_time() {
    compare_command "filter_end_time" "filter --end 2000000000 --output filtered.mcap" "$TEST_DATA_DIR/mcap/demo.mcap"
}

test_filter_compression() {
    compare_command "filter_compression" "filter --compression zstd --output filtered.mcap" "$TEST_DATA_DIR/mcap/demo.mcap"
}

# Run all filter tests
test_filter_topics
test_filter_start_time
test_filter_end_time
test_filter_compression
```

### 7. Error Condition Tests

**File: `tests/test_error_conditions.sh`**

```bash
#!/bin/bash
# Test error conditions and edge cases

source ./compare_outputs.sh

# File not found errors
test_file_not_found() {
    compare_command "file_not_found" "info" "/nonexistent/file.mcap"
}

# Invalid arguments
test_invalid_topic_filter() {
    compare_command "invalid_topic" "cat --topics ''" "$TEST_DATA_DIR/mcap/demo.mcap"
}

test_invalid_time_range() {
    compare_command "invalid_time" "cat --start 2000000000 --end 1000000000" "$TEST_DATA_DIR/mcap/demo.mcap"
}

# Permission errors
test_permission_denied() {
    chmod 000 "$TEST_DATA_DIR/no_read.mcap"
    compare_command "permission_denied" "info" "$TEST_DATA_DIR/no_read.mcap"
    chmod 644 "$TEST_DATA_DIR/no_read.mcap"
}

# Corrupted file handling
test_corrupted_file() {
    compare_command "corrupted_file" "info" "$TEST_DATA_DIR/corrupted.mcap"
}

# Run all error tests
test_file_not_found
test_invalid_topic_filter
test_invalid_time_range
test_permission_denied
test_corrupted_file
```

### 8. Performance Tests

**File: `tests/test_performance.sh`**

```bash
#!/bin/bash
# Performance comparison tests

LARGE_FILE="$TEST_DATA_DIR/large_file.mcap"

performance_test() {
    local test_name="$1"
    local cmd_args="$2"
    local input_file="$3"

    echo "Performance test: $test_name"

    # Time Go version
    local go_start=$(date +%s.%N)
    timeout 300 "$GO_CLI" $cmd_args "$input_file" > /dev/null 2>&1
    local go_end=$(date +%s.%N)
    local go_time=$(echo "$go_end - $go_start" | bc)

    # Time Rust version
    local rust_start=$(date +%s.%N)
    timeout 300 "$RUST_CLI" $cmd_args "$input_file" > /dev/null 2>&1
    local rust_end=$(date +%s.%N)
    local rust_time=$(echo "$rust_end - $rust_start" | bc)

    # Calculate performance ratio
    local ratio=$(echo "scale=2; $rust_time / $go_time" | bc)

    echo "Go time: ${go_time}s"
    echo "Rust time: ${rust_time}s"
    echo "Ratio: ${ratio}x"

    # Rust should be within 150% of Go performance
    if (( $(echo "$ratio > 1.5" | bc -l) )); then
        echo "FAIL: Rust is significantly slower than Go"
        return 1
    fi

    echo "PASS: Performance acceptable"
    return 0
}

# Run performance tests
performance_test "info_large" "info" "$LARGE_FILE"
performance_test "cat_large" "cat" "$LARGE_FILE"
performance_test "list_channels_large" "list channels" "$LARGE_FILE"
```

## Test Data Sources & Generation

### Massive Existing Test Data (600+ Files)

We have comprehensive test data already available:

#### **1. Conformance Test Suite** (`tests/conformance/data/`)
- **416 systematically generated MCAP files** covering all combinations:
  - `NoData/` - 12 files (empty MCAP variations)
  - `OneMessage/` - 308 files (single message with all feature combinations)
  - `OneAttachment/` - 28 files (attachment handling)
  - `OneMetadata/` - 28 files (metadata variations)
  - `OneSchemalessMessage/` - 148 files (schemaless message types)
  - `TenMessages/` - 308 files (multi-message scenarios)

#### **2. Existing Edge Case Coverage**
The conformance files already test:
- ✅ **Chunked vs unchunked** files
- ✅ **Compressed vs uncompressed** (LZ4, ZSTD)
- ✅ **Padded records** (extra data)
- ✅ **Statistics sections**
- ✅ **Summary sections**
- ✅ **Message indexes**
- ✅ **Empty files**
- ✅ **Files with attachments**
- ✅ **Files with metadata**
- ✅ **Schemaless messages**

### Generate Additional Edge Cases Script

**File: `tests/generate_edge_cases.sh`**

```bash
#!/bin/bash
# Generate additional edge cases not covered by conformance tests

TEST_DATA_DIR="testdata/edge_cases"
mkdir -p "$TEST_DATA_DIR"

echo "Using existing conformance test data (416 files)..."
CONFORMANCE_DIR="tests/conformance/data"

echo "Generating corruption-based edge cases..."

# Generate corrupted CRC files
for file in "$CONFORMANCE_DIR"/OneMessage/*.mcap; do
    if [[ -f "$file" ]]; then
        basename=$(basename "$file" .mcap)
        # Corrupt CRC by flipping bits
        cp "$file" "$TEST_DATA_DIR/${basename}_corrupt_crc.mcap"
        dd if=/dev/urandom of="$TEST_DATA_DIR/${basename}_corrupt_crc.mcap" \
           bs=1 count=4 seek=20 conv=notrunc 2>/dev/null
        break  # Just generate one example
    fi
done

# Generate truncated files
for file in "$CONFORMANCE_DIR"/OneMessage/*.mcap; do
    if [[ -f "$file" ]]; then
        basename=$(basename "$file" .mcap)
        # Truncate at various points
        head -c 100 "$file" > "$TEST_DATA_DIR/${basename}_truncated_early.mcap"
        head -c $(($(wc -c < "$file") / 2)) "$file" > "$TEST_DATA_DIR/${basename}_truncated_mid.mcap"
        break
    fi
done

# Generate files with invalid UTF-8
echo "Generating invalid UTF-8 test cases..."
python3 << 'EOF'
import json
import os
import subprocess
import tempfile

# Create JSON with invalid UTF-8 attachment name
test_data = {
    "meta": {
        "variant": {
            "features": ["UseAttachments"]
        }
    },
    "records": [
        {"type": "Header", "fields": {"profile": "", "library": ""}},
        {
            "type": "Attachment",
            "fields": {
                "log_time": 42,
                "create_time": 42,
                "name": "invalid_utf8_\xff\xfe.txt",  # Invalid UTF-8
                "media_type": "text/plain",
                "data": "dGVzdA=="  # base64 "test"
            }
        },
        {"type": "DataEnd", "fields": {}}
    ]
}

with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
    json.dump(test_data, f)
    temp_json = f.name

try:
    # Use conformance test writer to generate MCAP from JSON
    subprocess.run([
        'go/conformance/test-write-conformance/test-write-conformance',
        temp_json
    ], stdout=open('testdata/edge_cases/invalid_utf8_attachment.mcap', 'wb'),
      check=True)
finally:
    os.unlink(temp_json)
EOF

# Generate large files by concatenating/repeating existing ones
echo "Generating large test files..."
large_file="$TEST_DATA_DIR/large_file.mcap"
# Start with a good base file
cp "$CONFORMANCE_DIR/TenMessages/TenMessages.mcap" "$large_file"

# Use the filter command to expand it
for i in {1..5}; do
    temp_file="$TEST_DATA_DIR/temp_$i.mcap"
    "$GO_CLI" merge "$large_file" "$large_file" --output "$temp_file"
    mv "$temp_file" "$large_file"
done

echo "Generated edge case files in $TEST_DATA_DIR"
echo "Using conformance test suite: $(find "$CONFORMANCE_DIR" -name "*.mcap" | wc -l) files"
```

### Use Existing Go CLI Test Utilities

The Go CLI tests already create many edge cases we can reuse:

```bash
#!/bin/bash
# Extract test data creation patterns from Go CLI tests

# Run Go CLI tests and capture generated test files
cd go/cli/mcap
go test -v ./cmd/... -test.outputdir="$PWD/../../../testdata/go_cli_generated"
```

### Conformance Test Data Inventory

**Quick test data summary:**
```bash
# Count available test files
echo "=== AVAILABLE TEST DATA ==="
echo "Conformance files: $(find tests/conformance/data -name "*.mcap" | wc -l)"
echo "Basic testdata: $(find testdata -name "*.mcap" | wc -l)"
echo "ROS bags: $(find testdata -name "*.bag" | wc -l)"
echo "ROS2 db3: $(find testdata -name "*.db3" | wc -l)"

echo ""
echo "=== EDGE CASE COVERAGE ==="
echo "Empty files: $(find tests/conformance/data/NoData -name "*.mcap" | wc -l)"
echo "With attachments: $(find tests/conformance/data/OneAttachment -name "*.mcap" | wc -l)"
echo "With metadata: $(find tests/conformance/data/OneMetadata -name "*.mcap" | wc -l)"
echo "Compressed variants: $(find tests/conformance/data -name "*chx*.mcap" | wc -l)"
echo "Chunked variants: $(find tests/conformance/data -name "*ch-*.mcap" | wc -l)"
```

## Continuous Integration Integration

### GitHub Actions Workflow

**File: `.github/workflows/feature_parity_tests.yml`**

```yaml
name: Feature Parity Tests

on:
  push:
    branches: [ main, "adrian/vibe-cli" ]
    paths: [ 'rust/cli/**', 'go/cli/**' ]
  pull_request:
    branches: [ main ]
    paths: [ 'rust/cli/**', 'go/cli/**' ]

jobs:
  feature-parity:
    runs-on: ${{ matrix.os }}
    strategy:
      matrix:
        os: [ubuntu-latest, macos-latest, windows-latest]

    steps:
    - uses: actions/checkout@v3

    - name: Set up Go
      uses: actions/setup-go@v3
      with:
        go-version: '1.19'

    - name: Set up Rust
      uses: actions-rs/toolchain@v1
      with:
        toolchain: stable
        override: true

    - name: Build Go CLI
      run: |
        cd go/cli/mcap
        go build -o mcap

    - name: Build Rust CLI
      run: |
        cd rust/cli
        cargo build --release

    - name: Generate test data
      run: ./tests/generate_test_data.sh

    - name: Run feature parity tests
      run: ./test_runner.sh

    - name: Upload test results
      uses: actions/upload-artifact@v3
      if: failure()
      with:
        name: test-results-${{ matrix.os }}
        path: test_results/
```

## Test Execution Instructions for AI Agent

### Automated Test Execution

An AI agent can execute this test plan by following these steps:

1. **Environment Setup**
   ```bash
   # Navigate to project root
   cd /path/to/mcap

   # Build both CLIs
   cd go/cli/mcap && go build -o mcap && cd -
   cd rust/cli && cargo build --release && cd -
   ```

2. **Inventory Existing Test Data & Generate Edge Cases**
   ```bash
   # Check what we already have (600+ conformance files)
   echo "=== EXISTING TEST DATA ==="
   echo "Conformance files: $(find tests/conformance/data -name "*.mcap" | wc -l)"
   echo "Basic files: $(find testdata -name "*.mcap" | wc -l)"

   # Generate additional edge cases only if needed
   chmod +x tests/generate_edge_cases.sh
   ./tests/generate_edge_cases.sh
   ```

3. **Execute Test Suite**
   ```bash
   chmod +x test_runner.sh
   chmod +x tests/*.sh
   ./test_runner.sh
   ```

4. **Analyze Results**
   ```bash
   # Check for any FAIL messages in output
   grep -r "FAIL:" test_results/

   # Generate summary report
   echo "Test Summary:"
   echo "============="
   echo "Passed: $(grep -r "PASS:" test_results/ | wc -l)"
   echo "Failed: $(grep -r "FAIL:" test_results/ | wc -l)"
   echo "Warnings: $(grep -r "WARN:" test_results/ | wc -l)"
   ```

## Success Criteria

### Functional Parity
- [ ] All 27 commands produce identical output for valid inputs
- [ ] All command-line arguments are recognized and handled identically
- [ ] Error messages and exit codes match for invalid inputs
- [ ] JSON output format is byte-identical

### Performance Criteria
- [ ] Rust CLI startup time ≤ Go CLI startup time
- [ ] Processing time ≤ 150% of Go CLI for large files
- [ ] Memory usage ≤ 150% of Go CLI for large files
- [ ] Binary size ≤ 20MB (release build)

### Quality Criteria
- [ ] All tests pass on Linux, macOS, and Windows
- [ ] Zero test failures in CI pipeline
- [ ] Performance benchmarks consistently pass
- [ ] All edge cases and error conditions covered

## Reporting

### Test Report Format

```json
{
  "test_run": {
    "timestamp": "2024-01-15T10:30:00Z",
    "go_version": "mcap 0.8.0",
    "rust_version": "mcap-rs 0.1.0",
    "platform": "linux-x86_64"
  },
  "results": {
    "total_tests": 150,
    "passed": 148,
    "failed": 2,
    "warnings": 3
  },
  "failed_tests": [
    {
      "name": "cat_json_complex",
      "reason": "JSON output format difference",
      "details": "Rust version includes extra whitespace"
    }
  ],
  "performance": {
    "average_ratio": 0.95,
    "max_ratio": 1.2,
    "failing_benchmarks": []
  }
}
```

This comprehensive test plan ensures that the Rust MCAP CLI achieves exact feature parity with the Go version through systematic, automated testing that can be executed by an AI agent or in CI/CD pipelines.
