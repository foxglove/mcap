#!/bin/bash
# compare_outputs.sh - Compare command outputs between Go and Rust CLIs

# Import these variables from test_runner.sh or set defaults
GO_CLI="${GO_CLI:-../../../go/cli/mcap/bin/mcap}"
RUST_CLI="${RUST_CLI:-../target/release/mcap}"
RESULTS_DIR="${RESULTS_DIR:-results}"

# Ensure results directory exists
mkdir -p "$RESULTS_DIR"

# Core comparison function
compare_command() {
    local test_name="$1"
    local cmd_args="$2"
    local input_file="$3"

    echo "Testing: $test_name"

    # Prepare command arguments
    local go_cmd=("$GO_CLI")
    local rust_cmd=("$RUST_CLI")

    # Parse command arguments (simple space splitting for now)
    if [[ -n "$cmd_args" ]]; then
        # Convert string to array - portable approach using eval
        eval "go_args=($cmd_args)"
        eval "rust_args=($cmd_args)"
        go_cmd+=("${go_args[@]}")
        rust_cmd+=("${rust_args[@]}")
    fi

    # Add input file if provided
    if [[ -n "$input_file" ]]; then
        go_cmd+=("$input_file")
        rust_cmd+=("$input_file")
    fi

    # Run Go version
    local go_exit_code=0
    timeout 30 "${go_cmd[@]}" > "$RESULTS_DIR/go_${test_name}.out" 2> "$RESULTS_DIR/go_${test_name}.err" || go_exit_code=$?

    # Run Rust version
    local rust_exit_code=0
    timeout 30 "${rust_cmd[@]}" > "$RESULTS_DIR/rust_${test_name}.out" 2> "$RESULTS_DIR/rust_${test_name}.err" || rust_exit_code=$?

    # Compare exit codes
    if [[ $go_exit_code -ne $rust_exit_code ]]; then
        echo "FAIL: Exit codes differ - Go: $go_exit_code, Rust: $rust_exit_code"
        return 1
    fi

    # Compare stdout
    if ! diff -u "$RESULTS_DIR/go_${test_name}.out" "$RESULTS_DIR/rust_${test_name}.out" > "$RESULTS_DIR/diff_${test_name}_stdout.diff"; then
        echo "FAIL: Stdout differs for $test_name"
        echo "  Diff saved to: $RESULTS_DIR/diff_${test_name}_stdout.diff"
        return 1
    fi

    # Compare stderr (with some flexibility for formatting)
    if ! diff -u "$RESULTS_DIR/go_${test_name}.err" "$RESULTS_DIR/rust_${test_name}.err" > "$RESULTS_DIR/diff_${test_name}_stderr.diff"; then
        # Check if it's just whitespace/formatting differences
        if diff -w "$RESULTS_DIR/go_${test_name}.err" "$RESULTS_DIR/rust_${test_name}.err" > /dev/null; then
            echo "WARN: Stderr differs for $test_name (whitespace only)"
        else
            echo "WARN: Stderr differs for $test_name (may be acceptable if only formatting)"
            echo "  Diff saved to: $RESULTS_DIR/diff_${test_name}_stderr.diff"
        fi
    fi

    echo "PASS: $test_name"
    return 0
}

# Performance comparison function
compare_performance() {
    local test_name="$1"
    local cmd_args="$2"
    local input_file="$3"
    local max_time="${4:-300}"  # Default 5 minute timeout

    echo "Performance test: $test_name"

    # Prepare command arguments
    local go_cmd=("$GO_CLI")
    local rust_cmd=("$RUST_CLI")

    if [[ -n "$cmd_args" ]]; then
        read -ra go_args <<< "$cmd_args"
        read -ra rust_args <<< "$cmd_args"
        go_cmd+=("${go_args[@]}")
        rust_cmd+=("${rust_args[@]}")
    fi

    if [[ -n "$input_file" ]]; then
        go_cmd+=("$input_file")
        rust_cmd+=("$input_file")
    fi

    # Time Go version
    local go_start=$(date +%s.%N)
    if timeout "$max_time" "${go_cmd[@]}" > /dev/null 2>&1; then
        local go_end=$(date +%s.%N)
        local go_time=$(echo "$go_end - $go_start" | bc -l 2>/dev/null || echo "0")
    else
        echo "FAIL: Go version timed out or failed"
        return 1
    fi

    # Time Rust version
    local rust_start=$(date +%s.%N)
    if timeout "$max_time" "${rust_cmd[@]}" > /dev/null 2>&1; then
        local rust_end=$(date +%s.%N)
        local rust_time=$(echo "$rust_end - $rust_start" | bc -l 2>/dev/null || echo "0")
    else
        echo "FAIL: Rust version timed out or failed"
        return 1
    fi

    # Calculate performance ratio (protect against division by zero)
    local ratio
    if command -v bc > /dev/null && [[ $(echo "$go_time > 0" | bc -l) -eq 1 ]]; then
        ratio=$(echo "scale=2; $rust_time / $go_time" | bc -l)
    else
        ratio="N/A"
    fi

    echo "Go time: ${go_time}s"
    echo "Rust time: ${rust_time}s"
    echo "Ratio: ${ratio}x (Rust/Go)"

    # Log performance data
    {
        echo "Performance Test: $test_name"
        echo "Go Time: ${go_time}s"
        echo "Rust Time: ${rust_time}s"
        echo "Ratio: ${ratio}x"
        echo "Timestamp: $(date)"
        echo "---"
    } >> "$RESULTS_DIR/performance.log"

    # Rust should be within 150% of Go performance (ratio <= 1.5)
    if command -v bc > /dev/null && [[ "$ratio" != "N/A" ]] && [[ $(echo "$ratio > 1.5" | bc -l) -eq 1 ]]; then
        echo "FAIL: Rust is significantly slower than Go (ratio: ${ratio}x)"
        return 1
    fi

    echo "PASS: Performance acceptable"
    return 0
}

# Utility function to check if a file exists and is readable
check_test_file() {
    local file="$1"
    local description="${2:-file}"

    if [[ ! -f "$file" ]]; then
        echo "SKIP: Test $description not found: $file"
        return 1
    fi

    if [[ ! -r "$file" ]]; then
        echo "SKIP: Test $description not readable: $file"
        return 1
    fi

    return 0
}

# Utility function to skip a test with a message
skip_test() {
    local test_name="$1"
    local reason="$2"

    echo "SKIP: $test_name - $reason"
    {
        echo "SKIPPED: $test_name"
        echo "Reason: $reason"
        echo "Timestamp: $(date)"
        echo "---"
    } >> "$RESULTS_DIR/skipped_tests.log"
}

# Function to validate CLI binaries are available
validate_clis() {
    local errors=0

    if [[ ! -x "$GO_CLI" ]]; then
        echo "ERROR: Go CLI not executable at $GO_CLI"
        ((errors++))
    fi

    if [[ ! -x "$RUST_CLI" ]]; then
        echo "ERROR: Rust CLI not executable at $RUST_CLI"
        ((errors++))
    fi

    return $errors
}

# Initialize performance log
echo "=== MCAP CLI Performance Comparison ===" > "$RESULTS_DIR/performance.log"
echo "Started at: $(date)" >> "$RESULTS_DIR/performance.log"
echo "" >> "$RESULTS_DIR/performance.log"

# Initialize skipped tests log
echo "=== Skipped Tests Log ===" > "$RESULTS_DIR/skipped_tests.log"
echo "Started at: $(date)" >> "$RESULTS_DIR/skipped_tests.log"
echo "" >> "$RESULTS_DIR/skipped_tests.log"
