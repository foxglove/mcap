#!/bin/bash
# test_runner.sh - Main test execution script

set -euo pipefail

GO_CLI="../../../go/cli/mcap/bin/mcap"
RUST_CLI="../target/release/mcap"
TEST_DATA_DIR="../../../testdata"
RESULTS_DIR="results"

echo "=== MCAP CLI Feature Parity Test Runner ==="
echo ""

# Ensure both CLIs are available
if [[ ! -x "$GO_CLI" ]]; then
    echo "ERROR: Go CLI not found at $GO_CLI"
    echo "Build it with: cd go/cli/mcap && mkdir -p bin && go build -o bin/mcap"
    exit 1
fi

if [[ ! -x "$RUST_CLI" ]]; then
    echo "ERROR: Rust CLI not found at $RUST_CLI"
    echo "Build it with: cd rust/cli && cargo build --release"
    exit 1
fi

# Display CLI versions
echo "Testing CLI versions:"
echo "Go CLI:   $($GO_CLI version 2>/dev/null || echo 'version command not available')"
echo "Rust CLI: $($RUST_CLI version 2>/dev/null || echo 'version command not available')"
echo ""

# Create results directory
mkdir -p "$RESULTS_DIR"

# Initialize test counters
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0
WARNING_TESTS=0

# Test result tracking
echo "=== Test Execution Log ===" > "$RESULTS_DIR/test_summary.log"

# Execute all test suites
echo "Executing test suites..."

test_suites=(
    "tests/test_version.sh"
    "tests/test_info.sh"
    "tests/test_list_commands.sh"
    "tests/test_cat.sh"
    "tests/test_doctor.sh"
    "tests/test_filter.sh"
    "tests/test_merge.sh"
    "tests/test_sort.sh"
    "tests/test_convert.sh"
    "tests/test_attachments.sh"
    "tests/test_metadata.sh"
    "tests/test_compression.sh"
    "tests/test_recovery.sh"
    "tests/test_du.sh"
    "tests/test_error_conditions.sh"
    "tests/test_performance.sh"
)

for suite in "${test_suites[@]}"; do
    if [[ -f "$suite" && -x "$suite" ]]; then
        echo "Running: $suite"
        suite_name=$(basename "$suite" .sh)

        if "$suite" > "$RESULTS_DIR/${suite_name}.log" 2>&1; then
            echo "  ✓ PASSED: $suite_name"
            ((PASSED_TESTS++))
        else
            echo "  ✗ FAILED: $suite_name"
            ((FAILED_TESTS++))
        fi
    else
        echo "  ⚠ SKIPPED: $suite (not found or not executable)"
        ((WARNING_TESTS++))
    fi
    ((TOTAL_TESTS++))
done

echo ""
echo "=== Test Summary ==="
echo "Total test suites: $TOTAL_TESTS"
echo "Passed: $PASSED_TESTS"
echo "Failed: $FAILED_TESTS"
echo "Skipped: $WARNING_TESTS"

# Generate detailed summary
{
    echo "Test execution completed at $(date)"
    echo "Platform: $(uname -a)"
    echo "Go CLI: $GO_CLI"
    echo "Rust CLI: $RUST_CLI"
    echo ""
    echo "Results:"
    echo "  Total: $TOTAL_TESTS"
    echo "  Passed: $PASSED_TESTS"
    echo "  Failed: $FAILED_TESTS"
    echo "  Skipped: $WARNING_TESTS"
    echo ""

    if [[ $FAILED_TESTS -gt 0 ]]; then
        echo "Failed test details:"
        grep -r "FAIL:" "$RESULTS_DIR/" 2>/dev/null || echo "  No detailed failure information available"
    fi

    echo ""
    echo "All test logs available in: $RESULTS_DIR/"
} >> "$RESULTS_DIR/test_summary.log"

# Exit with error if any tests failed
if [[ $FAILED_TESTS -gt 0 ]]; then
    echo ""
    echo "❌ Some tests failed. Check $RESULTS_DIR/ for details."
    exit 1
else
    echo ""
    echo "✅ All tests passed!"
    exit 0
fi
