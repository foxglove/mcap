#!/bin/bash
# Generate additional edge cases not covered by conformance tests

set -euo pipefail

TEST_DATA_DIR="../../../../testdata/edge_cases"
mkdir -p "$TEST_DATA_DIR"

echo "=== MCAP CLI Edge Case Test Data Generator ==="
echo ""

echo "Using existing conformance test data (416 files)..."
CONFORMANCE_DIR="../../../../tests/conformance/data"

# Check if conformance data exists
if [[ ! -d "$CONFORMANCE_DIR" ]]; then
    echo "WARN: Conformance test data not found at $CONFORMANCE_DIR"
    echo "      Proceeding with basic edge case generation only"
fi

echo "Generating corruption-based edge cases..."

# Generate corrupted CRC files
if [[ -d "$CONFORMANCE_DIR/OneMessage" ]]; then
    for file in "$CONFORMANCE_DIR"/OneMessage/*.mcap; do
        if [[ -f "$file" ]]; then
            basename=$(basename "$file" .mcap)
            # Corrupt CRC by flipping bits
            cp "$file" "$TEST_DATA_DIR/${basename}_corrupt_crc.mcap"
            dd if=/dev/urandom of="$TEST_DATA_DIR/${basename}_corrupt_crc.mcap" \
               bs=1 count=4 seek=20 conv=notrunc 2>/dev/null || true
            echo "  Generated: ${basename}_corrupt_crc.mcap"
            break  # Just generate one example
        fi
    done
fi

# Generate truncated files
if [[ -d "$CONFORMANCE_DIR/OneMessage" ]]; then
    for file in "$CONFORMANCE_DIR"/OneMessage/*.mcap; do
        if [[ -f "$file" ]]; then
            basename=$(basename "$file" .mcap)
            # Truncate at various points
            head -c 100 "$file" > "$TEST_DATA_DIR/${basename}_truncated_early.mcap" || true
            head -c $(($(wc -c < "$file") / 2)) "$file" > "$TEST_DATA_DIR/${basename}_truncated_mid.mcap" || true
            echo "  Generated: ${basename}_truncated_early.mcap"
            echo "  Generated: ${basename}_truncated_mid.mcap"
            break
        fi
    done
fi

# Generate empty file
touch "$TEST_DATA_DIR/empty.mcap"
echo "  Generated: empty.mcap"

# Generate file with no read permissions (for permission tests)
if [[ -f "../../../../testdata/mcap/demo.mcap" ]]; then
    cp "../../../../testdata/mcap/demo.mcap" "$TEST_DATA_DIR/no_read.mcap"
    chmod 000 "$TEST_DATA_DIR/no_read.mcap" || true
    echo "  Generated: no_read.mcap (no permissions)"
else
    echo "  SKIP: demo.mcap not found, cannot create no_read.mcap"
fi

# Generate large files by concatenating/repeating existing ones
echo "Generating large test files..."
large_file="$TEST_DATA_DIR/large_file.mcap"

if [[ -f "$CONFORMANCE_DIR/TenMessages/TenMessages.mcap" ]]; then
    # Start with a good base file
    cp "$CONFORMANCE_DIR/TenMessages/TenMessages.mcap" "$large_file"
    echo "  Base file: TenMessages.mcap"

    # Check if Go CLI is available to expand the file
    if [[ -x "../../../../go/cli/mcap/bin/mcap" ]]; then
        # Use the merge command to expand it
        for i in {1..3}; do  # Reduced iterations to avoid excessive file size
            temp_file="$TEST_DATA_DIR/temp_$i.mcap"
            if ../../../../go/cli/mcap/bin/mcap merge "$large_file" "$large_file" --output "$temp_file" 2>/dev/null; then
                mv "$temp_file" "$large_file"
                echo "  Expanded iteration $i"
            else
                echo "  WARN: Failed to expand file at iteration $i"
                break
            fi
        done
        echo "  Generated: large_file.mcap ($(du -h "$large_file" | cut -f1))"
    else
        echo "  WARN: Go CLI not available, using base file as large_file.mcap"
    fi
elif [[ -f "../../../../testdata/mcap/demo.mcap" ]]; then
    # Fallback to demo.mcap if conformance files not available
    cp "../../../../testdata/mcap/demo.mcap" "$large_file"
    echo "  Generated: large_file.mcap (fallback to demo.mcap)"
else
    echo "  WARN: No suitable base file found for large_file.mcap"
fi

echo ""
echo "Generated edge case files in $TEST_DATA_DIR:"
if [[ -d "$TEST_DATA_DIR" ]]; then
    ls -la "$TEST_DATA_DIR"
else
    echo "  No files generated"
fi

echo ""
echo "=== Test Data Inventory ==="
echo "Edge case files: $(find "$TEST_DATA_DIR" -name "*.mcap" 2>/dev/null | wc -l)"
if [[ -d "$CONFORMANCE_DIR" ]]; then
    echo "Conformance files: $(find "$CONFORMANCE_DIR" -name "*.mcap" 2>/dev/null | wc -l)"
else
    echo "Conformance files: 0 (directory not found)"
fi
echo "Basic testdata: $(find ../../../../testdata -name "*.mcap" 2>/dev/null | wc -l)"
echo "ROS bags: $(find ../../../../testdata -name "*.bag" 2>/dev/null | wc -l)"
echo "ROS2 db3: $(find ../../../../testdata -name "*.db3" 2>/dev/null | wc -l)"
