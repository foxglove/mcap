#!/usr/bin/env bash
# Generates rust test fixture mcap files from `demo.mcap`.
set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
DEMO_MCAP_PATH="$SCRIPT_DIR/../../../testdata/mcap/demo.mcap"

mcap filter "$DEMO_MCAP_PATH" \
    --include-topic-regex "/diagnostics" \
    --include-topic-regex "/tf" \
    --output-compression "zstd" \
    --chunk-size 4096 \
    --output "$SCRIPT_DIR/compressed.mcap"

mcap filter "$DEMO_MCAP_PATH" \
    --include-topic-regex "/diagnostics" \
    --include-topic-regex "/tf" \
    --output-compression "none" \
    --chunk-size 4096 \
    --output "$SCRIPT_DIR/uncompressed.mcap"
