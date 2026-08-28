#!/bin/sh
# Compile every .kotoba unit to wasm (emitted) and run fixture tests on web.
# No network: fixtures use the vendored NoData golden.
set -eu
root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
cd "$root"

if ! command -v kotoba >/dev/null 2>&1; then
  echo "kotoba CLI is required (kotoba-lang/kotoba v0.7.2)" >&2
  exit 1
fi

compile_wasm() {
  src=$1
  out=$2
  json=$(kotoba compile "$src" --target wasm --output "$out" --json)
  echo "$json" | python3 -c '
import json,sys
d=json.load(sys.stdin)
ok=d.get("kotoba.cli/ok?")
code=d.get("kotoba.cli/code")
print(sys.argv[1], ok, code, d.get("kotoba.cli/message") or "")
if not ok or code != "emitted":
    sys.exit(1)
' "$src"
  python3 -c '
import sys
b=open(sys.argv[1],"rb").read()
if b[:4] != b"\x00asm":
    print("not wasm magic", sys.argv[1], file=sys.stderr)
    sys.exit(1)
' "$out"
}

mkdir -p /tmp/mcap-kotoba-ci
compile_wasm src/mcap.kotoba /tmp/mcap-kotoba-ci/mcap.wasm

for guest in examples/encode_header.kotoba examples/decode_magic.kotoba test/fixtures.kotoba; do
  bundled=/tmp/mcap-kotoba-ci/$(basename "$guest")
  scripts/bundle.sh "$bundled" "$guest"
  compile_wasm "$bundled" "/tmp/mcap-kotoba-ci/$(basename "$guest" .kotoba).wasm"
done

scripts/bundle.sh /tmp/mcap-kotoba-ci/fixtures.kotoba test/fixtures.kotoba
web_json=$(kotoba compile /tmp/mcap-kotoba-ci/fixtures.kotoba --target web --output /tmp/mcap-kotoba-ci/fixtures.mjs --json)
echo "$web_json" | python3 -c '
import json,sys
d=json.load(sys.stdin)
print("fixtures web", d.get("kotoba.cli/ok?"), d.get("kotoba.cli/code"), d.get("kotoba.cli/message") or "")
if not d.get("kotoba.cli/ok?") or d.get("kotoba.cli/code") != "emitted":
    sys.exit(1)
'

node scripts/run-fixtures.mjs /tmp/mcap-kotoba-ci/fixtures.mjs
