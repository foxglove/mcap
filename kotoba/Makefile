.PHONY: build test

# kotoba CLI v0.7.2 must be on PATH.
build:
	kotoba compile src/mcap.kotoba --target wasm --output /tmp/mcap.wasm --json

test:
	./scripts/ci.sh
