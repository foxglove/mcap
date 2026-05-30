# Release process

Release numbering follows a major.minor.patch format, abbreviated as "X.Y.Z" below.

CI will build the appropriate packages once tags are pushed, as described below.

## Go library

1. Update the `Version` in go/mcap/version.go
2. Tag a release matching the version number `go/mcap/vX.Y.Z`.

## CLI

Tag a release matching `releases/mcap-cli/vX.Y.Z`.

The version number is set at build time based on the tag.

## C++

1. Update the version in all relevant files
   - cpp/bench/conanfile.py
   - cpp/build-docs.sh
   - cpp/build.sh
   - cpp/docs/conanfile.py
   - cpp/examples/conanfile.py
   - cpp/mcap/conanfile.py
   - cpp/mcap/include/mcap/types.hpp (`MCAP_LIBRARY_VERSION`)
   - cpp/test/conanfile.py
2. Tag a release matching the version number `releases/cpp/vX.Y.Z`

## Python

There are several python packages; updating any follows a similar process.

1. Update the version in the appropriate `__init__.py` file
2. Tag a release matching `releases/python/PACKAGE/vX.Y.Z`
   - For example, `releases/python/mcap/v1.2.3`

## TypeScript

There are several TS packages; updating any follows a similar process.

1. Check out the version of the code you want to release
2. Update the version in `typescript/{pkg}/package.json`
3. Make a PR with your changes to package.json
4. Wait for the PR to pass CI and merge
5. Checkout main and tag the merged commit with `releases/typescript/{pkg}/v#.#.#`
6. Push the new tag to the repo with `git push origin releases/typescript/{pkg}/v#.#.#`

## Swift

Tag a release matching the version number `releases/swift/vX.Y.Z`

## Rust

1. Update the version in rust/mcap/Cargo.toml
2. Tag a release matching the version number `releases/rust/vX.Y.Z`
