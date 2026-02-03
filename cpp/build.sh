#!/usr/bin/env bash

set -e

conan config init
# Keep it out of the reserved _/_ user/channel!
export VERSION=mcap/2.1.2@local/testing
#TODO For Conan 1.x only. Explicit -pr:b and -pr:h with default can be removed for Conan 2.x.
export PROFILE=default
# Export version under test to local cache.
conan export ./mcap $VERSION

# Build and run basic package tests for the different supported package configurations.
rm -rf ./test_package/build
conan test ./test_package $VERSION \
    -pr:b $PROFILE -pr:h $PROFILE -s compiler.cppstd=17 -s build_type=Debug \
    --build=missing
rm -rf ./test_package/build
conan test ./test_package $VERSION \
    -pr:b $PROFILE -pr:h $PROFILE -s compiler.cppstd=17 -s build_type=Debug \
    -o mcap:with_lz4=False -o mcap:with_zstd=False \
    --build=missing
rm -rf ./test_package/build
conan test ./test_package $VERSION \
    -pr:b $PROFILE -pr:h $PROFILE -s compiler.cppstd=17 -s build_type=Debug \
    -o mcap:shared=True \
    --build=missing
rm -rf ./test_package/build
conan test ./test_package $VERSION \
    -pr:b $PROFILE -pr:h $PROFILE -s compiler.cppstd=17 -s build_type=Debug \
    -o mcap:header_only=True \
    --build=missing
rm -rf ./test_package/build
conan test ./test_package $VERSION \
    -pr:b $PROFILE -pr:h $PROFILE \
    -s compiler.cppstd=17 -s build_type=Release \
    --build=missing

# Build full test suite. Run basic self tests.
rm -rf ./test/build
conan test ./test $VERSION \
  -pr:b $PROFILE -pr:h $PROFILE -s compiler.cppstd=17 -s build_type=Release \
  --build=missing

if [ "$1" != "--build-tests-only" ]; then
  # Build and run benchmark.
  rm -rf ./bench/build
  conan test ./bench $VERSION \
    -pr:b $PROFILE -pr:h $PROFILE -s compiler.cppstd=17 -s build_type=Release \
    --build=missing
  # Build examples. Run example selftests.
  rm -rf ./examples/build
  conan test ./examples $VERSION \
    -pr:b $PROFILE -pr:h $PROFILE -s compiler.cppstd=17 -s build_type=Release \
    --build=missing
fi
