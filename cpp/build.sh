#!/usr/bin/env bash

set -ex

conan profile detect

conan editable add ./mcap
conan install test --output-folder test/build/Debug \
  -s compiler.cppstd=17 -s build_type=Debug --build missing

if [ "$1" != "--build-tests-only" ]; then
  conan install bench --output-folder bench/build/Release \
    -s compiler.cppstd=17 -s build_type=Release --build missing
  conan install examples --output-folder examples/build/Release \
    -s compiler.cppstd=17 -s build_type=Release --build missing
  conan build examples --output-folder examples/build/Release
  conan build bench --output-folder bench/build/Release
fi

conan build test --output-folder test/build/Debug
