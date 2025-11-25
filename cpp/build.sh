#!/usr/bin/env bash

set -e

conan config init

conan editable add ./mcap mcap/2.1.2
conan install test --install-folder test/build/Debug \
  -s compiler.cppstd=17 -s build_type=Debug --build missing

if [ "$1" != "--build-tests-only" ]; then
  conan install bench --install-folder bench/build/Release \
    -s compiler.cppstd=17 -s build_type=Release --build missing
  conan install examples --install-folder examples/build/Release \
    -s compiler.cppstd=17 -s build_type=Release --build missing
  conan build examples --build-folder examples/build/Release
  conan build bench --build-folder bench/build/Release
fi

conan build test --build-folder test/build/Debug
