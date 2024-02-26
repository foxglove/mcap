#!/usr/bin/env bash

set -e

conan config init

conan editable add ./mcap mcap/1.4.0

if [ "$1" != "--build-tests-only" ]; then
  conan install bench --install-folder bench/build/Release \
    -s compiler.cppstd=17 -s build_type=Release --build missing
  conan install examples --install-folder examples/build/Release \
    -s compiler.cppstd=17 -s build_type=Release --build missing
  conan build examples --build-folder examples/build/Release
  conan build bench --build-folder bench/build/Release
fi

mkdir build
# cSpell:ignore DCMAKE
cmake . -B ./build "-DCMAKE_BUILD_TYPE=Debug"
cmake --build ./build
