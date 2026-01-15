#!/usr/bin/env bash

set -e

conan profile detect --force

conan editable add ./mcap --name mcap --version 2.1.2
conan install docs -s compiler.cppstd=17 -s build_type=Release --build=missing
cmake -S docs -B docs/build/Release -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_TOOLCHAIN_FILE=docs/build/Release/generators/conan_toolchain.cmake
cmake --build docs/build/Release

hdoc --verbose
