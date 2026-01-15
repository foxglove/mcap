#!/usr/bin/env bash

set -e

conan profile detect --force

conan editable add ./mcap --name mcap --version 2.1.2
conan install docs -s compiler.cppstd=17 -s build_type=Release --build=missing
build_dir="docs/build/Release"
if [ -f "docs/build/generators/conan_toolchain.cmake" ]; then
  build_dir="docs/build"
fi

build_dir="$(cd "${build_dir}" && pwd)"
cmake -S docs -B "${build_dir}" -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_TOOLCHAIN_FILE="${build_dir}/generators/conan_toolchain.cmake"
cmake --build "${build_dir}" --config Release

hdoc --verbose
