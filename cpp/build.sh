#!/usr/bin/env bash

set -e

conan profile detect --force

conan editable add ./mcap --name mcap --version 2.1.2

configure_and_build() {
  local target=$1
  local build_type=$2
  local build_dir="${target}/build/${build_type}"

  conan install "${target}" -s compiler.cppstd=17 -s build_type="${build_type}" --build=missing
  cmake -S "${target}" -B "${build_dir}" -DCMAKE_BUILD_TYPE="${build_type}" \
    -DCMAKE_TOOLCHAIN_FILE="${build_dir}/generators/conan_toolchain.cmake"
  cmake --build "${build_dir}"
}

if [ "$1" != "--build-tests-only" ]; then
  configure_and_build bench Release
  configure_and_build examples Release
fi

configure_and_build test Debug
