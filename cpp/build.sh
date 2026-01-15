#!/usr/bin/env bash

set -e

conan profile detect --force

conan editable add ./mcap --name mcap --version 2.1.2

configure_and_build() {
  local target=$1
  local build_type=$2

  conan install "${target}" -s compiler.cppstd=17 -s build_type="${build_type}" --build=missing
  local multi_config_dir="${target}/build"
  local single_config_dir="${target}/build/${build_type}"
  local build_dir=""

  if [ -f "${multi_config_dir}/generators/conan_toolchain.cmake" ]; then
    build_dir="${multi_config_dir}"
  elif [ -f "${single_config_dir}/generators/conan_toolchain.cmake" ]; then
    build_dir="${single_config_dir}"
  else
    echo "Conan toolchain file not found for ${target} (${build_type})" >&2
    exit 1
  fi

  build_dir="$(cd "${build_dir}" && pwd)"
  cmake -S "${target}" -B "${build_dir}" -DCMAKE_BUILD_TYPE="${build_type}" \
    -DCMAKE_TOOLCHAIN_FILE="${build_dir}/generators/conan_toolchain.cmake" \
    -DCMAKE_POLICY_DEFAULT_CMP0091=NEW
  cmake --build "${build_dir}" --config "${build_type}"
}

if [ "$1" != "--build-tests-only" ]; then
  configure_and_build bench Release
  configure_and_build examples Release
fi

configure_and_build test Debug
