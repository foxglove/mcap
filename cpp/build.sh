#!/usr/bin/env bash

set -e

CONAN_SETTINGS=(-s compiler.cppstd=17)

# Overwrites the local default profile; fine on CI, but devs with a custom profile
# should run `conan profile detect` once and remove --force if they need to keep it.
conan profile detect --force

conan editable remove "mcap/2.1.3" 2>/dev/null || true
conan editable add mcap

conan_install() {
  conan install "$1" -of "$2" "${CONAN_SETTINGS[@]}" -s build_type="$3" --build=missing
}

conan_build() {
  conan build "$1" -of "$2" "${CONAN_SETTINGS[@]}" -s build_type="$3" --build=editable
}

conan_install test test/build/Debug Debug

if [ "$1" != "--build-tests-only" ]; then
  conan_install bench bench/build/Release Release
  conan_install examples examples/build/Release Release
  conan_build examples examples/build/Release Release
  conan_build bench bench/build/Release Release
fi

conan_build test test/build/Debug Debug
