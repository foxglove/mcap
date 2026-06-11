#!/usr/bin/env bash

set -e

CONAN_SETTINGS=(-s compiler.cppstd=17)

# Create a default profile if one does not exist yet. We deliberately do not pass
# --force so an existing developer profile is left untouched; CI containers start
# without a profile, so detection still runs there. compiler.cppstd is forced via
# -s below, so the detected profile's default standard does not matter.
conan profile detect 2>/dev/null || true

# `conan editable add` is idempotent, but remove any stale entry first so a moved
# checkout does not leave the editable pointing at an old path.
conan editable remove -r "mcap/2.1.3" 2>/dev/null || true
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
