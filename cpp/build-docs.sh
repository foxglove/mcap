#!/usr/bin/env bash

set -e

CONAN_SETTINGS=(-s compiler.cppstd=17)

# See build.sh: create a default profile only if one does not already exist.
conan profile detect 2>/dev/null || true

# See build.sh: remove any stale editable entry before re-adding.
conan editable remove -r "mcap/2.1.3" 2>/dev/null || true
conan editable add mcap
conan install docs -of docs/build/Release "${CONAN_SETTINGS[@]}" -s build_type=Release --build=missing

conan build docs -of docs/build/Release "${CONAN_SETTINGS[@]}" -s build_type=Release --build=editable

hdoc --verbose
