#!/usr/bin/env bash

set -e

CONAN_SETTINGS=(-s compiler.cppstd=17)

# See build.sh: create a default profile only if one does not already exist.
conan profile detect 2>/dev/null || true

# build_docs builds only the docs translation unit, in build/docs, which is
# where .hdoc.toml looks.
conan build . "${CONAN_SETTINGS[@]}" -s build_type=Release -o "&:build_docs=True" --build=missing

hdoc --verbose
