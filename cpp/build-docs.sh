#!/usr/bin/env bash

set -e

CONAN_SETTINGS=(-s compiler.cppstd=17)

conan profile detect --force
conan profile update settings.compiler.cppstd=17

conan editable remove "mcap/2.1.3" 2>/dev/null || true
conan editable add mcap
conan install docs -of docs/build/Release "${CONAN_SETTINGS[@]}" -s build_type=Release --build=missing

conan build docs -of docs/build/Release "${CONAN_SETTINGS[@]}" -s build_type=Release --build=editable

hdoc --verbose
