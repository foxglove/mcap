#!/usr/bin/env bash

set -e

conan profile detect --force

conan editable remove "mcap/2.1.3" 2>/dev/null || true
conan editable add mcap
conan install docs -of docs/build/Release -s compiler.cppstd=17 -s build_type=Release --build=missing

conan build docs -of docs/build/Release --build=editable

hdoc --verbose
