#!/usr/bin/env bash

set -e

conan config init

conan editable add ./mcap mcap/1.4.0
conan install docs --install-folder docs/build/Release \
  -s compiler.cppstd=17 -s build_type=Release --build missing

conan build docs --build-folder docs/build/Release

hdoc --verbose
