#!/usr/bin/env bash

conan editable add ./mcap mcap/0.0.1
conan install bench --install-folder bench/build/Release \
  -s compiler.cppstd=17 -s build_type=Release --build missing
conan install examples --install-folder examples/build/Release \
  -s compiler.cppstd=17 -s build_type=Release --build missing
conan install test --install-folder test/build/Debug \
  -s compiler.cppstd=17 -s build_type=Debug --build missing

conan build examples --build-folder examples/build/Release
conan build bench --build-folder bench/build/Release
conan build test --build-folder test/build/Debug
