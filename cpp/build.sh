#!/usr/bin/env bash

set -e

cmake -S test -B test/build/Debug -DCMAKE_BUILD_TYPE=Debug

if [ "$1" != "--build-tests-only" ]; then
  cmake -S bench -B bench/build/Release -DCMAKE_BUILD_TYPE=Release
  cmake -S examples -B examples/build/Release -DCMAKE_BUILD_TYPE=Release
  cmake --build examples/build/Release
  cmake --build bench/build/Release
fi

cmake --build test/build/Debug
