#!/usr/bin/env bash

set -e

cmake -S docs -B docs/build/Release -DCMAKE_BUILD_TYPE=Release
cmake --build docs/build/Release

hdoc --verbose
