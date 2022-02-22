#!/usr/bin/env bash

set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

cd ${SCRIPT_DIR}

mkdir -p build
cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make
