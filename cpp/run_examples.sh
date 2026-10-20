#!/bin/bash
# Runs the built examples as a smoke test.
set -e

proto_mcap_filepath=/tmp/example_out.mcap

build/Release/bin/example_protobuf_writer $proto_mcap_filepath
build/Release/bin/example_protobuf_static_reader $proto_mcap_filepath
build/Release/bin/example_protobuf_dynamic_reader $proto_mcap_filepath
