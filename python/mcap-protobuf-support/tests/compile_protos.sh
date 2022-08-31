#!/bin/sh

protoc --proto_path=proto --python_out=. proto/test_proto/*.proto
