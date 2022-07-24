#!/bin/sh

protoc complex_message.proto --python_out . -o complex_message.fds
protoc simple_message.proto --python_out . -o simple_message.fds