#!/bin/sh 

if [ ! -d message-schemas ]; then
    git clone https://github.com/foxglove/message-schemas
fi

if [ ! -d ros ]; then
    find message-schemas/proto/ros -name \*.proto -exec protoc -I message-schemas/proto {} --python_out . \;
fi

protoc *.proto --python_out .
