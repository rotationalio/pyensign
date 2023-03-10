#!/bin/bash

# Default to the parent directory
PROTO_DIR=../ensign/proto

# Override with the -p flag
while getopts ":p:" opt; do
    case $opt in
        p) PROTO_DIR=$OPTARG ;;
        \?) echo "Invalid option -$OPTARG" >&2 ;;
    esac
done

if [ ! -d $PROTO_DIR ]; then
    echo "$PROTO_DIR directory does not exist. Please specify the protocol buffer source directory with -p or clone the repository into the parent directory."
    echo "git clone git@github.com:rotationalio/ensign.git ../ensign"
    exit 1
fi

# Build the protocol buffers
protoc -I $PROTO_DIR --python_out=src $(find $PROTO_DIR/ensign/v1beta1 -name '*.proto')
protoc -I $PROTO_DIR --python_out=src $(find $PROTO_DIR/mimetype/v1beta1 -name '*.proto')