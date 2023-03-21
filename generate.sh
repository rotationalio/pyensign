#!/bin/bash
# This script generates Python code from the ensign protocol buffers. The ensign
# repository must exist in the parent directory relative to this script in order to
# generate the code.

# Find the protocol buffer source directory from the path relative to this script
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
PROTO_DIR=$(realpath $DIR/../ensign/proto)

if [ ! -d $PROTO_DIR ]; then
    echo "cannot find directory $PROTO_DIR"
    echo "please clone the ensign repository into the parent directory"
    echo "git clone git@github.com:rotationalio/ensign.git ../ensign"
    exit 0
fi

API=$PROTO_DIR/api/v1beta1
MIMETYPE=$PROTO_DIR/mimetype/v1beta1

# Build the protocol buffers using grpcio-tools
python3 -m grpc_tools.protoc -I$PROTO_DIR \
    --python_out=pyensign \
    --grpc_python_out=pyensign \
    $API/ensign.proto

python3 -m grpc_tools.protoc -I$PROTO_DIR \
    --python_out=pyensign \
    $API/event.proto \
    $API/groups.proto \
    $API/topic.proto \

python3 -m grpc_tools.protoc -I$PROTO_DIR \
    --python_out=pyensign \
    $MIMETYPE/mimetype.proto