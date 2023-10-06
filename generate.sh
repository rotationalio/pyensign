#!/bin/bash
# This script generates Python code from the ensign protocol buffers. The ensign
# repository must exist in the parent directory relative to this script in order to
# generate the code.

# Find the protocol buffer source directory from the path relative to this script
# Unless the $ENSIGN_PROTOS environment variable is set.

if [ -z "$ENSIGN_PROTOS" ]; then
    DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
    PROTO_DIR=$(realpath -q $DIR/../ensign/proto)
else
    PROTO_DIR=$ENSIGN_PROTOS
fi

if [ -z "$PROTO_DIR" ] || [ ! -d $PROTO_DIR ]; then
    echo "cannot find protocol buffers directory $PROTO_DIR"
    echo ""
    echo "please either clone the ensign repository into the parent directory:"
    echo "git clone git@github.com:rotationalio/ensign.git ../ensign"
    echo ""
    echo "or set the \$ENSIGN_PROTOS environment variable"
    exit 1
fi

API=$PROTO_DIR/api/v1beta1
MIMETYPE=$PROTO_DIR/mimetype/v1beta1
REGION=$PROTO_DIR/region/v1beta1

# Build the protocol buffers using grpcio-tools
python3 -m grpc_tools.protoc -I$PROTO_DIR \
    --python_out=pyensign \
    --grpc_python_out=pyensign \
    $API/ensign.proto

# Do not continue if grpc_tools.protoc command failed
retval=$?
if [ $retval -ne 0 ]; then
    echo "could not generate $API/ensign.proto protocol buffers"
    exit 1
fi

python3 -m grpc_tools.protoc -I$PROTO_DIR \
    --python_out=pyensign \
    $API/event.proto \
    $API/groups.proto \
    $API/topic.proto \
    $API/query.proto \

# Do not continue if grpc_tools.protoc command failed
retval=$?
if [ $retval -ne 0 ]; then
    echo "could not generate $API protocol buffers"
    exit 1
fi

python3 -m grpc_tools.protoc -I$PROTO_DIR \
    --python_out=pyensign \
    $MIMETYPE/mimetype.proto

# Do not continue if grpc_tools.protoc command failed
retval=$?
if [ $retval -ne 0 ]; then
    echo "could not generate $MIMETYPE protocol buffers"
    exit 1
fi

python3 -m grpc_tools.protoc -I$PROTO_DIR \
    --python_out=pyensign \
    $REGION/region.proto

# Do not continue if grpc_tools.protoc command failed
retval=$?
if [ $retval -ne 0 ]; then
    echo "could not generate $REGION protocol buffers"
    exit 1
fi

# Fix the imports
sed -i'.bak' 's/from api.v1beta1/from pyensign.api.v1beta1/g' pyensign/api/v1beta1/*.py
sed -i'.bak' 's/from api.v1beta1/from pyensign.api.v1beta1/g' pyensign/mimetype/v1beta1/*.py
sed -i'.bak' 's/from region.v1beta1/from pyensign.region.v1beta1/g' pyensign/api/v1beta1/*.py
sed -i'.bak' 's/from mimetype.v1beta1/from pyensign.mimetype.v1beta1/g' pyensign/api/v1beta1/*.py
sed -i'.bak' 's/from mimetype.v1beta1/from pyensign.mimetype.v1beta1/g' pyensign/mimetype/v1beta1/*.py
rm pyensign/api/v1beta1/*.bak
rm pyensign/mimetype/v1beta1/*.bak