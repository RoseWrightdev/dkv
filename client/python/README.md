# dkv Python Client

A Python client wrapper for dkv.

## Installation

```bash
pip install .
```

For development dependencies (protobuf compilation tools):

```bash
pip install -e ".[dev]"
```

## Compilation of Protocol Buffers

To compile the `dkv.proto` definition into python files:

```bash
python3 -m grpc_tools.protoc -I../../api --python_out=./dkv --grpc_python_out=./dkv ../../api/dkv.proto
```
