# dkv Python Client

A modern, high-performance Python client wrapper for dkv supporting both synchronous and asynchronous (`asyncio`) interfaces.

## Development Setup

To create a virtual environment and install all dependencies (including developer tools, FastAPI, and Uvicorn for examples) using `uv`:

```bash
# 1. Create a virtual environment
uv venv

# 2. Activate the virtual environment
source .venv/bin/activate

# 3. Install in editable mode with development dependencies
uv pip install -e ".[dev]"
```

## Compilation of Protocol Buffers

To compile the `dkv.proto` definition into python files:

```bash
uv run python -m grpc_tools.protoc -I../../api --python_out=./dkv --grpc_python_out=./dkv ../../api/dkv.proto
```

## Running Examples

### Sync Client Example
```bash
uv run examples/usage.py
```

### Async Client Example
```bash
uv run examples/async_usage.py
```

### Async FastAPI Microservice
```bash
uv run uvicorn examples.fastapi_example:app --reload
```

