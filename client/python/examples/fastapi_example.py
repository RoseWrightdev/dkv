"""A complete, production-grade example of a FastAPI microservice backed by the DKVAsyncClient.

To run this example:
1. Ensure your DKV server is running (e.g. at 127.0.0.1:50051).
2. Install fastapi and uvicorn:
   $ pip install fastapi uvicorn
3. Run this app:
   $ uvicorn fastapi_example:app --reload
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel

from dkv import DKVAsyncClient, insecure_credentials


class KeyValueItem(BaseModel):
    value: str


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Connecting to DKV distributed database...")
    app.state.dkv = DKVAsyncClient.connect("127.0.0.1:50051", insecure_credentials())
    yield
    print("Closing DKV database connection...")
    await app.state.dkv.close()


app = FastAPI(
    title="dkv-py FastAPI Microservice",
    description="High-concurrency API demonstration using dkv-py",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/keys/{key}", status_code=status.HTTP_200_OK)
async def get_key(key: str):
    """Retrieve a value asynchronously from the DKV cluster."""
    dkv: DKVAsyncClient = app.state.dkv
    try:
        # Non-blocking async get call
        raw_val = await dkv.get(key)
        if raw_val is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Key '{key}' not found in DKV",
            )
        return {"key": key, "value": raw_val.decode("utf-8")}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database retrieval failed: {str(e)}",
        )


@app.post("/keys/{key}", status_code=status.HTTP_201_CREATED)
async def set_key(key: str, item: KeyValueItem):
    """Store a value asynchronously in the DKV cluster."""
    dkv: DKVAsyncClient = app.state.dkv
    try:
        # Convert string to bytes payload
        bytes_val = item.value.encode("utf-8")
        # Non-blocking async set call
        await dkv.set(key, bytes_val)
        return {"status": "success", "message": f"Stored '{key}' successfully"}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database write failed: {str(e)}",
        )


@app.delete("/keys/{key}", status_code=status.HTTP_200_OK)
async def delete_key(key: str):
    """Remove a key asynchronously from the DKV cluster."""
    dkv: DKVAsyncClient = app.state.dkv
    try:
        # Check if the key exists before deleting
        raw_val = await dkv.get(key)
        if raw_val is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Key '{key}' does not exist, cannot delete",
            )

        # Non-blocking async delete call
        await dkv.delete(key)
        return {"status": "success", "message": f"Deleted '{key}' successfully"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database delete failed: {str(e)}",
        )
