"""A complete, production-grade example of a FastAPI microservice backed by the OryxAsyncClient.

To run this example:
1. Ensure your ORYX server is running (e.g. at 127.0.0.1:50051).
2. Install fastapi and uvicorn:
   $ pip install fastapi uvicorn
3. Run this app:
   $ uvicorn fastapi_example:app --reload
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel

from oryx import OryxAsyncClient, insecure_credentials


class KeyValueItem(BaseModel):
    value: str


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Connecting to ORYX distributed database...")
    app.state.oryx = OryxAsyncClient.connect("127.0.0.1:50051", insecure_credentials())
    yield
    print("Closing ORYX database connection...")
    await app.state.oryx.close()


app = FastAPI(
    title="oryx-py FastAPI Microservice",
    description="High-concurrency API demonstration using oryx-py",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/keys/{key}", status_code=status.HTTP_200_OK)
async def get_key(key: str):
    """Retrieve a value asynchronously from the ORYX cluster."""
    oryx: OryxAsyncClient = app.state.oryx
    try:
        # Non-blocking async get call
        raw_val = await oryx.get(key)
        if raw_val is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Key '{key}' not found in ORYX",
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
    """Store a value asynchronously in the ORYX cluster."""
    oryx: OryxAsyncClient = app.state.oryx
    try:
        # Convert string to bytes payload
        bytes_val = item.value.encode("utf-8")
        # Non-blocking async set call
        await oryx.set(key, bytes_val)
        return {"status": "success", "message": f"Stored '{key}' successfully"}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database write failed: {str(e)}",
        )


@app.delete("/keys/{key}", status_code=status.HTTP_200_OK)
async def delete_key(key: str):
    """Remove a key asynchronously from the ORYX cluster."""
    oryx: OryxAsyncClient = app.state.oryx
    try:
        # Check if the key exists before deleting
        raw_val = await oryx.get(key)
        if raw_val is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Key '{key}' does not exist, cannot delete",
            )

        # Non-blocking async delete call
        await oryx.delete(key)
        return {"status": "success", "message": f"Deleted '{key}' successfully"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database delete failed: {str(e)}",
        )
