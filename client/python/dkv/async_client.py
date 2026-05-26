from typing import Optional

import grpc.aio

from . import dkv_pb2 as pb
from . import dkv_pb2_grpc as pb_grpc
from .client import _insecure_sentinel, insecure_credentials, tls_credentials


class DKVAsyncClient:
    """An asynchronous Python client for dkv"""

    def __init__(
        self,
        address: str,
        credentials: grpc.ChannelCredentials,
        timeout: float = 5.0,
        options: Optional[list] = None,
    ):
        """Initialize an Asynchronous DKV Client.

        Args:
            address: The host and port of the target DKV node, e.g. "localhost:50051".
            credentials: Explicit channel credentials. Use `insecure_credentials()` or `tls_credentials()`.
            timeout: Default timeout in seconds for RPC calls.
            options: Optional list of gRPC channel options.
        """
        self.address = address
        self.timeout = timeout

        if credentials is _insecure_sentinel:
            self.channel = grpc.aio.insecure_channel(address, options=options)
        else:
            self.channel = grpc.aio.secure_channel(
                address, credentials, options=options
            )

        self.stub = pb_grpc.DkvServiceStub(self.channel)

    @classmethod
    def connect(
        cls,
        address: str,
        credentials: grpc.ChannelCredentials,
        timeout: float = 5.0,
        options: Optional[list] = None,
    ) -> "DKVAsyncClient":
        """Open an asynchronous connection to a DKV node.

        Args:
            address: The host and port of the target DKV node, e.g. "localhost:50051".
            credentials: Explicit channel credentials. Use `insecure_credentials()` or `tls_credentials()`.
            timeout: Default timeout in seconds for RPC calls.
            options: Optional list of gRPC channel options.
        """
        return cls(address, credentials=credentials, timeout=timeout, options=options)

    async def get(self, key: str, timeout: Optional[float] = None) -> Optional[bytes]:
        """Retrieve the value associated with `key` asynchronously.

        Args:
            key: The unique string identifier whose value to retrieve.
            timeout: Optional override for the request timeout in seconds.

        Returns:
            The raw bytes stored for the key, or None if the key does not exist.
        """
        t = timeout if timeout is not None else self.timeout
        request = pb.GetRequest(key=key)
        try:
            response = await self.stub.Get(request, timeout=t)
            if response.exists:
                return response.value
            return None
        except grpc.RpcError as e:
            raise e

    async def set(
        self, key: str, value: bytes, timeout: Optional[float] = None
    ) -> None:
        """Store a value under a given key asynchronously.

        Args:
            key: The unique string identifier.
            value: The bytes payload to store.
            timeout: Optional override for the request timeout in seconds.
        """
        t = timeout if timeout is not None else self.timeout
        request = pb.SetRequest(key=key, value=value, timestamp=0, node_id="")
        try:
            await self.stub.Set(request, timeout=t)
        except grpc.RpcError as e:
            raise e

    async def delete(self, key: str, timeout: Optional[float] = None) -> None:
        """Remove a key and its associated value from the store asynchronously.

        Args:
            key: The unique string identifier to delete.
            timeout: Optional override for the request timeout in seconds.
        """
        t = timeout if timeout is not None else self.timeout
        request = pb.DeleteRequest(key=key, timestamp=0, node_id="")
        try:
            await self.stub.Delete(request, timeout=t)
        except grpc.RpcError as e:
            raise e

    async def close(self) -> None:
        """Close the underlying asynchronous gRPC channel.

        The client instance must not be used for any further operations after this call.
        """
        await self.channel.close()

    async def __aenter__(self) -> "DKVAsyncClient":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()
