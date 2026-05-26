from typing import Optional
import grpc

from . import dkv_pb2 as pb
from . import dkv_pb2_grpc as pb_grpc


class _InsecureCredentials:
    """Sentinel for insecure channel creation."""


_insecure_sentinel = _InsecureCredentials()



def insecure_credentials() -> grpc.ChannelCredentials:
    """Create insecure (plain-text) channel credentials.

    Mirrors the insecure credential options in the Go, Rust, and TS clients.
    """
    return _insecure_sentinel


def tls_credentials(
    root_certificates: Optional[bytes] = None,
    private_key: Optional[bytes] = None,
    certificate_chain: Optional[bytes] = None,
) -> grpc.ChannelCredentials:
    """Create TLS channel credentials using root certificates, private key, and certificate chain.

    Args:
        root_certificates: PEM-encoded root certificates, or None for system CAs.
        private_key: PEM-encoded private key for mutual TLS (mTLS).
        certificate_chain: PEM-encoded certificate chain for mutual TLS (mTLS).
    """
    return grpc.ssl_channel_credentials(
        root_certificates=root_certificates,
        private_key=private_key,
        certificate_chain=certificate_chain,
    )


class DKVClient:
    """A developer-friendly Python client for interacting with a DKV (distributed key-value) store node."""

    def __init__(
        self,
        address: str,
        credentials: grpc.ChannelCredentials,
        timeout: float = 5.0,
        options: Optional[list] = None,
    ):
        """Initialize a DKV Client.

        Args:
            address: The host and port of the target DKV node, e.g. "localhost:50051".
            credentials: Explicit channel credentials. Use `insecure_credentials()` or `tls_credentials()`.
            timeout: Default timeout in seconds for RPC calls.
            options: Optional list of gRPC channel options.
        """
        self.address = address
        self.timeout = timeout

        if credentials is _insecure_sentinel:
            self.channel = grpc.insecure_channel(address, options=options)
        else:
            self.channel = grpc.secure_channel(address, credentials, options=options)

        self.stub = pb_grpc.DkvServiceStub(self.channel)

    @classmethod
    def connect(
        cls,
        address: str,
        credentials: grpc.ChannelCredentials,
        timeout: float = 5.0,
        options: Optional[list] = None,
    ) -> "DKVClient":
        """Open a connection to a DKV node.

        Args:
            address: The host and port of the target DKV node, e.g. "localhost:50051".
            credentials: Explicit channel credentials. Use `insecure_credentials()` or `tls_credentials()`.
            timeout: Default timeout in seconds for RPC calls.
            options: Optional list of gRPC channel options.
        """
        return cls(address, credentials=credentials, timeout=timeout, options=options)


    def get(self, key: str, timeout: Optional[float] = None) -> Optional[bytes]:
        """Retrieve the value associated with `key`.

        Args:
            key: The unique string identifier whose value to retrieve.
            timeout: Optional override for the request timeout in seconds.

        Returns:
            The raw bytes stored for the key, or None if the key does not exist.
        """
        t = timeout if timeout is not None else self.timeout
        request = pb.GetRequest(key=key)
        try:
            response = self.stub.Get(request, timeout=t)
            if response.exists:
                return response.value
            return None
        except grpc.RpcError as e:
            raise e

    def set(self, key: str, value: bytes, timeout: Optional[float] = None) -> None:
        """Store a value under a given key.

        Args:
            key: The unique string identifier.
            value: The bytes payload to store.
            timeout: Optional override for the request timeout in seconds.
        """
        t = timeout if timeout is not None else self.timeout
        request = pb.SetRequest(key=key, value=value, timestamp=0, node_id="")
        try:
            self.stub.Set(request, timeout=t)
        except grpc.RpcError as e:
            raise e

    def delete(self, key: str, timeout: Optional[float] = None) -> None:
        """Remove a key and its associated value from the store.

        Args:
            key: The unique string identifier to delete.
            timeout: Optional override for the request timeout in seconds.
        """
        t = timeout if timeout is not None else self.timeout
        request = pb.DeleteRequest(key=key, timestamp=0, node_id="")
        try:
            self.stub.Delete(request, timeout=t)
        except grpc.RpcError as e:
            raise e

    def close(self) -> None:
        """Close the underlying gRPC channel.

        The client instance must not be used for any further operations after this call.
        """
        self.channel.close()

    def __enter__(self) -> "DKVClient":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
