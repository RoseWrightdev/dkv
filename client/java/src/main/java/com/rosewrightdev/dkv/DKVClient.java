package com.rosewrightdev.dkv;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.rosewrightdev.dkv.proto.*;
import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.TlsChannelCredentials;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * A Java client for interacting with dkv.
 * Supports both blocking (synchronous) and non-blocking (asynchronous
 * CompletableFuture) APIs.
 */
public class DKVClient implements AutoCloseable {
    private final ManagedChannel channel;
    private final DkvServiceGrpc.DkvServiceBlockingStub blockingStub;
    private final DkvServiceGrpc.DkvServiceFutureStub futureStub;

    /**
     * Initializes a new DKVClient with an already configured gRPC channel.
     *
     * @param channel The gRPC ManagedChannel to use.
     */
    public DKVClient(ManagedChannel channel) {
        this.channel = channel;
        this.blockingStub = DkvServiceGrpc.newBlockingStub(channel);
        this.futureStub = DkvServiceGrpc.newFutureStub(channel);
    }

    /**
     * Connects to a DKV node using the specified address and credentials.
     *
     * @param address     The target node address (e.g. "localhost:50051").
     * @param credentials The gRPC ChannelCredentials to use.
     * @return A connected DKVClient instance.
     */
    public static DKVClient connect(String address, ChannelCredentials credentials) {
        String[] parts = address.split(":");
        String host = parts[0];
        int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 50051;

        ManagedChannel channel = Grpc.newChannelBuilderForAddress(host, port, credentials).build();
        return new DKVClient(channel);
    }

    /**
     * Connects to a DKV node in insecure plaintext mode.
     *
     * @param address The target node address (e.g. "localhost:50051").
     * @return A connected DKVClient instance.
     */
    public static DKVClient connectInsecure(String address) {
        return connect(address, insecureCredentials());
    }

    /**
     * Creates insecure (plain-text) channel credentials.
     *
     * @return Insecure channel credentials.
     */
    public static ChannelCredentials insecureCredentials() {
        return InsecureChannelCredentials.create();
    }

    /**
     * Creates secure TLS channel credentials using system default trust managers.
     *
     * @return TLS channel credentials.
     */
    public static ChannelCredentials tlsCredentials() {
        return TlsChannelCredentials.create();
    }

    /**
     * Creates secure TLS channel credentials using the specified root certificates file.
     *
     * @param rootCertificates The file containing PEM-encoded root certificates.
     * @return TLS channel credentials.
     */
    public static ChannelCredentials tlsCredentials(java.io.File rootCertificates) throws java.io.IOException {
        return TlsChannelCredentials.newBuilder()
                .trustManager(rootCertificates)
                .build();
    }

    /**
     * Creates secure TLS channel credentials using the specified root certificates file,
     * private key, and certificate chain for mutual TLS (mTLS).
     *
     * @param rootCertificates  The file containing PEM-encoded root certificates.
     * @param privateKey       The file containing PEM-encoded private key for client authentication.
     * @param certificateChain The file containing PEM-encoded certificate chain for client authentication.
     * @return TLS channel credentials.
     */
    public static ChannelCredentials tlsCredentials(java.io.File rootCertificates, java.io.File privateKey, java.io.File certificateChain) throws java.io.IOException {
        return TlsChannelCredentials.newBuilder()
                .trustManager(rootCertificates)
                .keyManager(privateKey, certificateChain)
                .build();
    }

    // --- Blocking/Synchronous API (Thread-Safe) ---

    /**
     * Retrieves the value associated with the given key (blocking).
     *
     * @param key The unique key string to look up.
     * @return An Optional containing the stored byte array, or Optional.empty() if
     *         not found.
     */
    public Optional<byte[]> get(String key) {
        GetRequest request = GetRequest.newBuilder()
                .setKey(key)
                .build();
        GetResponse response = blockingStub.get(request);
        if (response.getExists()) {
            return Optional.of(response.getValue().toByteArray());
        }
        return Optional.empty();
    }

    /**
     * Stores a binary value under the given key (blocking).
     *
     * @param key   The key string.
     * @param value The value bytes.
     */
    public void set(String key, byte[] value) {
        SetRequest request = SetRequest.newBuilder()
                .setKey(key)
                .setValue(ByteString.copyFrom(value))
                .build();
        blockingStub.set(request);
    }

    /**
     * Removes the key-value pair from the store (blocking).
     * Resolves successfully even if the key did not exist.
     *
     * @param key The key string to delete.
     */
    public void delete(String key) {
        DeleteRequest request = DeleteRequest.newBuilder()
                .setKey(key)
                .build();
        blockingStub.delete(request);
    }

    /**
     * Retrieves the value associated with the given key asynchronously.
     *
     * @param key The unique key string to look up.
     * @return A CompletableFuture resolving to an Optional containing the value, or
     *         Optional.empty() if not found.
     */
    public CompletableFuture<Optional<byte[]>> getAsync(String key) {
        GetRequest request = GetRequest.newBuilder()
                .setKey(key)
                .build();
        CompletableFuture<Optional<byte[]>> cf = new CompletableFuture<>();

        ListenableFuture<GetResponse> lf = futureStub.get(request);
        lf.addListener(() -> {
            try {
                GetResponse response = lf.get();
                if (response.getExists()) {
                    cf.complete(Optional.of(response.getValue().toByteArray()));
                } else {
                    cf.complete(Optional.empty());
                }
            } catch (Exception e) {
                cf.completeExceptionally(e.getCause() != null ? e.getCause() : e);
            }
        }, Objects.requireNonNull(MoreExecutors.directExecutor()));

        return cf;
    }

    /**
     * Stores a binary value under the given key asynchronously.
     *
     * @param key   The key string.
     * @param value The value bytes.
     * @return A CompletableFuture representing completion of the store operation.
     */
    public CompletableFuture<Void> setAsync(String key, byte[] value) {
        SetRequest request = SetRequest.newBuilder()
                .setKey(key)
                .setValue(ByteString.copyFrom(value))
                .build();
        CompletableFuture<Void> cf = new CompletableFuture<>();

        ListenableFuture<SetResponse> lf = futureStub.set(request);
        lf.addListener(() -> {
            try {
                lf.get();
                cf.complete(null);
            } catch (Exception e) {
                cf.completeExceptionally(e.getCause() != null ? e.getCause() : e);
            }
        }, Objects.requireNonNull(MoreExecutors.directExecutor()));

        return cf;
    }

    /**
     * Removes the key-value pair from the store asynchronously.
     *
     * @param key The key string to delete.
     * @return A CompletableFuture representing completion of the delete operation.
     */
    public CompletableFuture<Void> deleteAsync(String key) {
        DeleteRequest request = DeleteRequest.newBuilder()
                .setKey(key)
                .build();
        CompletableFuture<Void> cf = new CompletableFuture<>();

        ListenableFuture<DeleteResponse> lf = futureStub.delete(request);
        lf.addListener(() -> {
            try {
                lf.get();
                cf.complete(null);
            } catch (Exception e) {
                cf.completeExceptionally(e.getCause() != null ? e.getCause() : e);
            }
        }, Objects.requireNonNull(MoreExecutors.directExecutor()));

        return cf;
    }

    /**
     * Closes the underlying gRPC channel and frees resources.
     */
    @Override
    public void close() throws Exception {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    /**
     * Gets the underlying gRPC blocking stub.
     *
     * @return The blocking stub.
     */
    public DkvServiceGrpc.DkvServiceBlockingStub getBlockingStub() {
        return blockingStub;
    }

    /**
     * Gets the underlying gRPC future stub.
     *
     * @return The future stub.
     */
    public DkvServiceGrpc.DkvServiceFutureStub getFutureStub() {
        return futureStub;
    }

    /**
     * Gets the underlying gRPC ManagedChannel.
     *
     * @return The managed channel.
     */
    public ManagedChannel getChannel() {
        return channel;
    }
}
