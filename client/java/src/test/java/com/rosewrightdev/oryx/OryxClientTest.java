package com.rosewrightdev.oryx;

import io.grpc.ChannelCredentials;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests validating OryxClient.
 */
public class OryxClientTest {

    @Test
    public void testClientInstantiation() {
        // Verify that the helper builder exists and produces the class correctly
        assertThrows(IllegalArgumentException.class, () ->
            // Should fail due to invalid port / format
            OryxClient.connect("invalid_address_format:abc:xyz", OryxClient.insecureCredentials()));
    }

    @Test
    public void testConnectInsecure() {
        assertDoesNotThrow(() -> {
            // Instantiating the channel should not throw immediate errors as gRPC is lazy
            try (OryxClient client = OryxClient.connectInsecure("localhost:50051")) {
                assertNotNull(client.getChannel());
                assertNotNull(client.getBlockingStub());
                assertNotNull(client.getFutureStub());
            }
        });
    }

    @Test
    public void testAsyncAPISignatures() {
        assertDoesNotThrow(() -> {
            try (OryxClient client = OryxClient.connectInsecure("localhost:50051")) {
                // Check that the returned futures are not null and compile correctly
                CompletableFuture<Optional<byte[]>> getFuture = client.getAsync("test");
                CompletableFuture<Void> setFuture = client.setAsync("test", new byte[0]);
                CompletableFuture<Void> deleteFuture = client.deleteAsync("test");

                assertNotNull(getFuture);
                assertNotNull(setFuture);
                assertNotNull(deleteFuture);
            }
        });
    }

    @Test
    public void testBlockingAPISignatures() {
        assertDoesNotThrow(() -> {
            try (OryxClient client = OryxClient.connectInsecure("localhost:50051")) {
                // Since no server is running, the blocking stubs should throw StatusRuntimeException
                assertThrows(io.grpc.StatusRuntimeException.class, () -> client.get("test"));
                assertThrows(io.grpc.StatusRuntimeException.class, () -> client.set("test", new byte[0]));
                assertThrows(io.grpc.StatusRuntimeException.class, () -> client.delete("test"));
            }
        });
    }

    @Test
    public void testCredentialsHelpers() {
        assertDoesNotThrow(() -> {
            ChannelCredentials insecure = OryxClient.insecureCredentials();
            assertNotNull(insecure);

            ChannelCredentials tls = OryxClient.tlsCredentials();
            assertNotNull(tls);
        });
    }

    @Test
    public void testFileBasedCredentialsSignatures() {
        // Verify that helper overloads compile and throw IOException if files don't exist
        File rootCert = new File("non_existent_root.pem");
        File privateKey = new File("non_existent_key.pem");
        File certChain = new File("non_existent_chain.pem");

        assertThrows(java.io.IOException.class, () ->
            OryxClient.tlsCredentials(rootCert));

        assertThrows(java.io.IOException.class, () ->
            OryxClient.tlsCredentials(rootCert, privateKey, certChain));
    }
}
