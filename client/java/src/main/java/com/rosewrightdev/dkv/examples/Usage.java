package com.rosewrightdev.dkv.examples;

import com.rosewrightdev.dkv.DKVClient;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Example program demonstrating the usage of the DKV Java Client.
 */
public class Usage {
    public static void main(String[] args) {
        String address = "localhost:50051";
        if (args.length > 0) {
            address = args[0];
        }

        System.out.println("Connecting to DKV node at " + address + "...");
        try (DKVClient client = DKVClient.connectInsecure(address)) {
            String key = "greeting";
            String value = "Hello from the DKV Java Client!";

            System.out.println("Storing pair: " + key + " => " + value);
            client.set(key, value.getBytes(StandardCharsets.UTF_8));

            System.out.println("Retrieving key: " + key);
            Optional<byte[]> retrievedBytes = client.get(key);
            if (retrievedBytes.isPresent()) {
                String retrievedValue = new String(retrievedBytes.get(), StandardCharsets.UTF_8);
                System.out.println("Success! Retrieved value: " + retrievedValue);
            } else {
                System.out.println("Error: Key was not found!");
            }

            System.out.println("Deleting key: " + key);
            client.delete(key);

            System.out.println("Retrieving key after deletion...");
            Optional<byte[]> postDeleteBytes = client.get(key);
            if (postDeleteBytes.isPresent()) {
                System.out.println("Error: Key still exists!");
            } else {
                System.out.println("Success! Key was successfully removed.");
            }

            System.out.println("\nAll operations completed successfully!");
        } catch (Exception e) {
            System.err.println("An error occurred: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
