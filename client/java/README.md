# DKV Java Client

A Java client for interacting with dkv.

## Features
- Thread-safe blocking and CompletableFuture-based async APIs.
- Automatic resource cleanup with `AutoCloseable`.

## Build
```bash
mvn clean install
```

## Running the Example
```bash
mvn exec:java
```

## Usage

```java
import com.rosewrightdev.dkv.DKVClient;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

public class App {
    public static void main(String[] args) {
        try (DKVClient client = DKVClient.connectInsecure("localhost:50051")) {
            // Set value
            client.set("myKey", "Hello, dkv!".getBytes(StandardCharsets.UTF_8));
            
            // Get value
            Optional<byte[]> value = client.get("myKey");
            value.ifPresent(bytes -> System.out.println(new String(bytes, StandardCharsets.UTF_8)));
            
            // Delete key
            client.delete("myKey");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
```
