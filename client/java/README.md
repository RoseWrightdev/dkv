# ORYX Java Client

A Java client for interacting with oryx.

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
import com.rosewrightdev.oryx.OryxClient;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

public class App {
    public static void main(String[] args) {
        try (OryxClient client = OryxClient.connectInsecure("localhost:50051")) {
            // Set value
            client.set("myKey", "Hello, oryx!".getBytes(StandardCharsets.UTF_8));
            
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
