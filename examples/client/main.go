package main

import (
	"fmt"
	"log"
	"time"

	"github.com/rosewrightdev/dkv"
)

func main() {
	// 1. Initialize an insecure client (for demonstration)
	// In production, you'd use NewClient with proper TLS credentials.
	client, err := dkv.NewInsecureClient("localhost:50051", 2*time.Second)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	fmt.Println("Connected to dkv server...")

	// 2. Set a key-value pair
	key := "greeting"
	value := []byte("Hello, dkv!")
	fmt.Printf("Setting key '%s' to '%s'...\n", key, string(value))
	if err := client.Set(key, value); err != nil {
		log.Fatalf("Failed to set key: %v", err)
	}

	// 3. Retrieve the value
	fmt.Printf("Getting key '%s'...\n", key)
	val, exists, err := client.Get(key)
	if err != nil {
		log.Fatalf("Failed to get key: %v", err)
	}
	if exists {
		fmt.Printf("Found value: '%s'\n", string(val))
	} else {
		fmt.Println("Key not found!")
	}

	// 4. Delete the key
	fmt.Printf("Deleting key '%s'...\n", key)
	if err := client.Delete(key); err != nil {
		log.Fatalf("Failed to delete key: %v", err)
	}

	// 5. Verify deletion
	fmt.Printf("Getting key '%s' again...\n", key)
	_, exists, err = client.Get(key)
	if err != nil {
		log.Fatalf("Failed to get key: %v", err)
	}
	if !exists {
		fmt.Println("Confirmed: key is gone.")
	}

	fmt.Println("Example finished successfully!")
}
