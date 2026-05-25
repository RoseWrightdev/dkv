// Package main provides a simple command-line interface to interact with a dkv server using vanilla Go.
package main

import (
	"fmt"
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/rosewrightdev/dkv"
)

func printUsage() {
	fmt.Println("Usage:")
	fmt.Println("  go run examples/cli/main.go [flags] <command> [arguments]")
	fmt.Println("\nFlags:")
	fmt.Println("  --addr, -a    dkv server gRPC address (default: \"localhost:50051\")")
	fmt.Println("\nCommands:")
	fmt.Println("  get <key>            Retrieve a value by key")
	fmt.Println("  set <key> <value>    Store a key-value pair")
	fmt.Println("  delete <key>         Remove a key from the store")
	fmt.Println("\nExamples:")
	fmt.Println("  go run examples/cli/main.go get greeting")
	fmt.Println("  go run examples/cli/main.go --addr localhost:50055 set greeting \"Hello, dkv!\"")
	fmt.Println("  go run examples/cli/main.go delete greeting")
}

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelError,
	})))

	args := os.Args[1:]

	// Show usage help if requested or empty args
	if len(args) == 0 {
		printUsage()
		return
	}

	for _, arg := range args {
		if arg == "-h" || arg == "--help" {
			printUsage()
			return
		}
	}

	addr := "localhost:50051"
	var cmdArgs []string

	// Parse flags and commands manually with zero external dependencies
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--addr" || arg == "-a" {
			if i+1 >= len(args) {
				fmt.Println("Error: missing value for flag", arg)
				os.Exit(1)
			}
			addr = args[i+1]
			i++ // skip value argument
		} else {
			cmdArgs = append(cmdArgs, arg)
		}
	}

	if len(cmdArgs) < 1 {
		fmt.Println("Error: missing command")
		printUsage()
		os.Exit(1)
	}

	cmd := cmdArgs[0]

	client, err := dkv.NewInsecureClient(addr, 2*time.Second)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer func() { _ = client.Close() }()

	switch cmd {
	case "get":
		if len(cmdArgs) < 2 {
			fmt.Println("Error: missing key")
			fmt.Println("Usage: get <key>")
			os.Exit(1)
		}
		key := cmdArgs[1]
		val, exists, err := client.Get(key)
		if err != nil {
			log.Fatalf("Get error: %v", err)
		}
		if !exists {
			fmt.Printf("Key '%s' not found!\n", key)
			os.Exit(1)
		}
		fmt.Printf("%s\n", string(val))

	case "set":
		if len(cmdArgs) < 3 {
			fmt.Println("Error: missing key or value")
			fmt.Println("Usage: set <key> <value>")
			os.Exit(1)
		}
		key := cmdArgs[1]
		value := cmdArgs[2]
		err := client.Set(key, []byte(value))
		if err != nil {
			log.Fatalf("Set error: %v", err)
		}
		fmt.Println("OK")

	case "delete":
		if len(cmdArgs) < 2 {
			fmt.Println("Error: missing key")
			fmt.Println("Usage: delete <key>")
			os.Exit(1)
		}
		key := cmdArgs[1]
		err := client.Delete(key)
		if err != nil {
			log.Fatalf("Delete error: %v", err)
		}
		fmt.Println("OK")

	default:
		fmt.Printf("Error: unknown command '%s'\n", cmd)
		printUsage()
		os.Exit(1)
	}
}
