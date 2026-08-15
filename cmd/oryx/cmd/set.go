package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var setCmd = &cobra.Command{
	Use:   "set <key> <value>",
	Short: "Set a key-value pair in a running oryx node",
	Long: `Store <value> at <key> in a running oryx node over gRPC.

Exit codes:
  0  key set successfully
  2  error (connection failure, bad TLS, etc.) — message printed to stderr

Example:
  oryx set mykey "hello world"
  oryx --insecure set mykey "hello world"`,
	Args: cobra.ExactArgs(2),
	RunE: runSet,
}

func runSet(_ *cobra.Command, args []string) error {
	key := args[0]
	value := []byte(args[1])

	client, err := dialClient()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: set %q: %v\n", key, err)
		os.Exit(2)
	}
	defer func() { _ = client.Close() }()

	if err := client.Set(key, value); err != nil {
		fmt.Fprintf(os.Stderr, "error: set %q: %v\n", key, err)
		os.Exit(2)
	}

	fmt.Println("OK")
	return nil
}
