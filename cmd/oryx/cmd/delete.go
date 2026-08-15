package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var deleteCmd = &cobra.Command{
	Use:     "delete <key>",
	Aliases: []string{"del"},
	Short:   "Delete a key from a running oryx node",
	Long: `Remove the entry stored at <key> from a running oryx node over gRPC.

Exit codes:
  0  key existed and was deleted
  1  key not found — nothing was deleted
  2  error

Example:
  oryx delete mykey
  oryx --insecure del mykey`,
	Args: cobra.ExactArgs(1),
	RunE: runDelete,
}

func runDelete(_ *cobra.Command, args []string) error {
	key := args[0]

	client, err := dialClient()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: delete %q: %v\n", key, err)
		os.Exit(2)
	}
	defer func() { _ = client.Close() }()

	existed, err := client.Delete(key)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: delete %q: %v\n", key, err)
		os.Exit(2)
	}

	if !existed {
		fmt.Println("nil")
		os.Exit(1)
	}

	fmt.Println("OK")
	return nil
}
