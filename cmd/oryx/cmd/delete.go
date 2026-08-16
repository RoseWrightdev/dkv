package cmd

import (
	"fmt"

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
		return &exitError{code: 2, err: fmt.Errorf("error: delete %q: %v", key, err)}
	}
	defer func() { _ = client.Close() }()

	existed, err := client.Delete(key)
	if err != nil {
		return &exitError{code: 2, err: fmt.Errorf("error: delete %q: %v", key, err)}
	}

	if !existed {
		fmt.Println("nil")
		return &exitError{code: 1}
	}

	fmt.Println("OK")
	return nil
}
