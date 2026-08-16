package cmd

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/rosewrightdev/oryx/cluster/gateway"
)

var getCmd = &cobra.Command{
	Use:   "get <key>",
	Short: "Get a value by key from a running oryx node",
	Long: `Retrieve the value stored at <key> from a running oryx node over gRPC.

Exit codes:
  0  key found — value printed to stdout
  1  key not found — "nil" printed to stdout
  2  error (connection failure, bad TLS, etc.) — message printed to stderr

Example:
  oryx get mykey
  oryx --host 10.0.0.1 --grpc-port 50055 --insecure get mykey`,
	Args: cobra.ExactArgs(1),
	RunE: runGet,
}

func runGet(_ *cobra.Command, args []string) error {
	key := args[0]

	client, err := dialClient()
	if err != nil {
		return &exitError{code: 2, err: fmt.Errorf("error: get %q: %v", key, err)}
	}
	defer func() { _ = client.Close() }()

	val, exists, err := client.Get(key)
	if err != nil {
		return &exitError{code: 2, err: fmt.Errorf("error: get %q: %v", key, err)}
	}

	if !exists {
		fmt.Println("nil")
		return &exitError{code: 1}
	}

	fmt.Printf("%s\n", val)
	return nil
}

// dialClient builds a gateway.Client from the persistent root flags.
func dialClient() (*gateway.Client, error) {
	addr := fmt.Sprintf("%s:%d", flagHost, flagGRPCPort)
	timeout := time.Duration(flagTimeout) * time.Second

	if flagInsecure {
		return gateway.NewInsecureClient(addr, timeout)
	}

	if flagTLSCert != "" {
		creds, err := loadClientTLS(flagTLSCert, flagTLSKey)
		if err != nil {
			return nil, err
		}
		return gateway.NewClient(addr, timeout, creds)
	}

	return nil, fmt.Errorf("connection requires --insecure or --tls-cert/--tls-key")
}
