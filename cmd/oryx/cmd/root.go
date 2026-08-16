// Package cmd defines all oryx subcommands.
package cmd

import (
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/spf13/cobra"
)

// Connection flags shared by all client subcommands (get, set, delete).
var (
	flagHost     string
	flagGRPCPort int
	flagInsecure bool
	flagTLSCert  string
	flagTLSKey   string
	flagTimeout  int
)

type exitError struct {
	code int
	err  error
}

func (e *exitError) Error() string {
	if e.err != nil {
		return e.err.Error()
	}
	return fmt.Sprintf("exit code %d", e.code)
}

func (e *exitError) Unwrap() error {
	return e.err
}

var rootCmd = &cobra.Command{
	Use:   "oryx",
	Short: "oryx — command-line interface for the oryx key-value database",
	Long: `oryx lets you start an oryx server node and interact
with a running cluster over gRPC.

Connection flags (--host, --grpc-port, --insecure, --tls-cert, --tls-key)
are available on every subcommand.`,
	SilenceUsage:  true,
	SilenceErrors: true,
}

// Execute is the library entry point called from main.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		var exitErr *exitError
		if errors.As(err, &exitErr) {
			if exitErr.err != nil {
				fmt.Fprintln(os.Stderr, exitErr.err)
			}
			os.Exit(exitErr.code)
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	// Suppress library-level INFO/WARN from CLI output by default.
	// The serve subcommand resets this to INFO so startup logs are visible.
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelError,
	})))

	// Persistent flags inherited by all subcommands.
	pf := rootCmd.PersistentFlags()
	pf.StringVar(&flagHost, "host", "127.0.0.1", "oryx node host")
	pf.IntVar(&flagGRPCPort, "grpc-port", 50051, "oryx node gRPC port")
	pf.BoolVar(&flagInsecure, "insecure", false, "use insecure gRPC (no TLS)")
	pf.StringVar(&flagTLSCert, "tls-cert", "", "path to TLS certificate file")
	pf.StringVar(&flagTLSKey, "tls-key", "", "path to TLS key file")
	pf.IntVar(&flagTimeout, "timeout", 5, "gRPC request timeout in seconds")

	rootCmd.AddCommand(serveCmd)
	rootCmd.AddCommand(getCmd)
	rootCmd.AddCommand(setCmd)
	rootCmd.AddCommand(deleteCmd)
}
