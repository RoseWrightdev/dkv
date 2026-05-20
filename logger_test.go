package dkv

import (
	"io"
	"log/slog"
)

// todo: refactor
func init() {
	// This keeps the terminal clean for test results.
	handler := slog.NewTextHandler(io.Discard, &slog.HandlerOptions{
		Level: slog.LevelError,
	})
	slog.SetDefault(slog.New(handler))
}
