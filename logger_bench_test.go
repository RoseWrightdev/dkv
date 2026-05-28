package dkv

import (
	"io"
	"log/slog"
	"testing"
)

func BenchmarkLogger_Init(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		handler := slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})
		_ = slog.New(handler)
	}
}
