// Package benchmarks measures slog overhead vs fmt.Printf.
//
// slog with a no-op handler (io.Discard) shows the base structural overhead.
// slog with JSON to a buffer shows the serialisation cost.
// The difference from plain fmt.Printf quantifies the slog tax.
package benchmarks

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-25-slog-pprof/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-25-slog-pprof/internal/simulator"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func jsonLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func BenchmarkPipeline_TextLog(b *testing.B) {
	pool := pipeline.New(simulator.New(simulator.FastConfig), 5, 2*time.Second, discardLogger())
	arts := pipeline.GenerateArticles(10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.ProcessAll(context.Background(), arts)
	}
}

func BenchmarkPipeline_JSONLog(b *testing.B) {
	pool := pipeline.New(simulator.New(simulator.FastConfig), 5, 2*time.Second, jsonLogger())
	arts := pipeline.GenerateArticles(10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.ProcessAll(context.Background(), arts)
	}
}
