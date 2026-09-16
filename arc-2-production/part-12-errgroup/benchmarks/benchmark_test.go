// Package benchmarks compares errgroup fan-out against the manual
// WaitGroup+channel fan-out from Part 10.
//
// On the happy path both should have identical throughput — errgroup adds
// no overhead when nothing fails. On the failure path, errgroup is faster:
// sibling goroutines are cancelled immediately, not left to run to completion.
package benchmarks

import (
	"context"
	"testing"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-12-errgroup/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-12-errgroup/internal/simulator"
)

func BenchmarkErrgroup_HappyPath(b *testing.B) {
	pool := pipeline.New(simulator.New(simulator.FastConfig), 5, 2*time.Second)
	arts := pipeline.GenerateArticles(10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.ProcessAll(context.Background(), arts)
	}
}

func BenchmarkErrgroup_WithFailures(b *testing.B) {
	// 30% server error rate — errgroup cancels siblings on first failure.
	// Compare to a poll of Part 10 code to see the cancellation saving.
	cfg := simulator.Config{
		MinLatency: 5 * time.Millisecond,
		MaxLatency: 30 * time.Millisecond,
		Failure:    simulator.FailureProfile{ServerErrRate: 0.3},
	}
	pool := pipeline.New(simulator.New(cfg), 5, 2*time.Second)
	arts := pipeline.GenerateArticles(10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.ProcessAll(context.Background(), arts)
	}
}
