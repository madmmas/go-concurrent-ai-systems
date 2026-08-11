// Package benchmarks measures the cost and benefit of per-article timeouts.
//
// Three scenarios:
//
//  1. Generous timeout — identical to Part 7 worker pool, timeout never fires.
//     Establishes that adding context costs nothing on the happy path.
//
//  2. Tight timeout — some articles exceed the deadline and fail fast.
//     Shows that timeout enforcement is cheap: failed articles return in
//     microseconds rather than waiting for the full simulated latency.
//
//  3. Unreliable provider — 20% of calls hang until context fires.
//     Shows the fail-fast benefit: the pipeline completes much faster than
//     if workers were left waiting for the hung calls to eventually time out.
//
// Run:
//
//	go test ./benchmarks/... -bench=. -benchmem -benchtime=3s -run='^$'
package benchmarks

import (
	"context"
	"testing"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-1-foundations/part-08-context-timeouts/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-1-foundations/part-08-context-timeouts/internal/simulator"
)

func newPool(cfg simulator.Config, articleTimeout time.Duration) *pipeline.WorkerPool {
	return pipeline.New(simulator.New(cfg), 5, articleTimeout)
}

// BenchmarkGenerousTimeout — timeout never fires, measures happy-path overhead.
// Compare to Part 7's BenchmarkWorkerPool_W5 — should be nearly identical.
func BenchmarkGenerousTimeout(b *testing.B) {
	cfg := simulator.Config{
		MinLatency: 5 * time.Millisecond,
		MaxLatency: 15 * time.Millisecond,
		Failure:    simulator.DefaultProfile,
	}
	pool := newPool(cfg, 5*time.Second) // generous — never fires
	arts := pipeline.GenerateArticles(10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.ProcessAll(context.Background(), arts)
	}
}

// BenchmarkTightTimeout — some articles exceed the 20ms deadline.
// Measures cost of failure detection: timed-out articles return fast.
func BenchmarkTightTimeout(b *testing.B) {
	cfg := simulator.Config{
		MinLatency: 5 * time.Millisecond,
		MaxLatency: 40 * time.Millisecond, // some will exceed 20ms timeout
		Failure:    simulator.DefaultProfile,
	}
	pool := newPool(cfg, 20*time.Millisecond)
	arts := pipeline.GenerateArticles(10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.ProcessAll(context.Background(), arts)
	}
}

// BenchmarkUnreliableProvider — 20% of calls hang until timeout fires.
// The benefit: workers recover in ArticleTimeout rather than waiting forever.
func BenchmarkUnreliableProvider(b *testing.B) {
	cfg := simulator.Config{
		MinLatency: 5 * time.Millisecond,
		MaxLatency: 15 * time.Millisecond,
		Failure:    simulator.UnreliableProfile,
	}
	pool := newPool(cfg, 100*time.Millisecond) // short timeout exposes hung calls fast
	arts := pipeline.GenerateArticles(10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.ProcessAll(context.Background(), arts)
	}
}
