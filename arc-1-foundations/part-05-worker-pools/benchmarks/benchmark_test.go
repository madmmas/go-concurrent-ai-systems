package benchmarks

import (
	"testing"
	"time"

	"github.com/moinuddin/go-concurrent-ai-systems/arc-1-foundations/part-05-worker-pools/internal/pipeline"
	"github.com/moinuddin/go-concurrent-ai-systems/arc-1-foundations/part-05-worker-pools/internal/simulator"
)

func newBenchPool(workers int) *pipeline.WorkerPool {
	cfg := simulator.Config{
		MinLatency: 5 * time.Millisecond,
		MaxLatency: 15 * time.Millisecond,
	}
	return pipeline.New(simulator.New(cfg), workers)
}

// BenchmarkWorkerPool_W1 simulates a single-worker pool — essentially
// sequential. Use this as the baseline denominator for the ratios below.
func BenchmarkWorkerPool_W1(b *testing.B) {
	pool := newBenchPool(1)
	articles := pipeline.GenerateArticles(20)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.ProcessAll(articles)
	}
}

// BenchmarkWorkerPool_W5 — 5 workers. A typical starting point for
// production pipelines before profiling real throughput.
func BenchmarkWorkerPool_W5(b *testing.B) {
	pool := newBenchPool(5)
	articles := pipeline.GenerateArticles(20)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.ProcessAll(articles)
	}
}

// BenchmarkWorkerPool_W20 — 20 workers. At this level with 20 articles,
// every article has its own worker and time should approach minimum latency.
// Adding more workers beyond article count gives no further improvement.
func BenchmarkWorkerPool_W20(b *testing.B) {
	pool := newBenchPool(20)
	articles := pipeline.GenerateArticles(20)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.ProcessAll(articles)
	}
}
