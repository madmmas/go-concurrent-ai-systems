package benchmarks

import (
	"testing"
	"time"

	"github.com/moinuddin/go-concurrent-ai-systems/arc-1-foundations/part-02-goroutines/internal/pipeline"
	"github.com/moinuddin/go-concurrent-ai-systems/arc-1-foundations/part-02-goroutines/internal/simulator"
)

func newBenchProcessor() *pipeline.ConcurrentProcessor {
	cfg := simulator.Config{
		MinLatency: 5 * time.Millisecond,
		MaxLatency: 15 * time.Millisecond,
	}
	return pipeline.New(simulator.New(cfg))
}

// BenchmarkProcessAll_1 — atomic unit. Should match Part 1's _1 benchmark
// closely: one article still does three sequential tasks internally.
func BenchmarkProcessAll_1(b *testing.B) {
	proc := newBenchProcessor()
	articles := pipeline.GenerateArticles(1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		proc.ProcessAll(articles)
	}
}

// BenchmarkProcessAll_100 — compare against Part 1's _100 benchmark.
// Expected: Part 1 ≈ 100× BenchmarkProcessAll_1.
//           Part 2 ≈ 1-2× BenchmarkProcessAll_1.
//
// Run benchstat to see the delta:
//
//	benchstat arc-1-foundations/part-01-sequential/benchmarks/baseline.txt \
//	          arc-1-foundations/part-02-goroutines/benchmarks/concurrent.txt
func BenchmarkProcessAll_100(b *testing.B) {
	proc := newBenchProcessor()
	articles := pipeline.GenerateArticles(100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		proc.ProcessAll(articles)
	}
}
