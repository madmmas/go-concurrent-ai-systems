// Package benchmarks measures the performance of the sequential pipeline,
// giving us hard numbers to compare against when concurrent designs arrive
// in Part 2.
//
// Save a baseline before moving to Part 2:
//
//	go test ./benchmarks/... -bench=. -benchmem -count=5 | tee benchmarks/baseline.txt
//
// Then compare after Part 2 is implemented:
//
//	benchstat benchmarks/baseline.txt benchmarks/concurrent.txt
package benchmarks

import (
	"testing"
	"time"

	"github.com/moinuddin/go-concurrent-ai-systems/arc-1-foundations/part-01-sequential/internal/pipeline"
	"github.com/moinuddin/go-concurrent-ai-systems/arc-1-foundations/part-01-sequential/internal/simulator"
)

func newBenchProcessor() *pipeline.Processor {
	cfg := simulator.Config{
		MinLatency: 5 * time.Millisecond,
		MaxLatency: 15 * time.Millisecond,
	}
	return pipeline.New(simulator.New(cfg))
}

// BenchmarkProcessAll_1 measures the cost of a single article — the atomic
// unit. Every multi-article result should be a near-exact multiple of this.
func BenchmarkProcessAll_1(b *testing.B) {
	proc := newBenchProcessor()
	articles := pipeline.GenerateArticles(1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		proc.ProcessAll(articles)
	}
}

// BenchmarkProcessAll_100 is the scale endpoint. Compare this against the
// concurrent version in Part 2 — the delta is the argument for goroutines.
//
// Expected sequential result: ~100× BenchmarkProcessAll_1.
// Expected concurrent result: ~1-2× BenchmarkProcessAll_1.
func BenchmarkProcessAll_100(b *testing.B) {
	proc := newBenchProcessor()
	articles := pipeline.GenerateArticles(100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		proc.ProcessAll(articles)
	}
}
