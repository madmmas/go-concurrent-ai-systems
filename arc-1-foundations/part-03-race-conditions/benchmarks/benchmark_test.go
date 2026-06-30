package benchmarks

import (
	"testing"
	"time"

	"github.com/moinuddin/go-concurrent-ai-systems/arc-1-foundations/part-03-race-conditions/internal/pipeline"
	"github.com/moinuddin/go-concurrent-ai-systems/arc-1-foundations/part-03-race-conditions/internal/simulator"
)

func newBenchProcessor() *pipeline.SafeProcessor {
	cfg := simulator.Config{
		MinLatency: 5 * time.Millisecond,
		MaxLatency: 15 * time.Millisecond,
	}
	return pipeline.New(simulator.New(cfg))
}

func BenchmarkProcessAll_1(b *testing.B) {
	proc := newBenchProcessor()
	articles := pipeline.GenerateArticles(1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		proc.ProcessAll(articles)
	}
}

func BenchmarkProcessAll_100(b *testing.B) {
	proc := newBenchProcessor()
	articles := pipeline.GenerateArticles(100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		proc.ProcessAll(articles)
	}
}
