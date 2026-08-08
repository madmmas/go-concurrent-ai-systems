// Package benchmarks measures the deadlock-safe pipeline from Part 4.
// Unlike Part 3's benchmark (which measured a data race), this benchmark
// verifies the correct, race-free implementation performs normally.
package benchmarks

import (
	"testing"

	"github.com/madmmas/go-concurrent-ai-systems/arc-1-foundations/part-04-deadlocks/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-1-foundations/part-04-deadlocks/internal/simulator"
)

func newBench() *pipeline.SafePipeline {
	return pipeline.New(simulator.New(simulator.FastConfig))
}

// BenchmarkMutexVersion measures the mutex-based safe pipeline.
func BenchmarkMutexVersion_10(b *testing.B) {
	p := newBench()
	articles := pipeline.GenerateArticles(10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.ProcessAll(articles)
	}
}

// BenchmarkChannelVersion measures the channel-based safe pipeline.
func BenchmarkChannelVersion_10(b *testing.B) {
	p := newBench()
	articles := pipeline.GenerateArticles(10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.ProcessAllWithChannel(articles)
	}
}
