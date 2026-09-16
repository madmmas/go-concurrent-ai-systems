// Benchmarks comparing unbuffered vs buffered channel pipelines.
//
// With simulated LLM latency the difference is small — the bottleneck
// is the sleep, not the channel operation. The real difference appears
// when the collector does real work between receives.
//
// Run:
//
//	go test ./benchmarks/... -bench=. -benchmem -benchtime=3s
package benchmarks

import (
	"testing"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-1-foundations/part-06-buffered-channels/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-1-foundations/part-06-buffered-channels/internal/simulator"
)

func newFastLLM() *simulator.LLMClient {
	return simulator.New(simulator.Config{
		MinLatency: 5 * time.Millisecond,
		MaxLatency: 15 * time.Millisecond,
	})
}

// BenchmarkUnbuffered_10 — baseline: every send waits for collector.
func BenchmarkUnbuffered_10(b *testing.B) {
	p := pipeline.NewUnbuffered(newFastLLM())
	articles := pipeline.GenerateArticles(10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.ProcessAll(articles)
	}
}

// BenchmarkBuffered_1_10 — buffer=1: nearly unbuffered, minimal decoupling.
func BenchmarkBuffered_1_10(b *testing.B) {
	p := pipeline.NewBuffered(newFastLLM(), 1)
	articles := pipeline.GenerateArticles(10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.ProcessAll(articles)
	}
}

// BenchmarkBuffered_10_10 — buffer=10: workers never block on send.
func BenchmarkBuffered_10_10(b *testing.B) {
	p := pipeline.NewBuffered(newFastLLM(), 10)
	articles := pipeline.GenerateArticles(10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.ProcessAll(articles)
	}
}
