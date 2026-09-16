// Package benchmarks measures token streaming latency and throughput.
//
// BenchmarkStreaming_Single  — 1 worker, 1 article (latency baseline)
// BenchmarkStreaming_Parallel — N workers, N articles (parallel streams)
//
// Unlike batch pipelines, streaming latency is dominated by the inter-token
// delay (10-60ms per token × 13 tokens = 130-780ms).
// The parallel benchmark shows that multiple streams do not interfere.
package benchmarks

import (
	"context"
	"testing"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-17-token-streaming/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-17-token-streaming/internal/simulator"
)

func BenchmarkStreaming_SingleArticle(b *testing.B) {
	pool := pipeline.New(simulator.New(simulator.FastConfig), 1, 5*time.Second)
	arts := pipeline.GenerateArticles(1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.ProcessAll(context.Background(), arts)
	}
}

func BenchmarkStreaming_FiveParallel(b *testing.B) {
	pool := pipeline.New(simulator.New(simulator.FastConfig), 5, 5*time.Second)
	arts := pipeline.GenerateArticles(5)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.ProcessAll(context.Background(), arts)
	}
}
