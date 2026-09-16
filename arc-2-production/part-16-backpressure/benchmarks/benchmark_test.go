// Package benchmarks shows how queue depth affects throughput and memory.
//
// BenchmarkBackpressure_DeepQueue   — queue = article count, no blocking
// BenchmarkBackpressure_ShallowQueue — queue = 2, strong backpressure
// BenchmarkBackpressure_Queue1       — extreme: producer blocks after each article
//
// Timing difference is small (bottleneck is LLM calls).
// The real difference is peak memory: DeepQueue holds all articles in memory
// simultaneously; Queue1 holds at most 1 + workers articles.
package benchmarks

import (
	"context"
	"testing"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-16-backpressure/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-16-backpressure/internal/simulator"
)

func BenchmarkBackpressure_DeepQueue(b *testing.B) {
	pool := pipeline.New(simulator.New(simulator.FastConfig), 3, 20, 500*time.Millisecond)
	arts := pipeline.GenerateArticles(10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.ProcessWithProducer(context.Background(), arts, 0)
	}
}

func BenchmarkBackpressure_ShallowQueue(b *testing.B) {
	pool := pipeline.New(simulator.New(simulator.FastConfig), 3, 2, 500*time.Millisecond)
	arts := pipeline.GenerateArticles(10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.ProcessWithProducer(context.Background(), arts, 0)
	}
}

func BenchmarkBackpressure_Queue1(b *testing.B) {
	pool := pipeline.New(simulator.New(simulator.FastConfig), 3, 1, 500*time.Millisecond)
	arts := pipeline.GenerateArticles(10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.ProcessWithProducer(context.Background(), arts, 0)
	}
}
