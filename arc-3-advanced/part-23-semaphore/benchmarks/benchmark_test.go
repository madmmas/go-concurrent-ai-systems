// Package benchmarks compares unrestricted vs semaphore-limited embedding.
package benchmarks

import (
	"context"
	"testing"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-23-semaphore/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-23-semaphore/internal/simulator"
)

var fastCfg = simulator.Config{
	MinLatency: 5 * time.Millisecond,
	MaxLatency: 15 * time.Millisecond,
	Failure:    simulator.DefaultProfile,
}

func BenchmarkSemaphore_EmbedSlots1(b *testing.B) {
	pool := pipeline.New(simulator.New(fastCfg), 10, 1, 2*time.Second)
	arts := pipeline.GenerateArticles(8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ { pool.ProcessAll(context.Background(), arts) }
}

func BenchmarkSemaphore_EmbedSlots3(b *testing.B) {
	pool := pipeline.New(simulator.New(fastCfg), 10, 3, 2*time.Second)
	arts := pipeline.GenerateArticles(8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ { pool.ProcessAll(context.Background(), arts) }
}

func BenchmarkSemaphore_EmbedSlotsUnlimited(b *testing.B) {
	pool := pipeline.New(simulator.New(fastCfg), 10, 10, 2*time.Second)
	arts := pipeline.GenerateArticles(8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ { pool.ProcessAll(context.Background(), arts) }
}
