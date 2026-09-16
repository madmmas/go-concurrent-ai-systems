// Package benchmarks measures singleflight deduplication savings.
//
// BenchmarkSingleflight_UniqueURLs    — no deduplication possible
// BenchmarkSingleflight_HalfDuplicate — 50% URL reuse
// BenchmarkSingleflight_AllSameURL    — maximum deduplication: 1 LLM call serves all
package benchmarks

import (
	"context"
	"testing"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-24-singleflight/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-24-singleflight/internal/simulator"
)

var fastCfg = simulator.Config{
	MinLatency: 10 * time.Millisecond,
	MaxLatency: 20 * time.Millisecond,
	Failure:    simulator.DefaultProfile,
}

func BenchmarkSingleflight_UniqueURLs(b *testing.B) {
	arts := pipeline.GenerateArticles(10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool := pipeline.New(simulator.New(fastCfg), 10, 2*time.Second)
		pool.ProcessAll(context.Background(), arts)
	}
}

func BenchmarkSingleflight_HalfDuplicate(b *testing.B) {
	arts := pipeline.GenerateArticlesWithDuplicates(10, 5)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool := pipeline.New(simulator.New(fastCfg), 10, 2*time.Second)
		pool.ProcessAll(context.Background(), arts)
	}
}

func BenchmarkSingleflight_AllSameURL(b *testing.B) {
	arts := pipeline.GenerateArticlesWithDuplicates(10, 1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool := pipeline.New(simulator.New(fastCfg), 10, 2*time.Second)
		pool.ProcessAll(context.Background(), arts)
	}
}
