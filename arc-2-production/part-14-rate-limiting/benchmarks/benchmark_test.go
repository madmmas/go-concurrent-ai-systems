// Package benchmarks proves the token bucket enforces its rate.
//
// BenchmarkRateLimit_Unlimited — no rate limit, baseline throughput
// BenchmarkRateLimit_10ps      — 10 calls/sec limit
// BenchmarkRateLimit_3ps       — 3 calls/sec limit (tight)
//
// Expected: BenchmarkRateLimit_3ps takes ~3× longer than _10ps for the
// same article count. The rate limiter is enforcing, not advisory.
package benchmarks

import (
	"context"
	"testing"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-14-rate-limiting/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-14-rate-limiting/internal/simulator"
)

var fastCfg = simulator.Config{
	MinLatency: 1 * time.Millisecond,
	MaxLatency: 2 * time.Millisecond,
	Failure:    simulator.DefaultProfile,
}

func BenchmarkRateLimit_Unlimited(b *testing.B) {
	// rate=1000/s burst=100 — effectively unlimited
	pool := pipeline.New(simulator.New(fastCfg), 5, 5*time.Second, 1000, 100)
	arts := pipeline.GenerateArticles(5) // 5 arts × 2 calls = 10 calls
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.ProcessAll(context.Background(), arts)
	}
}

func BenchmarkRateLimit_10ps(b *testing.B) {
	// 10 calls/sec, 5 articles × 2 calls = 10 calls → ~1s minimum
	pool := pipeline.New(simulator.New(fastCfg), 5, 5*time.Second, 10, 1)
	arts := pipeline.GenerateArticles(5)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.ProcessAll(context.Background(), arts)
	}
}

func BenchmarkRateLimit_3ps(b *testing.B) {
	// 3 calls/sec, 5 articles × 2 calls = 10 calls → ~3.3s minimum
	pool := pipeline.New(simulator.New(fastCfg), 5, 10*time.Second, 3, 1)
	arts := pipeline.GenerateArticles(5)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.ProcessAll(context.Background(), arts)
	}
}
