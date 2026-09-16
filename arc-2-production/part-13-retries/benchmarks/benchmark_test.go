// Package benchmarks measures retry overhead at different failure rates.
//
// BenchmarkRetry_NoFailures    — baseline: retries exist but never fire
// BenchmarkRetry_LowFailures   — 10% rate limit: occasional retries
// BenchmarkRetry_HighFailures  — 40% rate limit: heavy retry pressure
//
// The ratio between these shows the retry cost model:
// at 40% failure rate with 3 attempts, expected attempts per article = ~1.9
package benchmarks

import (
	"context"
	"testing"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-13-retries/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-13-retries/internal/simulator"
)

var fastRetry = pipeline.RetryConfig{
	MaxAttempts: 3,
	BaseDelay:   5 * time.Millisecond,
	MaxDelay:    20 * time.Millisecond,
	JitterFrac:  0.0, // no jitter in benchmarks for consistency
}

func BenchmarkRetry_NoFailures(b *testing.B) {
	pool := pipeline.New(simulator.New(simulator.FastConfig), 5, 500*time.Millisecond, fastRetry)
	arts := pipeline.GenerateArticles(10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.ProcessAll(context.Background(), arts)
	}
}

func BenchmarkRetry_LowFailureRate(b *testing.B) {
	cfg := simulator.Config{
		MinLatency: 5 * time.Millisecond,
		MaxLatency: 15 * time.Millisecond,
		Failure:    simulator.FailureProfile{RateLimitRate: 0.1},
	}
	pool := pipeline.New(simulator.New(cfg), 5, 500*time.Millisecond, fastRetry)
	arts := pipeline.GenerateArticles(10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.ProcessAll(context.Background(), arts)
	}
}

func BenchmarkRetry_HighFailureRate(b *testing.B) {
	cfg := simulator.Config{
		MinLatency: 5 * time.Millisecond,
		MaxLatency: 15 * time.Millisecond,
		Failure:    simulator.FailureProfile{RateLimitRate: 0.4},
	}
	pool := pipeline.New(simulator.New(cfg), 5, 500*time.Millisecond, fastRetry)
	arts := pipeline.GenerateArticles(10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.ProcessAll(context.Background(), arts)
	}
}
