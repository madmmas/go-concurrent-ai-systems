package pipeline_test

import (
	"context"
	"testing"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-14-rate-limiting/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-14-rate-limiting/internal/simulator"
)

func TestRateLimit_AllResultsDelivered(t *testing.T) {
	// High rate limit — should not throttle
	pool := pipeline.New(simulator.New(simulator.FastConfig), 3, 500*time.Millisecond, 100, 10)
	results, _ := pool.ProcessAll(context.Background(), pipeline.GenerateArticles(9))
	if len(results) != 9 {
		t.Fatalf("expected 9 results, got %d", len(results))
	}
}

func TestRateLimit_EnforcesLimit(t *testing.T) {
	// Very tight rate: 2 calls/second, burst=1
	// 4 articles × 2 calls each = 8 calls → should take ~4s at 2/s
	pool := pipeline.New(
		simulator.New(simulator.Config{
			MinLatency: 1 * time.Millisecond,
			MaxLatency: 2 * time.Millisecond,
			Failure:    simulator.DefaultProfile,
		}),
		5, 5*time.Second, 2.0, 1,
	)
	articles := pipeline.GenerateArticles(4)

	start := time.Now()
	results, _ := pool.ProcessAll(context.Background(), articles)
	elapsed := time.Since(start)

	if len(results) != 4 {
		t.Fatalf("expected 4 results, got %d", len(results))
	}
	// 8 calls at 2/s = 4s minimum — assert at least 3s elapsed
	if elapsed < 3*time.Second {
		t.Errorf("rate limit not enforced: finished in %v (expected ≥3s)", elapsed)
	}
}

func TestRateLimit_NoRace(t *testing.T) {
	pool := pipeline.New(simulator.New(simulator.FastConfig), 5, 200*time.Millisecond, 1000, 50)
	results, _ := pool.ProcessAll(context.Background(), pipeline.GenerateArticles(20))
	if len(results) != 20 {
		t.Errorf("expected 20 results, got %d", len(results))
	}
}
