package pipeline_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-15-circuit-breaker/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-15-circuit-breaker/internal/simulator"
)

func newFastPool(threshold int, cooldown time.Duration) *pipeline.CBPool {
	cb := pipeline.NewCircuitBreaker(threshold, cooldown)
	return pipeline.New(simulator.New(simulator.FastConfig), 3, 500*time.Millisecond, cb)
}

func TestCB_ClosedStateAllowsCalls(t *testing.T) {
	pool := newFastPool(5, 1*time.Second)
	results, _ := pool.ProcessAll(context.Background(), pipeline.GenerateArticles(6))
	if len(results) != 6 {
		t.Fatalf("expected 6, got %d", len(results))
	}
}

func TestCB_OpensAfterThreshold(t *testing.T) {
	// 100% failure, threshold=2 → should open after 2 failures
	cfg := simulator.Config{
		MinLatency: 5 * time.Millisecond,
		MaxLatency: 10 * time.Millisecond,
		Failure:    simulator.FailureProfile{ServerErrRate: 1.0},
	}
	cb   := pipeline.NewCircuitBreaker(2, 10*time.Second)
	pool := pipeline.New(simulator.New(cfg), 1, 500*time.Millisecond, cb)

	results, _ := pool.ProcessAll(context.Background(), pipeline.GenerateArticles(10))

	// After 2 failures circuit opens — remaining articles should be ErrCircuitOpen
	openCount := 0
	for _, r := range results {
		if errors.Is(r.Err, pipeline.ErrCircuitOpen) {
			openCount++
		}
	}
	if openCount == 0 {
		t.Error("expected some articles to be rejected by open circuit")
	}
}

func TestCB_AllResultsAccountedFor(t *testing.T) {
	cfg := simulator.Config{
		MinLatency: 5 * time.Millisecond,
		MaxLatency: 10 * time.Millisecond,
		Failure:    simulator.FailureProfile{ServerErrRate: 0.3},
	}
	cb   := pipeline.NewCircuitBreaker(3, 50*time.Millisecond)
	pool := pipeline.New(simulator.New(cfg), 3, 200*time.Millisecond, cb)

	results, _ := pool.ProcessAll(context.Background(), pipeline.GenerateArticles(15))
	if len(results) != 15 {
		t.Errorf("expected 15 results, got %d", len(results))
	}
}
