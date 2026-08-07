package pipeline_test

import (
	"context"
	"testing"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-10-fan-out-fan-in/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-10-fan-out-fan-in/internal/simulator"
)

func newFastPool(workers int) *pipeline.FanOutPool {
	return pipeline.New(simulator.New(simulator.FastConfig), workers, 500*time.Millisecond)
}

// TestFanOut_AllResultsDelivered verifies every article produces a result.
func TestFanOut_AllResultsDelivered(t *testing.T) {
	pool := newFastPool(3)
	articles := pipeline.GenerateArticles(9)
	results, _ := pool.ProcessAll(context.Background(), articles)
	if len(results) != len(articles) {
		t.Fatalf("expected %d results, got %d", len(articles), len(results))
	}
}

// TestFanOut_FasterThanSequential verifies that fan-out within each article
// is actually faster than running tasks serially.
//
// Sequential: summarise + sentiment + keywords ≈ 3 × avg_latency
// Fan-out:    all three concurrently ≈ max(latency of each) ≈ 1 × avg_latency
func TestFanOut_FasterThanSequential(t *testing.T) {
	cfg := simulator.Config{
		MinLatency: 50 * time.Millisecond,
		MaxLatency: 80 * time.Millisecond,
		Failure:    simulator.DefaultProfile,
	}
	pool := pipeline.New(simulator.New(cfg), 1, 5*time.Second)
	articles := pipeline.GenerateArticles(1)

	_, dur := pool.ProcessAll(context.Background(), articles)

	// Sequential would take ~3 × 65ms ≈ 195ms
	// Fan-out should take ~65ms (the slowest of the three concurrent tasks)
	// We assert it finishes in less than 2× the max single-task latency.
	maxSerial := 3 * cfg.MaxLatency
	if dur > maxSerial/2 {
		t.Errorf(
			"fan-out took %v — expected less than %v (half of serial time %v)",
			dur, maxSerial/2, maxSerial,
		)
	}
}

// TestFanOut_NoRace verifies no data races under concurrency.
// Run with: go test -race ./internal/...
func TestFanOut_NoRace(t *testing.T) {
	pool := newFastPool(5)
	results, _ := pool.ProcessAll(context.Background(), pipeline.GenerateArticles(20))
	if len(results) != 20 {
		t.Errorf("expected 20 results, got %d", len(results))
	}
}

// TestFanOut_ContextCancellation verifies pipeline stops cleanly.
func TestFanOut_ContextCancellation(t *testing.T) {
	cfg := simulator.Config{
		MinLatency: 100 * time.Millisecond,
		MaxLatency: 200 * time.Millisecond,
		Failure:    simulator.DefaultProfile,
	}
	pool := pipeline.New(simulator.New(cfg), 2, 5*time.Second)
	articles := pipeline.GenerateArticles(10)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	start := time.Now()
	results, _ := pool.ProcessAll(ctx, articles)
	elapsed := time.Since(start)

	if elapsed > 2*time.Second {
		t.Errorf("cancellation took too long: %v", elapsed)
	}
	if len(results) != len(articles) {
		t.Errorf("expected %d results (including cancelled), got %d",
			len(articles), len(results))
	}
}
