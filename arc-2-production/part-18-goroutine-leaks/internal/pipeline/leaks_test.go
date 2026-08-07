package pipeline_test

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-18-goroutine-leaks/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-18-goroutine-leaks/internal/simulator"
)

func newPool(workers int) *pipeline.FixedPool {
	return pipeline.New(simulator.New(simulator.FastConfig), workers, 500*time.Millisecond)
}

// TestFixedPool_NoGoroutineLeak verifies that goroutine count returns to
// its pre-run level after ProcessAll completes.
// This is the core test for Part 18 — count before, run, count after, diff.
func TestFixedPool_NoGoroutineLeak(t *testing.T) {
	pool := newPool(5)
	articles := pipeline.GenerateArticles(20)

	// Give the runtime a moment to settle before counting
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	before := pipeline.CountGoroutines()

	pool.ProcessAll(context.Background(), articles)

	// Give goroutines a moment to exit after ProcessAll returns
	time.Sleep(100 * time.Millisecond)
	runtime.GC()
	after := pipeline.CountGoroutines()

	// Allow 2 goroutines of slack (test framework, GC, etc.)
	if after > before+2 {
		t.Errorf("goroutine leak detected: before=%d after=%d (leaked ~%d)",
			before, after, after-before)
	}
}

// TestFixedPool_AllResultsDelivered verifies correctness.
func TestFixedPool_AllResultsDelivered(t *testing.T) {
	pool := newPool(3)
	results, _ := pool.ProcessAll(context.Background(), pipeline.GenerateArticles(9))
	if len(results) != 9 {
		t.Fatalf("expected 9, got %d", len(results))
	}
}

// TestFixedPool_CancellationNoLeak verifies that context cancellation
// doesn't leave goroutines stuck waiting on a channel.
func TestFixedPool_CancellationNoLeak(t *testing.T) {
	cfg := simulator.Config{
		MinLatency: 200 * time.Millisecond,
		MaxLatency: 400 * time.Millisecond,
		Failure:    simulator.DefaultProfile,
	}
	pool := pipeline.New(simulator.New(cfg), 3, 5*time.Second)

	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	before := pipeline.CountGoroutines()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	pool.ProcessAll(ctx, pipeline.GenerateArticles(15))

	time.Sleep(200 * time.Millisecond)
	runtime.GC()
	after := pipeline.CountGoroutines()

	if after > before+2 {
		t.Errorf("goroutine leak on cancellation: before=%d after=%d", before, after)
	}
}
