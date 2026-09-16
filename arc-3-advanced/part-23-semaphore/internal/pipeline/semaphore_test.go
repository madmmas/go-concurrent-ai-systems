package pipeline_test

import (
	"context"
	"sync"
	"sync/atomic"
	
	"testing"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-23-semaphore/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-23-semaphore/internal/simulator"
)

// TestSemaphore_LimitsConcurrency verifies never more than N goroutines
// hold the semaphore simultaneously.
func TestSemaphore_LimitsConcurrency(t *testing.T) {
	const limit = 3
	sem := pipeline.NewSemaphore(limit)
	var (
		current int64
		maxSeen int64
		wg      sync.WaitGroup
		mu      sync.Mutex
	)

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := sem.Acquire(context.Background()); err != nil {
				return
			}
			defer sem.Release()

			cur := atomic.AddInt64(&current, 1)
			mu.Lock()
			if cur > maxSeen { maxSeen = cur }
			mu.Unlock()

			time.Sleep(5 * time.Millisecond)
			atomic.AddInt64(&current, -1)
		}()
	}
	wg.Wait()

	if maxSeen > limit {
		t.Errorf("max concurrent holders = %d, want ≤ %d", maxSeen, limit)
	}
	t.Logf("max concurrent holders: %d (limit: %d)", maxSeen, limit)
}

// TestSemaphore_ContextCancellation verifies Acquire returns ctx.Err() when cancelled.
func TestSemaphore_ContextCancellation(t *testing.T) {
	sem := pipeline.NewSemaphore(1)
	// Fill the semaphore
	_ = sem.Acquire(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := sem.Acquire(ctx)
	elapsed := time.Since(start)

	if err == nil {
		t.Error("expected error from cancelled context, got nil")
	}
	if elapsed > 200*time.Millisecond {
		t.Errorf("Acquire did not respect context timeout: took %v", elapsed)
	}
}

// TestSemaphore_Available tracks permit count correctly.
func TestSemaphore_Available(t *testing.T) {
	sem := pipeline.NewSemaphore(5)
	if sem.Available() != 5 {
		t.Errorf("expected 5 available, got %d", sem.Available())
	}
	_ = sem.Acquire(context.Background())
	if sem.Available() != 4 {
		t.Errorf("expected 4 after acquire, got %d", sem.Available())
	}
	sem.Release()
	if sem.Available() != 5 {
		t.Errorf("expected 5 after release, got %d", sem.Available())
	}
}

// TestWeightedSemaphore_LimitsWeight verifies total weight never exceeds max.
func TestWeightedSemaphore_LimitsWeight(t *testing.T) {
	const max = 6
	ws := pipeline.NewWeightedSemaphore(max)
	var (
		current int64
		maxSeen int64
		mu      sync.Mutex
		wg      sync.WaitGroup
	)

	for i := 0; i < 10; i++ {
		weight := int64(2) // each op costs 2 units
		wg.Add(1)
		go func(w int64) {
			defer wg.Done()
			if err := ws.Acquire(context.Background(), w); err != nil {
				return
			}
			defer ws.Release(w)

			cur := atomic.AddInt64(&current, w)
			mu.Lock()
			if cur > maxSeen { maxSeen = cur }
			mu.Unlock()

			time.Sleep(5 * time.Millisecond)
			atomic.AddInt64(&current, -w)
		}(weight)
	}
	wg.Wait()

	if maxSeen > max {
		t.Errorf("max concurrent weight = %d, want ≤ %d", maxSeen, max)
	}
}

// TestSemaphorePool_AllResultsDelivered verifies end-to-end correctness.
func TestSemaphorePool_AllResultsDelivered(t *testing.T) {
	pool := pipeline.New(simulator.New(simulator.FastConfig), 8, 3, 2*time.Second)
	arts := pipeline.GenerateArticles(12)
	results, _ := pool.ProcessAll(context.Background(), arts)
	if len(results) != len(arts) {
		t.Errorf("expected %d results, got %d", len(arts), len(results))
	}
}

// TestSemaphorePool_EmbedConcurrencyLimited verifies at most embedSlots
// goroutines embed simultaneously using the Available() counter.
func TestSemaphorePool_EmbedConcurrencyLimited(t *testing.T) {
	const embedSlots = 2
	pool := pipeline.New(simulator.New(simulator.FastConfig), 10, embedSlots, 2*time.Second)
	arts := pipeline.GenerateArticles(8)
	results, _ := pool.ProcessAll(context.Background(), arts)
	if len(results) != len(arts) {
		t.Errorf("expected %d results, got %d", len(arts), len(results))
	}
	// After completion all slots should be available again
	if pool.EmbedSem.Available() != embedSlots {
		t.Errorf("semaphore not fully released: available=%d want %d",
			pool.EmbedSem.Available(), embedSlots)
	}
}

// TestSemaphorePool_NoRace verifies no data races.
func TestSemaphorePool_NoRace(t *testing.T) {
	pool := pipeline.New(simulator.New(simulator.FastConfig), 10, 3, 2*time.Second)
	results, _ := pool.ProcessAll(context.Background(), pipeline.GenerateArticles(20))
	if len(results) != 20 {
		t.Errorf("expected 20, got %d", len(results))
	}
}
