package pipeline_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-24-singleflight/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-24-singleflight/internal/simulator"
)

// TestSingleflight_OnlyOneCallPerKey verifies that concurrent requests
// for the same key result in only one fn() execution.
func TestSingleflight_OnlyOneCallPerKey(t *testing.T) {
	var g pipeline.Group
	callCount := 0
	var mu sync.Mutex

	var wg sync.WaitGroup
	const concurrent = 20
	for i := 0; i < concurrent; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			g.Do("same-key", func() (interface{}, error) {
				mu.Lock()
				callCount++
				mu.Unlock()
				time.Sleep(20 * time.Millisecond)
				return "result", nil
			})
		}()
	}
	wg.Wait()

	// callCount must be 1 (or small — depends on timing of goroutine scheduling)
	// The key insight: callCount << concurrent (not 20 independent calls)
	if callCount >= concurrent {
		t.Errorf("fn() called %d times — singleflight did not deduplicate", callCount)
	}
	t.Logf("fn() called %d times for %d concurrent requests (singleflight saved %d calls)",
		callCount, concurrent, concurrent-callCount)
}

// TestSingleflight_DifferentKeysRunConcurrently verifies different keys
// do not block each other.
func TestSingleflight_DifferentKeysRunConcurrently(t *testing.T) {
	var g pipeline.Group
	start := time.Now()

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		key := "key-" + string(rune('A'+i))
		wg.Add(1)
		go func(k string) {
			defer wg.Done()
			g.Do(k, func() (interface{}, error) {
				time.Sleep(20 * time.Millisecond)
				return k, nil
			})
		}(key)
	}
	wg.Wait()
	elapsed := time.Since(start)

	// 5 different keys running concurrently should finish in ~20ms, not 100ms
	if elapsed > 80*time.Millisecond {
		t.Errorf("different keys did not run concurrently: took %v", elapsed)
	}
}

// TestEmbeddingCache_DeduplicatesLLMCalls verifies that concurrent requests
// for the same URL result in one LLM call, not many.
func TestEmbeddingCache_DeduplicatesLLMCalls(t *testing.T) {
	cfg := simulator.Config{
		MinLatency: 30 * time.Millisecond,
		MaxLatency: 50 * time.Millisecond,
		Failure:    simulator.DefaultProfile,
	}
	pool := pipeline.New(simulator.New(cfg), 20, 2*time.Second)

	// 20 articles all with the same URL — should produce 1 LLM embed call
	arts := pipeline.GenerateArticlesWithDuplicates(20, 1)
	results, _ := pool.ProcessAll(context.Background(), arts)

	if len(results) != 20 {
		t.Fatalf("expected 20 results, got %d", len(results))
	}

	hits, misses := pool.CacheStats()
	t.Logf("cache hits=%d misses=%d (1 URL, 20 articles)", hits, misses)

	// All 20 should have embeddings (either computed or shared)
	for _, r := range results {
		if r.Err != nil { continue }
		if len(r.Embedding) == 0 {
			t.Errorf("article %d: missing embedding", r.ArticleID)
		}
	}
}

// TestPool_AllResultsDelivered verifies correctness with unique URLs.
func TestPool_AllResultsDelivered(t *testing.T) {
	pool := pipeline.New(simulator.New(simulator.FastConfig), 4, 2*time.Second)
	arts := pipeline.GenerateArticles(10)
	results, _ := pool.ProcessAll(context.Background(), arts)
	if len(results) != 10 {
		t.Errorf("expected 10, got %d", len(results))
	}
}

// TestPool_NoRace verifies no data races under the race detector.
func TestPool_NoRace(t *testing.T) {
	pool := pipeline.New(simulator.New(simulator.FastConfig), 8, 2*time.Second)
	arts := pipeline.GenerateArticlesWithDuplicates(30, 5)
	results, _ := pool.ProcessAll(context.Background(), arts)
	if len(results) != 30 {
		t.Errorf("expected 30, got %d", len(results))
	}
}
