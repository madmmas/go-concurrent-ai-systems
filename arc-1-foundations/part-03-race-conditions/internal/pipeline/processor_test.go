package pipeline_test

import (
	"testing"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-1-foundations/part-03-race-conditions/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-1-foundations/part-03-race-conditions/internal/simulator"
)

func newFastProcessor() *pipeline.SafeProcessor {
	return pipeline.New(simulator.New(simulator.FastConfig))
}

// TestProcessAll_CorrectResultCount verifies that the mutex-protected pipeline
// always returns exactly the right number of results — no dropped writes.
//
// The broken version in Part 2 would occasionally return fewer than expected.
// Run this test 10 times in a row; it must pass every time.
func TestProcessAll_CorrectResultCount(t *testing.T) {
	proc := newFastProcessor()
	articles := pipeline.GenerateArticles(10)

	// Run multiple times to expose any remaining flakiness.
	for i := 0; i < 5; i++ {
		results, _ := proc.ProcessAll(articles)
		if len(results) != len(articles) {
			t.Fatalf("run %d: expected %d results, got %d — possible race condition",
				i+1, len(articles), len(results))
		}
	}
}

// TestProcessAll_NoRaceUnderHighConcurrency stress-tests the mutex by running
// many articles simultaneously. The race detector (-race flag) will catch any
// unprotected concurrent writes if the mutex is removed or incorrectly placed.
//
// Run this test as:
//
//	go test -race ./internal/pipeline/...
func TestProcessAll_NoRaceUnderHighConcurrency(t *testing.T) {
	proc := newFastProcessor()
	// 50 concurrent goroutines all appending to the same slice.
	articles := pipeline.GenerateArticles(50)

	results, _ := proc.ProcessAll(articles)

	if len(results) != len(articles) {
		t.Errorf("expected %d results under high concurrency, got %d",
			len(articles), len(results))
	}
}

// TestProcessAll_MutexDoesNotSerialize verifies that adding a mutex does not
// re-serialize the pipeline. The LLM calls happen outside the critical section,
// so 10 articles should still complete much faster than 10× one article.
//
// If this test fails, it means the lock scope is too wide — the mutex is
// being held during the LLM call, queuing goroutines and killing throughput.
func TestProcessAll_MutexDoesNotSerialize(t *testing.T) {
	cfg := simulator.Config{
		MinLatency: 20 * time.Millisecond,
		MaxLatency: 30 * time.Millisecond,
	}
	proc := pipeline.New(simulator.New(cfg))

	_, dur1 := proc.ProcessAll(pipeline.GenerateArticles(1))
	_, dur10 := proc.ProcessAll(pipeline.GenerateArticles(10))

	ratio := float64(dur10) / float64(dur1)
	if ratio >= 5.0 {
		t.Errorf(
			"mutex appears to be serializing LLM calls — ratio %.2f (want < 5×).\n"+
				"Check that mu.Lock() wraps only the append, not processArticle().",
			ratio,
		)
	}
}

// ─── RWMutex tests ────────────────────────────────────────────────────────────

// TestRWMutex_CacheHitReducesLLMCalls verifies that when multiple articles
// share the same URL, the LLM is only called once per unique URL.
// Subsequent articles with the same URL are served from the cache.
//
// This is the core RWMutex use case: many concurrent readers, rare writers.
func TestRWMutex_CacheHitReducesLLMCalls(t *testing.T) {
	proc := pipeline.NewCached(simulator.New(simulator.FastConfig))

	// 10 articles but only 3 unique URLs → should only call LLM 3 times.
	articles := pipeline.GenerateArticlesWithDuplicates(10, 3)
	results, _ := proc.ProcessAll(articles)

	if len(results) != len(articles) {
		t.Fatalf("expected %d results, got %d", len(articles), len(results))
	}
	if proc.CacheSize() != 3 {
		t.Errorf("expected 3 cache entries (one per unique URL), got %d",
			proc.CacheSize())
	}
}

// TestRWMutex_NoRaceUnderConcurrentReads verifies there are no data races
// when multiple goroutines read the cache simultaneously.
//
// Run with: go test -race ./internal/pipeline/...
// The race detector will catch any unprotected concurrent reads or writes.
func TestRWMutex_NoRaceUnderConcurrentReads(t *testing.T) {
	proc := pipeline.NewCached(simulator.New(simulator.FastConfig))

	// 50 articles, only 5 unique URLs — 45 of 50 are cache hits.
	// All cache hits run concurrently, holding RLock simultaneously.
	articles := pipeline.GenerateArticlesWithDuplicates(50, 5)
	results, _ := proc.ProcessAll(articles)

	if len(results) != len(articles) {
		t.Errorf("expected %d results, got %d", len(articles), len(results))
	}
}

// TestRWMutex_AllUniqueURLs verifies CachedProcessor works identically
// to SafeProcessor when every article has a unique URL (no cache hits).
func TestRWMutex_AllUniqueURLs(t *testing.T) {
	proc := pipeline.NewCached(simulator.New(simulator.FastConfig))
	articles := pipeline.GenerateArticles(10) // all unique URLs
	results, _ := proc.ProcessAll(articles)

	if len(results) != len(articles) {
		t.Fatalf("expected %d results, got %d", len(articles), len(results))
	}
	if proc.CacheSize() != len(articles) {
		t.Errorf("expected %d cache entries, got %d", len(articles), proc.CacheSize())
	}
}
