package pipeline_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-22-sync-primitives/internal/model"
	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-22-sync-primitives/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-22-sync-primitives/internal/simulator"
)

func newFactory() *pipeline.ClientFactory {
	return pipeline.NewFactory(simulator.FastConfig)
}

// TestOnce_InitialisedExactlyOnce verifies all goroutines receive the same client.
func TestOnce_InitialisedExactlyOnce(t *testing.T) {
	factory := newFactory()
	const goroutines = 50
	clients := make([]*simulator.LLMClient, goroutines)
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			clients[idx] = factory.Get()
		}(i)
	}
	wg.Wait()
	first := clients[0]
	for i, c := range clients {
		if c != first {
			t.Errorf("goroutine %d got different client pointer", i)
		}
	}
}

// TestDedupeCache_CacheHitPreventsDuplicateProcessing verifies process() runs at most once per URL.
func TestDedupeCache_CacheHitPreventsDuplicateProcessing(t *testing.T) {
	cache := &pipeline.DedupeCache{}
	callCount := 0
	var mu sync.Mutex
	var wg sync.WaitGroup
	const concurrent = 20
	for i := 0; i < concurrent; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cache.LoadOrProcess("https://news.example.com/article/1", func() model.AIResult {
				mu.Lock()
				callCount++
				mu.Unlock()
				return model.AIResult{ArticleID: 1, Summary: "summary"}
			})
		}()
	}
	wg.Wait()
	if callCount != 1 {
		t.Errorf("process() called %d times, want exactly 1 for %d concurrent requests", callCount, concurrent)
	}
}

// TestDedupeCache_UniqueURLsAllProcessed verifies all unique URLs get processed.
func TestDedupeCache_UniqueURLsAllProcessed(t *testing.T) {
	cache := &pipeline.DedupeCache{}
	for i := 0; i < 10; i++ {
		url := fmt.Sprintf("https://news.example.com/%d", i)
		cache.LoadOrProcess(url, func() model.AIResult {
			return model.AIResult{ArticleID: i}
		})
	}
	if cache.Size() != 10 {
		t.Errorf("expected 10 cache entries, got %d", cache.Size())
	}
}

// TestPool_GetReturnsValidObject verifies GetResult returns a usable zeroed AIResult.
func TestPool_GetReturnsValidObject(t *testing.T) {
	r := pipeline.GetResult()
	if r == nil {
		t.Fatal("GetResult() returned nil")
	}
	r.ArticleID = 42
	r.Summary = "test"
	pipeline.PutResult(r)
	r2 := pipeline.GetResult()
	if r2.ArticleID != 0 || r2.Summary != "" {
		t.Errorf("pool returned dirty object: ArticleID=%d Summary=%q", r2.ArticleID, r2.Summary)
	}
}

// TestAtomic_ConcurrentCounters verifies atomic counters are race-free.
func TestAtomic_ConcurrentCounters(t *testing.T) {
	counter := &pipeline.AtomicCounter{}
	const goroutines = 100
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			if idx%3 == 0 {
				counter.RecordFailure()
			} else {
				counter.RecordSuccess()
			}
		}(i)
	}
	wg.Wait()
	calls, success, failed := counter.Snapshot()
	if calls != goroutines {
		t.Errorf("calls=%d, want %d", calls, goroutines)
	}
	if success+failed != calls {
		t.Errorf("success+failed=%d != calls=%d", success+failed, calls)
	}
}

// TestAtomic_CASLimit verifies RecordConditional enforces the limit exactly.
func TestAtomic_CASLimit(t *testing.T) {
	counter := &pipeline.AtomicCounter{}
	const limit = 5
	const goroutines = 20
	accepted := int64(0)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if counter.RecordConditional(limit) {
				mu.Lock(); accepted++; mu.Unlock()
			}
		}()
	}
	wg.Wait()
	calls, _, _ := counter.Snapshot()
	if calls != limit {
		t.Errorf("calls=%d, want exactly %d", calls, limit)
	}
}

// TestPrimitivesPool_AllResultsDelivered verifies end-to-end correctness.
func TestPrimitivesPool_AllResultsDelivered(t *testing.T) {
	pool := pipeline.NewPrimitivesPool(newFactory(), 4)
	results, _ := pool.ProcessAll(context.Background(), pipeline.GenerateArticles(10))
	if len(results) != 10 {
		t.Errorf("expected 10, got %d", len(results))
	}
}

// TestPrimitivesPool_DedupReducesCalls verifies sync.Map dedup works end-to-end.
func TestPrimitivesPool_DedupReducesCalls(t *testing.T) {
	pool := pipeline.NewPrimitivesPool(newFactory(), 4)
	arts := pipeline.GenerateArticlesWithDuplicates(9, 3)
	results, _ := pool.ProcessAll(context.Background(), arts)
	if len(results) != 9 {
		t.Errorf("expected 9 results, got %d", len(results))
	}
	if pool.CacheSize() != 3 {
		t.Errorf("expected 3 cache entries, got %d", pool.CacheSize())
	}
}

// TestPrimitivesPool_NoRace verifies no data races under the race detector.
func TestPrimitivesPool_NoRace(t *testing.T) {
	pool := pipeline.NewPrimitivesPool(newFactory(), 8)
	arts := pipeline.GenerateArticlesWithDuplicates(30, 5)
	results, _ := pool.ProcessAll(context.Background(), arts)
	if len(results) != 30 {
		t.Errorf("expected 30, got %d", len(results))
	}
}
