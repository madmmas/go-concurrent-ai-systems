package pipeline_test

import (
	"context"
	"fmt"
	"strings"
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

// TestDedupeCache_HitDoesNotAllocate verifies the hot read path is allocation-free.
func TestDedupeCache_HitDoesNotAllocate(t *testing.T) {
	cache := &pipeline.DedupeCache{}
	process := func() model.AIResult { return model.AIResult{ArticleID: 1} }
	cache.LoadOrProcess("https://news.example.com/article/1", process)
	allocs := testing.AllocsPerRun(1000, func() {
		cache.LoadOrProcess("https://news.example.com/article/1", process)
	})
	if allocs != 0 {
		t.Errorf("cache hit allocated %.0f times per call, want 0", allocs)
	}
}

// TestPool_GetReturnsEmptyBuffer verifies GetBuffer never hands back stale bytes.
func TestPool_GetReturnsEmptyBuffer(t *testing.T) {
	b := pipeline.GetBuffer()
	b.WriteString("previous article's prompt")
	pipeline.PutBuffer(b)
	b2 := pipeline.GetBuffer()
	if b2.Len() != 0 {
		t.Errorf("pool returned dirty buffer: %q", b2.String())
	}
}

// TestPool_PromptSurvivesBufferReuse verifies the prompt string is a copy,
// not a view into a buffer that another goroutine may now be writing to.
func TestPool_PromptSurvivesBufferReuse(t *testing.T) {
	arts := pipeline.GenerateArticles(2)
	p1 := pipeline.BuildPrompt(arts[0])
	want := p1
	_ = pipeline.BuildPrompt(arts[1]) // likely reuses the same buffer
	if p1 != want || !strings.Contains(p1, arts[0].Title) {
		t.Errorf("prompt 1 was overwritten by buffer reuse: %q", p1)
	}
}

// TestPool_ConcurrentPromptsNoRace builds prompts from many goroutines.
func TestPool_ConcurrentPromptsNoRace(t *testing.T) {
	arts := pipeline.GenerateArticles(50)
	var wg sync.WaitGroup
	for _, a := range arts {
		wg.Add(1)
		go func(a model.Article) {
			defer wg.Done()
			if p := pipeline.BuildPrompt(a); !strings.Contains(p, a.Title+"\n") {
				t.Errorf("article %d: prompt missing its own title", a.ID)
			}
		}(a)
	}
	wg.Wait()
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
				mu.Lock()
				accepted++
				mu.Unlock()
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
