// Package pipeline demonstrates the missing sync primitives for Part 22.
//
// Arc 1 covered sync.Mutex and sync.RWMutex.
// Arc 2 used sync/atomic without explaining it.
// Part 22 covers the rest:
//
//   sync.Once   — initialise exactly once, regardless of concurrency
//   sync.Map    — concurrent-safe map for read-heavy workloads
//   sync.Pool   — reuse allocations to reduce GC pressure
//   atomic.*    — lock-free integer operations (Add, Load, Store, CAS)
//
// All demonstrated through the news platform: singleton LLM client,
// article deduplication cache, result object pool, and call counter.
package pipeline

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-22-sync-primitives/internal/model"
	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-22-sync-primitives/internal/simulator"
)

// ─── sync.Once: singleton LLM client ─────────────────────────────────────────

// ClientFactory uses sync.Once to guarantee the LLMClient is initialised
// exactly once, regardless of how many goroutines call Get() simultaneously.
//
// In Arc 1/2, every main.go called simulator.New() directly.
// In production, initialisation is expensive: TLS handshake, credential
// loading, connection pool creation. sync.Once ensures it happens once.
type ClientFactory struct {
	once   sync.Once
	client *simulator.LLMClient
	cfg    simulator.Config
}

// NewFactory returns a ClientFactory. The client is NOT created yet.
func NewFactory(cfg simulator.Config) *ClientFactory {
	return &ClientFactory{cfg: cfg}
}

// Get returns the shared LLMClient, initialising it on the first call.
// Safe to call from any number of goroutines simultaneously.
func (f *ClientFactory) Get() *simulator.LLMClient {
	f.once.Do(func() {
		fmt.Println("[factory] initialising LLM client (runs exactly once)")
		f.client = simulator.New(f.cfg)
	})
	return f.client
}

// ─── sync.Map: article deduplication cache ────────────────────────────────────

// cacheEntry pairs sync.Once with the computed result so concurrent
// LoadOrProcess callers for the same URL wait on one process() call.
type cacheEntry struct {
	once   sync.Once
	result model.AIResult
}

// DedupeCache uses sync.Map to prevent processing the same article URL twice.
// sync.Map is optimised for read-heavy workloads where the same keys are
// read repeatedly after initial write — exactly the dedup cache pattern.
//
// When to use sync.Map over map+RWMutex:
//   - sync.Map: keys written once, read many times. No iteration needed.
//   - map+RWMutex: frequent writes, iteration required, or complex operations.
//
// Note: bare Load-then-Store races can still double-call process().
// Pairing LoadOrStore with sync.Once per key (below) closes that window.
// Part 24's singleflight is the production pattern for the same problem.
type DedupeCache struct {
	seen sync.Map // map[string]*cacheEntry
}

// LoadOrProcess checks the cache for url. On a hit, returns the cached result.
// On a miss, calls process() and stores the result before returning.
// Safe for concurrent use — LoadOrStore + sync.Once guarantees only one
// goroutine executes process() for any given URL even under concurrent callers.
func (c *DedupeCache) LoadOrProcess(
	url string,
	process func() model.AIResult,
) (model.AIResult, bool) {
	actual, _ := c.seen.LoadOrStore(url, &cacheEntry{})
	entry := actual.(*cacheEntry)

	hit := true
	entry.once.Do(func() {
		entry.result = process()
		hit = false
	})
	return entry.result, hit
}

// Size returns the number of unique URLs seen.
func (c *DedupeCache) Size() int {
	count := 0
	c.seen.Range(func(_, _ any) bool { count++; return true })
	return count
}

// ─── sync.Pool: AIResult object pool ─────────────────────────────────────────

// ResultPool reuses AIResult allocations across goroutines.
// In a pipeline processing thousands of articles per minute, each article
// allocates an AIResult struct. sync.Pool keeps a per-P free-list so
// goroutines can reuse objects without heap allocation.
//
// Important: Pool objects may be collected by GC at any time.
// Always reset fields before returning an object to the pool.
var ResultPool = sync.Pool{
	New: func() any {
		return &model.AIResult{}
	},
}

// GetResult returns an AIResult from the pool (or allocates a new one).
func GetResult() *model.AIResult {
	r := ResultPool.Get().(*model.AIResult)
	// Reset all fields before use
	*r = model.AIResult{}
	return r
}

// PutResult returns an AIResult to the pool.
// Do not use r after calling PutResult.
func PutResult(r *model.AIResult) {
	ResultPool.Put(r)
}

// ─── sync/atomic: lock-free call counter ─────────────────────────────────────

// AtomicCounter demonstrates the four main atomic operations.
// Use atomic for simple integer counters shared across goroutines.
// Use sync.Mutex when you need more than one value to change atomically.
type AtomicCounter struct {
	calls   int64 // total calls
	success int64 // successful calls
	failed  int64 // failed calls
}

// RecordSuccess atomically increments both calls and success counters.
// CAS (Compare-And-Swap) pattern shown in RecordConditional below.
func (c *AtomicCounter) RecordSuccess() {
	atomic.AddInt64(&c.calls, 1)
	atomic.AddInt64(&c.success, 1)
}

// RecordFailure atomically increments both calls and failed counters.
func (c *AtomicCounter) RecordFailure() {
	atomic.AddInt64(&c.calls, 1)
	atomic.AddInt64(&c.failed, 1)
}

// Snapshot returns a consistent point-in-time read of all three counters.
// atomic.Load reads the value without acquiring any lock.
func (c *AtomicCounter) Snapshot() (calls, success, failed int64) {
	return atomic.LoadInt64(&c.calls),
		atomic.LoadInt64(&c.success),
		atomic.LoadInt64(&c.failed)
}

// RecordConditional uses Compare-And-Swap to increment only if calls < limit.
// CAS atomically: if *addr == old, set *addr = new; return whether it changed.
// This is the foundation of lock-free data structures.
func (c *AtomicCounter) RecordConditional(limit int64) bool {
	for {
		current := atomic.LoadInt64(&c.calls)
		if current >= limit {
			return false // limit reached
		}
		if atomic.CompareAndSwapInt64(&c.calls, current, current+1) {
			return true // successfully incremented
		}
		// CAS failed — another goroutine changed calls — retry
	}
}

// ─── Full pipeline using all four primitives ──────────────────────────────────

// PrimitivesPool combines Once + Map + Pool + atomic in one pipeline.
type PrimitivesPool struct {
	factory *ClientFactory
	cache   *DedupeCache
	counter *AtomicCounter
	Workers int
	Timeout time.Duration
}

// NewPrimitivesPool returns a PrimitivesPool.
func NewPrimitivesPool(factory *ClientFactory, workers int) *PrimitivesPool {
	return &PrimitivesPool{
		factory: factory,
		cache:   &DedupeCache{},
		counter: &AtomicCounter{},
		Workers: workers,
		Timeout: 5 * time.Second,
	}
}

// ProcessAll processes articles using all four sync primitives.
func (p *PrimitivesPool) ProcessAll(
	ctx context.Context, articles []model.Article,
) ([]model.AIResult, time.Duration) {
	start := time.Now()

	jobs := make(chan model.Article, len(articles))
	results := make(chan model.AIResult, len(articles))

	var wg sync.WaitGroup
	for w := 0; w < p.Workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// sync.Once: Get() returns the same client every time
			llm := p.factory.Get()

			for article := range jobs {
				// sync.Map: check dedup cache before calling LLM
				result, hit := p.cache.LoadOrProcess(article.URL, func() model.AIResult {
					// sync.Pool: get a reusable AIResult
					r := GetResult()
					defer func() { PutResult(r) }()

					articleCtx, cancel := context.WithTimeout(ctx, p.Timeout)
					defer cancel()

					if err := llm.Call(articleCtx, "Summarise", article.ID); err != nil {
						p.counter.RecordFailure() // atomic
						return model.AIResult{ArticleID: article.ID, Err: err}
					}
					p.counter.RecordSuccess() // atomic
					return model.AIResult{
						ArticleID: article.ID,
						Summary:   "AI summary",
						Sentiment: "Positive",
					}
				})

				if hit {
					result.ArticleID = article.ID // stamp current ID
					fmt.Printf("  [article %d] dedup cache hit\n", article.ID)
				}
				results <- result
			}
		}()
	}

	for _, a := range articles {
		jobs <- a
	}
	close(jobs)

	go func() { wg.Wait(); close(results) }()

	var out []model.AIResult
	for r := range results {
		out = append(out, r)
	}
	return out, time.Since(start)
}

// Stats returns call statistics from the atomic counter.
func (p *PrimitivesPool) Stats() (calls, success, failed int64) {
	return p.counter.Snapshot()
}

// CacheSize returns the number of unique URLs processed.
func (p *PrimitivesPool) CacheSize() int { return p.cache.Size() }

// GenerateArticles produces n articles with unique URLs.
func GenerateArticles(n int) []model.Article {
	articles := make([]model.Article, n)
	for i := range articles {
		articles[i] = model.Article{
			ID:    i + 1,
			Title: fmt.Sprintf("Breaking News %d", i+1),
			URL:   fmt.Sprintf("https://news.example.com/article/%d", i+1),
		}
	}
	return articles
}

// GenerateArticlesWithDuplicates produces n articles across k unique URLs.
func GenerateArticlesWithDuplicates(n, uniqueURLs int) []model.Article {
	articles := make([]model.Article, n)
	for i := range articles {
		urlID := (i % uniqueURLs) + 1
		articles[i] = model.Article{
			ID:    i + 1,
			Title: fmt.Sprintf("Breaking News %d", i+1),
			URL:   fmt.Sprintf("https://news.example.com/article/%d", urlID),
		}
	}
	return articles
}

// ArticleItem is an alias for model.Article used by cmd/ to avoid import cycles.
type ArticleItem = model.Article

// MakeArticles is an alias for GenerateArticles.
func MakeArticles(n int) []model.Article { return GenerateArticles(n) }

// MakeArticlesWithDuplicates is an alias for GenerateArticlesWithDuplicates.
func MakeArticlesWithDuplicates(n, k int) []model.Article {
	return GenerateArticlesWithDuplicates(n, k)
}
