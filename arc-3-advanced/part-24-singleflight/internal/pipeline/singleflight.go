// Package pipeline implements singleflight for Part 24.
//
// Problem: when 50 goroutines all request an embedding for the same article
// URL simultaneously, 50 LLM calls go out even though one result would do.
// This wastes tokens, hits rate limits, and adds latency.
//
// singleflight.Group deduplicates concurrent calls with the same key:
// only the first caller executes the function; all others block and
// share the result when it arrives.
//
// This package implements singleflight with stdlib only.
// In production: use golang.org/x/sync/singleflight directly.
package pipeline

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-24-singleflight/internal/model"
	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-24-singleflight/internal/simulator"
)

// ─── Singleflight implementation (stdlib only) ────────────────────────────────

// call represents a single in-flight call.
type call struct {
	wg  sync.WaitGroup
	val interface{}
	err error
}

// Result is what DoChan delivers — same shape as x/sync/singleflight.Result.
type Result struct {
	Val    interface{}
	Err    error
	Shared bool
}

// PanicError is returned to callers that were waiting on a call whose fn
// panicked. The goroutine that ran fn re-panics as normal.
type PanicError struct{ Value interface{} }

func (p *PanicError) Error() string { return fmt.Sprintf("singleflight: fn panicked: %v", p.Value) }

// Group deduplicates concurrent calls by key.
// API mirrors golang.org/x/sync/singleflight.Group (Do, DoChan).
type Group struct {
	mu sync.Mutex
	m  map[string]*call
	// dups counts callers that joined an in-flight call, per key.
	dups map[string]int
}

// Do executes fn if no call for key is in-flight; otherwise blocks and
// returns the result of the in-flight call.
// Returns (value, err, shared) where shared=true means more than one
// caller received this result.
func (g *Group) Do(key string, fn func() (interface{}, error)) (interface{}, error, bool) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*call)
		g.dups = make(map[string]int)
	}
	if c, ok := g.m[key]; ok {
		g.dups[key]++
		g.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err, true
	}
	c := new(call)
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	shared := g.doCall(c, key, fn)
	return c.val, c.err, shared
}

// DoChan is like Do but returns a channel, so the caller can select on it
// alongside ctx.Done() and stop waiting without cancelling the shared call.
func (g *Group) DoChan(key string, fn func() (interface{}, error)) <-chan Result {
	ch := make(chan Result, 1) // buffered: sender never blocks if caller gave up
	go func() {
		v, err, shared := g.Do(key, fn)
		ch <- Result{Val: v, Err: err, Shared: shared}
	}()
	return ch
}

// doCall runs fn and always releases waiters — even if fn panics.
func (g *Group) doCall(c *call, key string, fn func() (interface{}, error)) (shared bool) {
	normalReturn := false
	defer func() {
		r := recover()
		if !normalReturn {
			c.err = &PanicError{Value: r}
		}
		g.mu.Lock()
		shared = g.dups[key] > 0
		delete(g.m, key)
		delete(g.dups, key)
		g.mu.Unlock()
		c.wg.Done() // waiters wake with the value or the PanicError
		if !normalReturn {
			panic(r) // the goroutine that ran fn still sees its own panic
		}
	}()
	c.val, c.err = fn()
	normalReturn = true
	return
}

// ─── Embedding cache using singleflight ───────────────────────────────────────

// EmbeddingCache caches embeddings by article URL.
// Two layers:
//   - cache (map + RWMutex): successful embeddings, kept for the run
//   - group (singleflight): merges concurrent misses into one LLM call;
//     forgets the call once it returns, so a failure is NOT remembered
type EmbeddingCache struct {
	mu    sync.RWMutex
	cache map[string][]float32
	group Group
	llm   *simulator.LLMClient

	// CallTimeout bounds the shared LLM call. It is deliberately separate
	// from any one caller's deadline.
	CallTimeout time.Duration

	// Counters for observability (atomic)
	hits   int64 // served from cache
	shared int64 // joined another caller's in-flight LLM call
	calls  int64 // LLM calls actually made
}

// NewEmbeddingCache returns an EmbeddingCache backed by the given LLM client.
func NewEmbeddingCache(llm *simulator.LLMClient) *EmbeddingCache {
	return &EmbeddingCache{
		cache:       make(map[string][]float32),
		llm:         llm,
		CallTimeout: 5 * time.Second,
	}
}

func (c *EmbeddingCache) lookup(url string) ([]float32, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	emb, ok := c.cache[url]
	return emb, ok
}

// GetEmbedding returns the embedding for url, calling the LLM only if needed.
// Concurrent callers for the same url share one LLM call via singleflight.
// Each caller waits only as long as its own ctx allows.
func (c *EmbeddingCache) GetEmbedding(ctx context.Context, url string, articleID int) ([]float32, error) {
	if emb, ok := c.lookup(url); ok {
		atomic.AddInt64(&c.hits, 1)
		fmt.Printf("  [%d] embedding cache hit for %s\n", articleID, url)
		return emb, nil
	}

	ch := c.group.DoChan(url, func() (interface{}, error) {
		// Re-check: a call for this URL may have finished between our
		// cache miss and joining the group.
		if emb, ok := c.lookup(url); ok {
			return emb, nil
		}
		// Detach from the caller that happened to arrive first. If its
		// article times out, the others still need this embedding.
		// WithoutCancel keeps ctx values (trace IDs) but drops its deadline;
		// CallTimeout puts a bound back on.
		callCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.CallTimeout)
		defer cancel()

		atomic.AddInt64(&c.calls, 1)
		fmt.Printf("  [%d] embedding LLM call for %s\n", articleID, url)
		if err := c.llm.Call(callCtx, "Embed", articleID); err != nil {
			return nil, err // not cached — the next caller tries again
		}
		emb := []float32{0.1, 0.2, 0.3, float32(articleID) * 0.01}

		c.mu.Lock()
		c.cache[url] = emb
		c.mu.Unlock()
		return emb, nil
	})

	select {
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		if res.Shared {
			atomic.AddInt64(&c.shared, 1)
			fmt.Printf("  [%d] singleflight: shared embedding for %s\n", articleID, url)
		}
		return res.Val.([]float32), nil
	case <-ctx.Done():
		// This caller gives up; the shared call carries on for the others.
		return nil, ctx.Err()
	}
}

// Stats returns how embedding requests were served.
// shared counts every caller that received a shared result, including
// the one that made the call — so calls + shared can exceed requests.
func (c *EmbeddingCache) Stats() (hits, shared, calls int64) {
	return atomic.LoadInt64(&c.hits), atomic.LoadInt64(&c.shared), atomic.LoadInt64(&c.calls)
}

// CacheSize returns the number of cached embeddings.
func (c *EmbeddingCache) CacheSize() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.cache)
}

// ─── Pipeline using singleflight embedding cache ──────────────────────────────

// SingleflightPool processes articles using a singleflight embedding cache.
type SingleflightPool struct {
	llm     *simulator.LLMClient
	Workers int
	Timeout time.Duration
	cache   *EmbeddingCache
}

// New returns a SingleflightPool.
func New(llm *simulator.LLMClient, workers int, timeout time.Duration) *SingleflightPool {
	return &SingleflightPool{
		llm:     llm,
		Workers: workers,
		Timeout: timeout,
		cache:   NewEmbeddingCache(llm),
	}
}

// ProcessAll processes articles, deduplicating embedding calls via singleflight.
func (p *SingleflightPool) ProcessAll(ctx context.Context, articles []model.Article) ([]model.AIResult, time.Duration) {
	start := time.Now()
	jobs := make(chan model.Article, len(articles))
	results := make(chan model.AIResult, len(articles))

	var wg sync.WaitGroup
	for w := 0; w < p.Workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for article := range jobs {
				articleCtx, cancel := context.WithTimeout(ctx, p.Timeout)
				results <- p.processArticle(articleCtx, article)
				cancel()
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

func (p *SingleflightPool) processArticle(ctx context.Context, a model.Article) model.AIResult {
	r := model.AIResult{ArticleID: a.ID}

	emb, err := p.cache.GetEmbedding(ctx, a.URL, a.ID)
	if err != nil {
		r.Err = err
		return r
	}
	r.Embedding = emb

	if err := p.llm.Call(ctx, "Summarise", a.ID); err != nil {
		r.Err = err
		return r
	}
	r.Summary = "AI summary"
	return r
}

// CacheStats returns cache hits, shared results, and LLM calls made.
func (p *SingleflightPool) CacheStats() (hits, shared, calls int64) { return p.cache.Stats() }

// CacheSize returns the number of cached embeddings.
func (p *SingleflightPool) CacheSize() int { return p.cache.CacheSize() }

// GenerateArticles produces n articles with unique URLs.
func GenerateArticles(n int) []model.Article {
	articles := make([]model.Article, n)
	for i := range articles {
		articles[i] = model.Article{
			ID:  i + 1,
			URL: fmt.Sprintf("https://news.example.com/article/%d", i+1),
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
			ID:  i + 1,
			URL: fmt.Sprintf("https://news.example.com/article/%d", urlID),
		}
	}
	return articles
}
