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
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-24-singleflight/internal/model"
	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-24-singleflight/internal/simulator"
)

// ─── Singleflight implementation (stdlib only) ────────────────────────────────

// call represents a single in-flight or completed call.
type call struct {
	wg  sync.WaitGroup
	val interface{}
	err error
}

// Group deduplicates concurrent calls by key.
// Identical to golang.org/x/sync/singleflight.Group semantics.
type Group struct {
	mu sync.Mutex
	m  map[string]*call
}

// Do executes fn if no call for key is in-flight; otherwise blocks and
// returns the result of the in-flight call.
// Returns (value, err, shared) where shared=true means the result was shared.
func (g *Group) Do(key string, fn func() (interface{}, error)) (interface{}, error, bool) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*call)
	}

	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err, true // shared result
	}

	c := new(call)
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	c.val, c.err = fn()
	c.wg.Done()

	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()

	return c.val, c.err, false // computed by this caller
}

// ─── Embedding cache using singleflight ───────────────────────────────────────

// EmbeddingCache caches embeddings by article URL.
// singleflight prevents duplicate in-flight calls for the same URL.
type EmbeddingCache struct {
	mu       sync.RWMutex
	cache    map[string][]float32
	group    Group
	llm      *simulator.LLMClient
	// Counters for observability
	hits   int64
	misses int64
	mu2    sync.Mutex
}

// NewEmbeddingCache returns an EmbeddingCache backed by the given LLM client.
func NewEmbeddingCache(llm *simulator.LLMClient) *EmbeddingCache {
	return &EmbeddingCache{
		cache: make(map[string][]float32),
		llm:   llm,
	}
}

// GetEmbedding returns the embedding for url, calling the LLM only if needed.
// Concurrent callers for the same url share one LLM call via singleflight.
func (c *EmbeddingCache) GetEmbedding(ctx context.Context, url string, articleID int) ([]float32, error) {
	// Check cache first (read lock)
	c.mu.RLock()
	if emb, ok := c.cache[url]; ok {
		c.mu.RUnlock()
		c.mu2.Lock(); c.hits++; c.mu2.Unlock()
		fmt.Printf("  [%d] embedding cache hit for %s\n", articleID, url)
		return emb, nil
	}
	c.mu.RUnlock()

	c.mu2.Lock(); c.misses++; c.mu2.Unlock()

	// Singleflight: only one goroutine calls LLM per URL
	val, err, shared := c.group.Do(url, func() (interface{}, error) {
		fmt.Printf("  [%d] embedding LLM call for %s\n", articleID, url)
		if err := c.llm.Call(ctx, "Embed", articleID); err != nil {
			return nil, err
		}
		emb := []float32{0.1, 0.2, 0.3, float32(articleID) * 0.01}

		c.mu.Lock()
		c.cache[url] = emb
		c.mu.Unlock()

		return emb, nil
	})

	if err != nil {
		return nil, err
	}

	if shared {
		fmt.Printf("  [%d] singleflight: shared embedding for %s\n", articleID, url)
	}
	return val.([]float32), nil
}

// Stats returns cache hit/miss counts.
func (c *EmbeddingCache) Stats() (hits, misses int64) {
	c.mu2.Lock()
	defer c.mu2.Unlock()
	return c.hits, c.misses
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
	jobs    := make(chan model.Article, len(articles))
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

	for _, a := range articles { jobs <- a }
	close(jobs)
	go func() { wg.Wait(); close(results) }()

	var out []model.AIResult
	for r := range results { out = append(out, r) }
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

// CacheStats returns the embedding cache hit/miss counts.
func (p *SingleflightPool) CacheStats() (hits, misses int64) { return p.cache.Stats() }

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
