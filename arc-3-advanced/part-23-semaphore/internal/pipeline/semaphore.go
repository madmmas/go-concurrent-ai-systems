// Package pipeline demonstrates the semaphore pattern for Part 23.
//
// The worker pool from Part 7 limits goroutine count.
// A semaphore limits concurrent access to a specific section of code,
// inside goroutines that may be doing other work.
//
// News platform use case: 20 worker goroutines process articles,
// but only 5 can call the embedding API simultaneously — the provider
// has a concurrency limit, not just a rate limit.
//
// Two implementations:
//   ChannelSemaphore — stdlib only, idiomatic Go
//   WeightedSemaphore — supports different "weights" per operation
//     (embedding a 100-token chunk costs less than a 2000-token chunk)
package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-23-semaphore/internal/model"
	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-23-semaphore/internal/simulator"
)

// ─── Channel-based semaphore ──────────────────────────────────────────────────

// Semaphore limits concurrent access to a resource.
// Based on: make(chan struct{}, maxConcurrent)
//
// Why channels work as semaphores:
//   - Buffered channel with N capacity = semaphore with N permits
//   - Acquire: send into channel (blocks when full)
//   - Release: receive from channel (frees a slot)
//   - Context-aware: select on ctx.Done() for cancellable acquire
type Semaphore struct {
	ch chan struct{}
}

// NewSemaphore returns a Semaphore with the given concurrency limit.
func NewSemaphore(maxConcurrent int) *Semaphore {
	return &Semaphore{ch: make(chan struct{}, maxConcurrent)}
}

// Acquire blocks until a permit is available or ctx is cancelled.
func (s *Semaphore) Acquire(ctx context.Context) error {
	select {
	case s.ch <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Release returns a permit to the semaphore.
// Must be called exactly once per successful Acquire.
func (s *Semaphore) Release() { <-s.ch }

// Available returns the number of unused permits.
func (s *Semaphore) Available() int { return cap(s.ch) - len(s.ch) }

// ─── Weighted semaphore ───────────────────────────────────────────────────────

// WeightedSemaphore limits total concurrent "weight" rather than count.
// A heavy operation (weight=3) consumes 3 permits; a light one consumes 1.
// Used when operations have different resource costs.
type WeightedSemaphore struct {
	mu      sync.Mutex
	cond    *sync.Cond
	current int64
	max     int64
}

// NewWeightedSemaphore returns a WeightedSemaphore with total capacity max.
func NewWeightedSemaphore(max int64) *WeightedSemaphore {
	ws := &WeightedSemaphore{max: max}
	ws.cond = sync.NewCond(&ws.mu)
	return ws
}

// Acquire blocks until weight permits are available or ctx is cancelled.
func (ws *WeightedSemaphore) Acquire(ctx context.Context, weight int64) error {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	for ws.current+weight > ws.max {
		// Check context while holding the lock
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		ws.cond.Wait()
	}
	ws.current += weight
	return nil
}

// Release returns weight permits to the semaphore.
func (ws *WeightedSemaphore) Release(weight int64) {
	ws.mu.Lock()
	ws.current -= weight
	ws.cond.Broadcast()
	ws.mu.Unlock()
}

// ─── Pipeline using channel semaphore ─────────────────────────────────────────

// SemaphorePool uses many workers but limits concurrent LLM calls via semaphore.
// Workers = total goroutines (many — for article-level parallelism)
// EmbedSlots = max concurrent embed calls (few — provider concurrency limit)
type SemaphorePool struct {
	llm       *simulator.LLMClient
	Workers   int
	EmbedSem  *Semaphore // limits concurrent embedding calls
	Timeout   time.Duration
}

// New returns a SemaphorePool.
func New(llm *simulator.LLMClient, workers, embedSlots int, timeout time.Duration) *SemaphorePool {
	return &SemaphorePool{
		llm:      llm,
		Workers:  workers,
		EmbedSem: NewSemaphore(embedSlots),
		Timeout:  timeout,
	}
}

// ProcessAll processes articles — workers run concurrently but only
// embedSlots goroutines can embed simultaneously.
func (p *SemaphorePool) ProcessAll(ctx context.Context, articles []model.Article) ([]model.AIResult, time.Duration) {
	start := time.Now()
	jobs    := make(chan model.Article, len(articles))
	results := make(chan model.AIResult, len(articles))

	var wg sync.WaitGroup
	for w := 0; w < p.Workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for article := range jobs {
				select {
				case <-ctx.Done():
					results <- model.AIResult{ArticleID: article.ID, Err: ctx.Err()}
				default:
					articleCtx, cancel := context.WithTimeout(ctx, p.Timeout)
					results <- p.processArticle(articleCtx, article)
					cancel()
				}
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

func (p *SemaphorePool) processArticle(ctx context.Context, a model.Article) model.AIResult {
	r := model.AIResult{ArticleID: a.ID}

	// Summarisation: not rate-limited, all workers can run simultaneously
	if err := p.llm.Call(ctx, "Summarise", a.ID); err != nil {
		r.Err = err
		return r
	}
	r.Summary = "AI summary"

	// Embedding: acquire semaphore before calling — limits concurrency
	fmt.Printf("  [article %d] acquiring embed slot (available: %d)\n",
		a.ID, p.EmbedSem.Available())
	if err := p.EmbedSem.Acquire(ctx); err != nil {
		r.Err = err
		return r
	}
	defer p.EmbedSem.Release()
	fmt.Printf("  [article %d] embed slot acquired\n", a.ID)

	if err := p.llm.Call(ctx, "Embed", a.ID); err != nil {
		r.Err = err
		return r
	}
	r.Embedding = []float32{0.1, 0.2, 0.3}
	fmt.Printf("  [article %d] embed complete, slot released\n", a.ID)
	return r
}

// GenerateArticles produces n dummy articles.
func GenerateArticles(n int) []model.Article {
	articles := make([]model.Article, n)
	for i := range articles {
		articles[i] = model.Article{
			ID:    i + 1,
			Title: fmt.Sprintf("Breaking News %d", i+1),
			URL:   fmt.Sprintf("https://news.example.com/%d", i+1),
		}
	}
	return articles
}
