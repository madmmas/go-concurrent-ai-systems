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
//
//	ChannelSemaphore — stdlib only, idiomatic Go
//	WeightedSemaphore — supports different "weights" per operation
//	  (embedding a 100-token chunk costs less than a 2000-token chunk);
//	  API mirrors golang.org/x/sync/semaphore
package pipeline

import (
	"context"
	"errors"
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

// ErrWeightTooLarge is returned when a single Acquire asks for more weight
// than the semaphore's total capacity — it could never succeed.
var ErrWeightTooLarge = errors.New("semaphore: weight exceeds capacity")

// waiter is one blocked Acquire. ready is closed when its weight is granted.
type waiter struct {
	n     int64
	ready chan struct{}
}

// WeightedSemaphore limits total concurrent "weight" rather than count.
// A heavy operation (weight=3) consumes 3 units; a light one consumes 1.
// Used when operations have different resource costs — embedding a
// 2000-token article costs the provider more than a 100-token one.
//
// The API mirrors golang.org/x/sync/semaphore.Weighted.
//
// Design:
//   - Waiters queue in FIFO order, each with its own ready channel, so a
//     blocked Acquire can select on ready AND ctx.Done(). (A sync.Cond
//     cannot be selected on — a Cond-based Acquire ignores cancellation
//     while it waits.)
//   - Release grants weight to waiters strictly from the front. A heavy
//     waiter at the front blocks lighter ones behind it; otherwise a stream
//     of small requests could starve a large one forever.
type WeightedSemaphore struct {
	mu      sync.Mutex
	size    int64
	cur     int64
	waiters []*waiter
}

// NewWeightedSemaphore returns a WeightedSemaphore with total capacity max.
func NewWeightedSemaphore(max int64) *WeightedSemaphore {
	return &WeightedSemaphore{size: max}
}

// Acquire blocks until weight units are available or ctx is done.
// On failure it returns ctx.Err() (or ErrWeightTooLarge) and holds nothing.
func (ws *WeightedSemaphore) Acquire(ctx context.Context, weight int64) error {
	ws.mu.Lock()
	if weight > ws.size {
		ws.mu.Unlock()
		return ErrWeightTooLarge
	}
	// Fast path: capacity free and nobody queued ahead of us.
	if ws.size-ws.cur >= weight && len(ws.waiters) == 0 {
		ws.cur += weight
		ws.mu.Unlock()
		return nil
	}
	w := &waiter{n: weight, ready: make(chan struct{})}
	ws.waiters = append(ws.waiters, w)
	ws.mu.Unlock()

	select {
	case <-w.ready:
		return nil
	case <-ctx.Done():
		ws.mu.Lock()
		select {
		case <-w.ready:
			// Granted in the instant ctx fired. We own the weight, so give
			// it back — the caller sees an error and will not Release.
			ws.cur -= weight
			ws.notifyLocked()
		default:
			ws.removeLocked(w)
			// If we were at the front, waiters behind us may now fit.
			ws.notifyLocked()
		}
		ws.mu.Unlock()
		return ctx.Err()
	}
}

// TryAcquire takes weight units without blocking. Reports success.
func (ws *WeightedSemaphore) TryAcquire(weight int64) bool {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if ws.size-ws.cur >= weight && len(ws.waiters) == 0 {
		ws.cur += weight
		return true
	}
	return false
}

// Release returns weight units and wakes waiters that now fit.
func (ws *WeightedSemaphore) Release(weight int64) {
	ws.mu.Lock()
	ws.cur -= weight
	if ws.cur < 0 {
		ws.mu.Unlock()
		panic("semaphore: released more than held")
	}
	ws.notifyLocked()
	ws.mu.Unlock()
}

// notifyLocked grants weight to waiters from the front of the queue,
// stopping at the first one that does not fit (FIFO, no starvation).
func (ws *WeightedSemaphore) notifyLocked() {
	for len(ws.waiters) > 0 {
		w := ws.waiters[0]
		if ws.size-ws.cur < w.n {
			return
		}
		ws.cur += w.n
		ws.waiters = ws.waiters[1:]
		close(w.ready)
	}
}

func (ws *WeightedSemaphore) removeLocked(target *waiter) {
	for i, w := range ws.waiters {
		if w == target {
			ws.waiters = append(ws.waiters[:i], ws.waiters[i+1:]...)
			return
		}
	}
}

// ─── Pipeline using channel semaphore ─────────────────────────────────────────

// SemaphorePool uses many workers but limits concurrent LLM calls via semaphore.
// Workers = total goroutines (many — for article-level parallelism)
// EmbedSlots = max concurrent embed calls (few — provider concurrency limit)
type SemaphorePool struct {
	llm      *simulator.LLMClient
	Workers  int
	EmbedSem *Semaphore // limits concurrent embedding calls
	Timeout  time.Duration
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
	jobs := make(chan model.Article, len(articles))
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
