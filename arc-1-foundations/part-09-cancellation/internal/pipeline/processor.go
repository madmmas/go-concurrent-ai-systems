// Package pipeline implements graceful shutdown via context cancellation.
//
// Part 9 adds two things to the Part 8 worker pool:
//
//  1. The pipeline accepts an externally-controlled context — one the caller
//     can cancel at any time, for any reason (OS signal, deadline, test teardown).
//
//  2. A ShutdownReport summarises what completed, what was cancelled, and
//     what was still queued but never started — so the caller knows the
//     exact state of the pipeline when it stopped.
//
// This is how every production Go service handles SIGTERM: a top-level
// context is cancelled, in-flight work is cancelled via the context hierarchy,
// queued work is drained and reported as Queued, and the process exits cleanly.
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-1-foundations/part-09-cancellation/internal/model"
	"github.com/madmmas/go-concurrent-ai-systems/arc-1-foundations/part-09-cancellation/internal/simulator"
)

// ErrQueued marks an article that was drained from the jobs channel after
// cancellation without ever starting LLM work. Distinct from context.Canceled
// on an in-flight article (which counts as Cancelled).
var ErrQueued = errors.New("article never started — drained from queue")

// ShutdownReport summarises the state of the pipeline when it stopped.
// In normal operation all counts go to Succeeded. Under cancellation,
// some go to Cancelled (in-flight) and some to Queued (never started).
type ShutdownReport struct {
	Succeeded int
	Failed    int // timed out or hard error
	Cancelled int // context cancelled while in-flight
	Queued    int // never started — drained after pipeline stopped
	Duration  time.Duration
}

func (r ShutdownReport) String() string {
	return fmt.Sprintf(
		"succeeded=%d failed=%d cancelled=%d queued=%d duration=%v",
		r.Succeeded, r.Failed, r.Cancelled, r.Queued, r.Duration.Round(time.Millisecond),
	)
}

// WorkerPool processes articles with graceful shutdown support.
type WorkerPool struct {
	llm            *simulator.LLMClient
	Workers        int
	ArticleTimeout time.Duration
}

// New returns a WorkerPool.
func New(llm *simulator.LLMClient, workers int, articleTimeout time.Duration) *WorkerPool {
	if workers <= 0 {
		panic("WorkerPool: Workers must be > 0")
	}
	if articleTimeout <= 0 {
		panic("WorkerPool: ArticleTimeout must be > 0")
	}
	return &WorkerPool{llm: llm, Workers: workers, ArticleTimeout: articleTimeout}
}

// ProcessAll processes articles and returns all results plus a ShutdownReport.
//
// If ctx is cancelled mid-run:
//   - in-flight articles are cancelled via the context hierarchy
//     (articleCtx derives from pipeline ctx) and counted as Cancelled
//   - articles still in the jobs queue are drained and reported as Queued
//   - the pipeline returns promptly rather than hanging until all jobs complete
func (p *WorkerPool) ProcessAll(ctx context.Context, articles []model.Article) ([]model.AIResult, ShutdownReport) {
	start := time.Now()

	jobs := make(chan model.Article, len(articles))
	resultsCh := make(chan model.AIResult, len(articles))

	var wg sync.WaitGroup
	for w := 1; w <= p.Workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			p.worker(ctx, workerID, jobs, resultsCh)
		}(w)
	}

	for _, article := range articles {
		jobs <- article
	}
	close(jobs)

	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	var (
		results []model.AIResult
		report  ShutdownReport
	)

	for result := range resultsCh {
		results = append(results, result)
		switch {
		case result.Err == nil:
			report.Succeeded++
		case errors.Is(result.Err, ErrQueued):
			report.Queued++
		case errors.Is(result.Err, context.Canceled):
			// Explicit cancel while in-flight.
			report.Cancelled++
		case errors.Is(result.Err, context.DeadlineExceeded):
			// Parent pipeline deadline/cancel surfaces as DeadlineExceeded on
			// the child articleCtx. A true per-article timeout only happens
			// while the pipeline context is still live → Failed.
			if ctx.Err() != nil {
				report.Cancelled++
			} else {
				report.Failed++
			}
		default:
			report.Failed++
		}
	}

	report.Duration = time.Since(start)
	return results, report
}

// worker processes articles from jobs until the channel closes or ctx is done.
// When ctx is cancelled, it drains remaining jobs from the queue and marks
// them as Queued rather than leaving them hanging.
func (p *WorkerPool) worker(ctx context.Context, id int, jobs <-chan model.Article, results chan<- model.AIResult) {
	for article := range jobs {
		// Pipeline context already cancelled — drain remaining jobs quickly.
		select {
		case <-ctx.Done():
			fmt.Printf("[worker %d] pipeline cancelled — skipping article %d\n", id, article.ID)
			results <- model.AIResult{ArticleID: article.ID, Err: ErrQueued}
			continue
		default:
		}

		fmt.Printf("[worker %d] starting article %d\n", id, article.ID)

		// Per-article timeout derived from pipeline context.
		// If the pipeline ctx is cancelled, articleCtx is cancelled too —
		// in-flight work stops and is reported as Cancelled.
		func() {
			articleCtx, cancel := context.WithTimeout(ctx, p.ArticleTimeout)
			defer cancel()

			result := p.processArticle(articleCtx, article)
			if result.Err != nil {
				fmt.Printf("[worker %d] article %d: %v\n", id, article.ID, result.Err)
			} else {
				fmt.Printf("[worker %d] article %d: done\n", id, article.ID)
			}
			results <- result
		}()
	}
	fmt.Printf("[worker %d] exiting\n", id)
}

func (p *WorkerPool) processArticle(ctx context.Context, article model.Article) model.AIResult {
	result := model.AIResult{ArticleID: article.ID}

	if err := p.llm.Call(ctx, "Summarization", article.ID); err != nil {
		result.Err = err
		return result
	}
	result.Summary = "AI-generated summary"

	if err := p.llm.Call(ctx, "Sentiment Analysis", article.ID); err != nil {
		result.Err = err
		return result
	}
	result.Sentiment = "Positive"

	if err := p.llm.Call(ctx, "Keyword Extraction", article.ID); err != nil {
		result.Err = err
		return result
	}
	result.Keywords = []string{"AI", "Go", "Concurrency"}

	return result
}

// GenerateArticles produces n dummy articles.
func GenerateArticles(n int) []model.Article {
	articles := make([]model.Article, n)
	for i := range articles {
		articles[i] = model.Article{
			ID:      i + 1,
			Title:   fmt.Sprintf("Breaking News %d", i+1),
			Content: "Some article content...",
		}
	}
	return articles
}
