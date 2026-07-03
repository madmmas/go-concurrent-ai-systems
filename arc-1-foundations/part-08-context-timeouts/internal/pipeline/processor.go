// Package pipeline implements the context-aware worker pool introduced in Part 6.
//
// The key changes from Part 5:
//
//  1. Every LLM call now takes a context.Context.
//  2. Each article gets a per-article timeout via context.WithTimeout.
//  3. Results carry an Err field — a timed-out article produces a result
//     with Err set rather than silently disappearing.
//
// This is the minimum correct way to call any external API in Go.
// Without per-call timeouts, one slow provider response can block a worker
// indefinitely, starving the rest of the pipeline.
package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-1-foundations/part-08-context-timeouts/internal/model"
	"github.com/madmmas/go-concurrent-ai-systems/arc-1-foundations/part-08-context-timeouts/internal/simulator"
)

// WorkerPool processes articles using a fixed number of worker goroutines.
// Each article is processed with a per-article timeout.
type WorkerPool struct {
	llm            *simulator.LLMClient
	Workers        int
	ArticleTimeout time.Duration // timeout applied per article
}

// New returns a WorkerPool. ArticleTimeout is the maximum time allowed
// to process one article through all three AI tasks.
func New(llm *simulator.LLMClient, workers int, articleTimeout time.Duration) *WorkerPool {
	if workers <= 0 {
		panic("WorkerPool: Workers must be > 0")
	}
	if articleTimeout <= 0 {
		panic("WorkerPool: ArticleTimeout must be > 0")
	}
	return &WorkerPool{
		llm:            llm,
		Workers:        workers,
		ArticleTimeout: articleTimeout,
	}
}

// ProcessAll feeds articles into the worker pool and collects all results.
// Results with Err != nil represent articles that timed out or failed.
func (p *WorkerPool) ProcessAll(ctx context.Context, articles []model.Article) ([]model.AIResult, time.Duration) {
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

	results := make([]model.AIResult, 0, len(articles))
	for result := range resultsCh {
		results = append(results, result)
	}

	return results, time.Since(start)
}

// worker pulls articles from jobs and processes each with a per-article timeout.
func (p *WorkerPool) worker(ctx context.Context, id int, jobs <-chan model.Article, results chan<- model.AIResult) {
	for article := range jobs {
		// Check if the pipeline-level context is already cancelled before
		// spending time on a new article.
		select {
		case <-ctx.Done():
			results <- model.AIResult{
				ArticleID: article.ID,
				Err:       ctx.Err(),
			}
			continue
		default:
		}

		fmt.Printf("[worker %d] starting article %d\n", id, article.ID)

		// Each article gets its own child context with a per-article deadline.
		// This child context derives from the pipeline context — cancelling
		// the pipeline cancels every in-flight article automatically.
		articleCtx, cancel := context.WithTimeout(ctx, p.ArticleTimeout)
		result := p.processArticle(articleCtx, article)
		cancel() // always call cancel to release resources, even on success

		if result.Err != nil {
			fmt.Printf("[worker %d] article %d failed: %v\n", id, article.ID, result.Err)
		} else {
			fmt.Printf("[worker %d] article %d done\n", id, article.ID)
		}

		results <- result
	}
}

// processArticle runs all three AI tasks for one article.
// If any task's context deadline fires, remaining tasks are skipped
// and the partial result is returned with Err set.
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
