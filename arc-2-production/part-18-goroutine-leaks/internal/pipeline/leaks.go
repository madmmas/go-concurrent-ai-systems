// Package pipeline demonstrates goroutine leaks and how to detect them.
//
// A goroutine leak is a goroutine that starts and never exits.
// Common causes in AI pipelines:
//   1. Blocked channel send with no receiver (channel is never closed/read)
//   2. Blocked channel receive (channel is never closed or written to)
//   3. Abandoned streaming goroutine (consumer exits, producer keeps running)
//   4. Context never cancelled, goroutine waiting on ctx.Done forever
//
// Detection tools:
//   runtime.NumGoroutine() — count before and after; delta = leaked goroutines
//   go tool pprof http://localhost:6060/debug/pprof/goroutine — stack traces
//
// This package shows both broken (leaking) and fixed implementations.
package pipeline

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-18-goroutine-leaks/internal/model"
	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-18-goroutine-leaks/internal/simulator"
)

// CountGoroutines returns the current goroutine count.
// Use before and after an operation to detect leaks.
func CountGoroutines() int {
	return runtime.NumGoroutine()
}

// LeakyPool demonstrates a goroutine leak via an abandoned channel.
// This is for educational purposes — do not use in production.
type LeakyPool struct {
	llm     *simulator.LLMClient
	Workers int
}

// NewLeaky returns a LeakyPool for demonstration.
func NewLeaky(llm *simulator.LLMClient, workers int) *LeakyPool {
	return &LeakyPool{llm: llm, Workers: workers}
}

// ProcessAllLeaky has a goroutine leak: workers send on an unbuffered channel.
// If we return early (context cancel) without draining, senders block forever.
// BUG: intentional — demonstrates the leak pattern.
func (p *LeakyPool) ProcessAllLeaky(ctx context.Context, articles []model.Article) []model.AIResult {
	resultsCh := make(chan model.AIResult) // unbuffered — no capacity
	var wg sync.WaitGroup

	for _, a := range articles {
		wg.Add(1)
		go func(art model.Article) {
			defer wg.Done()
			time.Sleep(50 * time.Millisecond) // simulate work
			// BUG: if resultsCh is never drained (consumer gone), this BLOCKS FOREVER
			resultsCh <- model.AIResult{ArticleID: art.ID, Summary: "summary"}
		}(a)
	}

	// BUG: this goroutine is leaked if the caller returns early without waiting
	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	var results []model.AIResult
	for {
		select {
		case <-ctx.Done():
			// Return early without draining — remaining senders LEAK.
			fmt.Printf("leaky: cancelled after %d results — abandoning channel\n", len(results))
			return results
		case r, ok := <-resultsCh:
			if !ok {
				return results
			}
			results = append(results, r)
		}
	}
}

// FixedPool demonstrates the correct pattern that prevents goroutine leaks.
type FixedPool struct {
	llm     *simulator.LLMClient
	Workers int
	Timeout time.Duration
}

// New returns a FixedPool.
func New(llm *simulator.LLMClient, workers int, timeout time.Duration) *FixedPool {
	if workers <= 0 {
		panic("FixedPool: Workers must be > 0")
	}
	return &FixedPool{llm: llm, Workers: workers, Timeout: timeout}
}

// ProcessAll uses a BUFFERED channel and context to prevent leaks.
// Key fixes vs LeakyPool:
//   1. Buffered resultsCh — workers never block on send
//   2. Context propagation — goroutines exit when ctx cancelled
//   3. Proper cleanup ordering — closer goroutine runs after workers done
func (p *FixedPool) ProcessAll(ctx context.Context, articles []model.Article) ([]model.AIResult, time.Duration) {
	start := time.Now()

	// Buffered channel — workers send without blocking even if collector is slow
	jobs      := make(chan model.Article, len(articles))
	resultsCh := make(chan model.AIResult, len(articles))

	var wg sync.WaitGroup
	for w := 0; w < p.Workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for article := range jobs {
				select {
				case <-ctx.Done():
					// Context cancelled — exit cleanly, don't send to resultsCh
					// (it's buffered, so we could, but we exit to avoid doing more work)
					resultsCh <- model.AIResult{ArticleID: article.ID, Err: ctx.Err()}
					continue
				default:
				}
				articleCtx, cancel := context.WithTimeout(ctx, p.Timeout)
				r := p.processArticle(articleCtx, article)
				cancel()
				resultsCh <- r
			}
		}()
	}

	for _, a := range articles {
		jobs <- a
	}
	close(jobs)

	go func() {
		wg.Wait()
		close(resultsCh) // always executes — no leak
	}()

	var results []model.AIResult
	for r := range resultsCh {
		results = append(results, r)
	}
	return results, time.Since(start)
}

func (p *FixedPool) processArticle(ctx context.Context, article model.Article) model.AIResult {
	result := model.AIResult{ArticleID: article.ID}
	if err := p.llm.Call(ctx, "Summarisation", article.ID); err != nil {
		result.Err = err
		return result
	}
	result.Summary = "AI-generated summary"
	result.Sentiment = "Positive"
	result.Keywords = []string{"AI", "Go", "NoLeak"}
	fmt.Printf("[article %d] done\n", article.ID)
	return result
}

// GenerateArticles produces n dummy articles.
func GenerateArticles(n int) []model.Article {
	articles := make([]model.Article, n)
	for i := range articles {
		articles[i] = model.Article{ID: i + 1, Title: fmt.Sprintf("Breaking News %d", i+1)}
	}
	return articles
}
