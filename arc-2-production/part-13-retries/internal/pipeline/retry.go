// Package pipeline implements retry with exponential backoff for Part 13.
//
// Part 12 showed that tasks can fail. Part 13 shows what to do about it.
//
// Retry strategy:
//   - Exponential backoff: wait doubles each attempt (1s, 2s, 4s...)
//   - Jitter: randomise wait to avoid thundering herd
//   - Max attempts: give up after N tries
//   - Dead letter: articles that exhaust retries go to a dead letter channel
//
// Only retryable errors are retried (rate limits, server errors).
// Context cancellation and timeouts are not retried.
package pipeline

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-13-retries/internal/model"
	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-13-retries/internal/simulator"
)

// RetryConfig controls the retry behaviour.
type RetryConfig struct {
	MaxAttempts int
	BaseDelay   time.Duration  // first retry wait
	MaxDelay    time.Duration  // cap on exponential growth
	JitterFrac  float64        // jitter as fraction of delay (0.0-1.0)
}

var DefaultRetryConfig = RetryConfig{
	MaxAttempts: 3,
	BaseDelay:   100 * time.Millisecond,
	MaxDelay:    2 * time.Second,
	JitterFrac:  0.2,
}

// RetryPool processes articles with retry logic on retryable errors.
type RetryPool struct {
	llm      *simulator.LLMClient
	Workers  int
	Timeout  time.Duration
	Retry    RetryConfig
	rng      *rand.Rand
	rngMu    sync.Mutex
}

// New returns a RetryPool.
func New(llm *simulator.LLMClient, workers int, timeout time.Duration, retry RetryConfig) *RetryPool {
	if workers <= 0 {
		panic("RetryPool: Workers must be > 0")
	}
	return &RetryPool{
		llm: llm, Workers: workers, Timeout: timeout, Retry: retry,
		rng: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// ProcessResult includes the article result plus a dead-letter channel.
type ProcessResult struct {
	Result     model.AIResult
	DeadLetter bool // true if exhausted all retries
}

// ProcessAll processes articles with retry. Returns results and dead-letter items.
func (p *RetryPool) ProcessAll(ctx context.Context, articles []model.Article) ([]ProcessResult, time.Duration) {
	start := time.Now()

	jobs      := make(chan model.Article, len(articles))
	resultsCh := make(chan ProcessResult, len(articles))

	var wg sync.WaitGroup
	for w := 0; w < p.Workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for article := range jobs {
				select {
				case <-ctx.Done():
					resultsCh <- ProcessResult{
						Result:     model.AIResult{ArticleID: article.ID, Err: ctx.Err()},
						DeadLetter: false,
					}
				default:
					resultsCh <- p.processWithRetry(ctx, article)
				}
			}
		}()
	}

	for _, a := range articles {
		jobs <- a
	}
	close(jobs)

	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	var results []ProcessResult
	for r := range resultsCh {
		results = append(results, r)
	}
	return results, time.Since(start)
}

// processWithRetry attempts the article, retrying on retryable errors.
func (p *RetryPool) processWithRetry(ctx context.Context, article model.Article) ProcessResult {
	var lastErr error
	for attempt := 1; attempt <= p.Retry.MaxAttempts; attempt++ {
		if ctx.Err() != nil {
			return ProcessResult{
				Result: model.AIResult{ArticleID: article.ID, Err: ctx.Err()},
			}
		}

		articleCtx, cancel := context.WithTimeout(ctx, p.Timeout)
		result := p.processArticle(articleCtx, article)
		cancel()

		if result.Err == nil {
			result.Retries = attempt - 1
			return ProcessResult{Result: result}
		}

		lastErr = result.Err

		// Only retry on retryable errors
		if !simulator.IsRetryable(result.Err) {
			fmt.Printf("[article %d] non-retryable error: %v\n", article.ID, result.Err)
			return ProcessResult{Result: result, DeadLetter: true}
		}

		if attempt < p.Retry.MaxAttempts {
			delay := p.backoffDelay(attempt)
			fmt.Printf("[article %d] attempt %d failed (%v), retrying in %v\n",
				article.ID, attempt, result.Err, delay.Round(time.Millisecond))

			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return ProcessResult{Result: model.AIResult{ArticleID: article.ID, Err: ctx.Err()}}
			}
		}
	}

	fmt.Printf("[article %d] exhausted %d attempts — dead letter: %v\n",
		article.ID, p.Retry.MaxAttempts, lastErr)
	return ProcessResult{
		Result:     model.AIResult{ArticleID: article.ID, Err: lastErr, Retries: p.Retry.MaxAttempts - 1},
		DeadLetter: true,
	}
}

// backoffDelay computes the wait before attempt n+1 with jitter.
func (p *RetryPool) backoffDelay(attempt int) time.Duration {
	delay := p.Retry.BaseDelay * (1 << uint(attempt-1)) // 2^(attempt-1) × base
	if delay > p.Retry.MaxDelay {
		delay = p.Retry.MaxDelay
	}
	p.rngMu.Lock()
	jitter := time.Duration(p.rng.Float64() * p.Retry.JitterFrac * float64(delay))
	p.rngMu.Unlock()
	return delay + jitter
}

func (p *RetryPool) processArticle(ctx context.Context, article model.Article) model.AIResult {
	result := model.AIResult{ArticleID: article.ID}
	tasks := []struct {
		name string
		dest *string
		val  string
	}{
		{"Summarisation", &result.Summary, "AI-generated summary"},
		{"Sentiment Analysis", &result.Sentiment, "Positive"},
	}
	for _, t := range tasks {
		if err := p.llm.Call(ctx, t.name, article.ID); err != nil {
			result.Err = err
			return result
		}
		*t.dest = t.val
	}
	result.Keywords = []string{"AI", "Go", "Retry"}
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
