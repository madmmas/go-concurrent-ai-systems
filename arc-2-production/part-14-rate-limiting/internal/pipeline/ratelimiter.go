// Package pipeline implements token-bucket rate limiting for Part 14.
//
// Part 13 added retries when the provider returns 429.
// Part 14 prevents 429s from happening in the first place by limiting
// how many calls we make per second — a token bucket rate limiter.
//
// Architecture:
//   TokenBucket — refills tokens at a fixed rate, callers acquire before calling.
//   RateLimitedPool — wraps the worker pool, enforcing the rate limit.
//
// This is the same pattern used by every HTTP client library's rate limiter,
// AWS SDK's adaptive retry, and OpenAI's official Go SDK.
package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-14-rate-limiting/internal/model"
	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-14-rate-limiting/internal/simulator"
)

// TokenBucket implements a token-bucket rate limiter.
// Tokens refill at Rate per second up to a maximum of Burst.
type TokenBucket struct {
	mu       sync.Mutex
	tokens   float64
	maxTokens float64
	rate     float64 // tokens per second
	lastFill time.Time
}

// NewTokenBucket creates a TokenBucket with the given rate and burst.
func NewTokenBucket(ratePerSec float64, burst int) *TokenBucket {
	return &TokenBucket{
		tokens:    float64(burst),
		maxTokens: float64(burst),
		rate:      ratePerSec,
		lastFill:  time.Now(),
	}
}

// Acquire blocks until a token is available or ctx is cancelled.
func (tb *TokenBucket) Acquire(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		tb.mu.Lock()
		now := time.Now()
		elapsed := now.Sub(tb.lastFill).Seconds()
		tb.tokens = min64(tb.maxTokens, tb.tokens + elapsed*tb.rate)
		tb.lastFill = now

		if tb.tokens >= 1.0 {
			tb.tokens--
			tb.mu.Unlock()
			return nil
		}

		// Calculate how long until next token arrives
		waitSec := (1.0 - tb.tokens) / tb.rate
		tb.mu.Unlock()

		wait := time.Duration(waitSec * float64(time.Second))
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func min64(a, b float64) float64 {
	if a < b { return a }
	return b
}

// RateLimitedPool enforces a per-second call rate on LLM API calls.
type RateLimitedPool struct {
	llm     *simulator.LLMClient
	Workers int
	Timeout time.Duration
	bucket  *TokenBucket
}

// New returns a RateLimitedPool.
// ratePerSec: max LLM calls per second
// burst: max simultaneous calls before rate limiting kicks in
func New(llm *simulator.LLMClient, workers int, timeout time.Duration, ratePerSec float64, burst int) *RateLimitedPool {
	if workers <= 0 {
		panic("RateLimitedPool: Workers must be > 0")
	}
	return &RateLimitedPool{
		llm: llm, Workers: workers, Timeout: timeout,
		bucket: NewTokenBucket(ratePerSec, burst),
	}
}

// ProcessAll processes articles, acquiring rate limit tokens before each LLM call.
func (p *RateLimitedPool) ProcessAll(ctx context.Context, articles []model.Article) ([]model.AIResult, time.Duration) {
	start := time.Now()

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
					resultsCh <- model.AIResult{ArticleID: article.ID, Err: ctx.Err()}
				default:
					articleCtx, cancel := context.WithTimeout(ctx, p.Timeout)
					resultsCh <- p.processArticle(articleCtx, article)
					cancel()
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

	var results []model.AIResult
	for r := range resultsCh {
		results = append(results, r)
	}
	return results, time.Since(start)
}

func (p *RateLimitedPool) processArticle(ctx context.Context, article model.Article) model.AIResult {
	result := model.AIResult{ArticleID: article.ID}

	// Acquire token BEFORE making the LLM call — this is the rate limit enforcement point
	if err := p.bucket.Acquire(ctx); err != nil {
		result.Err = err
		return result
	}
	if err := p.llm.Call(ctx, "Summarisation", article.ID); err != nil {
		result.Err = err
		return result
	}
	result.Summary = "AI-generated summary"

	if err := p.bucket.Acquire(ctx); err != nil {
		result.Err = err
		return result
	}
	if err := p.llm.Call(ctx, "Sentiment Analysis", article.ID); err != nil {
		result.Err = err
		return result
	}
	result.Sentiment = "Positive"

	result.Keywords = []string{"AI", "Go", "RateLimit"}
	fmt.Printf("[article %d] processed\n", article.ID)
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
