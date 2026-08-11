// Package pipeline implements backpressure via bounded channels for Part 16.
//
// Parts 10-15 focused on per-call resilience.
// Part 16 focuses on system-level flow control: what happens when producers
// generate work faster than consumers can process it.
//
// Without backpressure:
//   Fast scraper → unbounded queue → memory grows → OOM
//
// With backpressure (bounded input channel):
//   Fast scraper → bounded queue → blocks when full → natural throttle
//
// The bounded channel IS the backpressure mechanism.
// When the channel is full, producers block — they slow down automatically.
// No explicit signal needed; the channel's capacity is the contract.
package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-16-backpressure/internal/model"
	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-16-backpressure/internal/simulator"
)

// BackpressurePool uses a bounded jobs channel to apply backpressure.
// QueueDepth controls how many articles can be queued waiting for workers.
// When the queue is full, article production blocks naturally.
type BackpressurePool struct {
	llm        *simulator.LLMClient
	Workers    int
	Timeout    time.Duration
	QueueDepth int // bounded channel capacity
}

// New returns a BackpressurePool.
func New(llm *simulator.LLMClient, workers, queueDepth int, timeout time.Duration) *BackpressurePool {
	if workers <= 0 {
		panic("BackpressurePool: Workers must be > 0")
	}
	if queueDepth <= 0 {
		panic("BackpressurePool: QueueDepth must be > 0")
	}
	return &BackpressurePool{
		llm: llm, Workers: workers,
		Timeout: timeout, QueueDepth: queueDepth,
	}
}

// ProcessWithProducer simulates a producer feeding articles at the given rate.
// The producer blocks when the queue is full — that is the backpressure.
// produceInterval is the delay between producing each article.
func (p *BackpressurePool) ProcessWithProducer(
	ctx context.Context,
	articles []model.Article,
	produceInterval time.Duration,
) ([]model.AIResult, time.Duration) {
	start := time.Now()

	// Bounded queue — this is the backpressure mechanism.
	// Workers: consumers. QueueDepth: buffer between producer and consumers.
	jobs      := make(chan model.Article, p.QueueDepth)
	resultsCh := make(chan model.AIResult, len(articles))

	// Producer goroutine — feeds articles into bounded queue.
	// Blocks when queue is full (backpressure applied to producer).
	go func() {
		defer close(jobs)
		for _, article := range articles {
			select {
			case <-ctx.Done():
				return
			case jobs <- article: // blocks if queue full — that is backpressure
				fmt.Printf("[producer] queued article %d (queue depth: blocking at %d)\n",
					article.ID, p.QueueDepth)
				if produceInterval > 0 {
					time.Sleep(produceInterval)
				}
			}
		}
	}()

	// Consumer workers — drain the bounded queue.
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

func (p *BackpressurePool) processArticle(ctx context.Context, article model.Article) model.AIResult {
	result := model.AIResult{ArticleID: article.ID}
	if err := p.llm.Call(ctx, "Summarisation", article.ID); err != nil {
		result.Err = err
		return result
	}
	result.Summary = "AI-generated summary"
	result.Sentiment = "Positive"
	result.Keywords = []string{"AI", "Go", "Backpressure"}
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
