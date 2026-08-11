// Package pipeline implements token streaming for Part 17.
//
// Modern LLM APIs stream tokens incrementally — you don't wait for the
// full response, you process tokens as they arrive.
//
// Architecture:
//   LLM call → token channel → consumer (renderer, aggregator, etc.)
//
// This maps directly to Server-Sent Events (SSE) and WebSocket streaming
// in production AI systems.
package pipeline

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-17-token-streaming/internal/model"
	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-17-token-streaming/internal/simulator"
)

// Token represents a single token streamed from the LLM.
type Token struct {
	ArticleID int
	Text      string
	Done      bool  // true on the final token
	Err       error
}

// StreamingResult holds a completed streaming session.
type StreamingResult struct {
	ArticleID  int
	FullText   string
	TokenCount int
	Duration   time.Duration
	Err        error
}

// StreamingPool processes articles with token-level streaming per article.
type StreamingPool struct {
	llm     *simulator.LLMClient
	Workers int
	Timeout time.Duration
	rng     *rand.Rand
	rngMu   sync.Mutex
}

// New returns a StreamingPool.
func New(llm *simulator.LLMClient, workers int, timeout time.Duration) *StreamingPool {
	if workers <= 0 {
		panic("StreamingPool: Workers must be > 0")
	}
	return &StreamingPool{
		llm: llm, Workers: workers, Timeout: timeout,
		rng: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// ProcessAll processes articles, streaming tokens per article.
func (p *StreamingPool) ProcessAll(ctx context.Context, articles []model.Article) ([]StreamingResult, time.Duration) {
	start := time.Now()

	jobs      := make(chan model.Article, len(articles))
	resultsCh := make(chan StreamingResult, len(articles))

	var wg sync.WaitGroup
	for w := 0; w < p.Workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for article := range jobs {
				select {
				case <-ctx.Done():
					resultsCh <- StreamingResult{ArticleID: article.ID, Err: ctx.Err()}
				default:
					articleCtx, cancel := context.WithTimeout(ctx, p.Timeout)
					resultsCh <- p.streamArticle(articleCtx, article)
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

	var results []StreamingResult
	for r := range resultsCh {
		results = append(results, r)
	}
	return results, time.Since(start)
}

// streamArticle simulates streaming a summary token by token.
// In production, this is an HTTP SSE response or WebSocket stream.
func (p *StreamingPool) streamArticle(ctx context.Context, article model.Article) StreamingResult {
	start := time.Now()

	// Token channel — buffer a few tokens so the streamer doesn't block on each one
	tokenCh := make(chan Token, 10)

	// Streamer goroutine — produces tokens into the channel
	go func() {
		defer close(tokenCh)
		tokens := p.generateTokens(article.ID)
		for i, tok := range tokens {
			select {
			case <-ctx.Done():
				tokenCh <- Token{ArticleID: article.ID, Err: ctx.Err(), Done: true}
				return
			default:
			}

			// Simulate inter-token latency (much faster than full call)
			p.rngMu.Lock()
			delay := time.Duration(p.rng.Intn(50)+10) * time.Millisecond
			p.rngMu.Unlock()

			time.Sleep(delay)

			isLast := i == len(tokens)-1
			tokenCh <- Token{
				ArticleID: article.ID,
				Text:      tok,
				Done:      isLast,
			}
			fmt.Printf("[article %d] token: %q\n", article.ID, tok)
		}
	}()

	// Consumer — aggregates streamed tokens into full text
	result := StreamingResult{ArticleID: article.ID}
	for tok := range tokenCh {
		if tok.Err != nil {
			result.Err = tok.Err
			break
		}
		result.FullText += tok.Text + " "
		result.TokenCount++
	}

	result.Duration = time.Since(start)
	fmt.Printf("[article %d] streaming complete: %d tokens in %v\n",
		article.ID, result.TokenCount, result.Duration.Round(time.Millisecond))
	return result
}

// generateTokens returns a simulated list of summary tokens for an article.
func (p *StreamingPool) generateTokens(id int) []string {
	return []string{
		"Breaking", "news", "article", fmt.Sprintf("#%d", id), "discusses",
		"the", "latest", "developments", "in", "AI", "and", "Go", "concurrency.",
	}
}

// GenerateArticles produces n dummy articles.
func GenerateArticles(n int) []model.Article {
	articles := make([]model.Article, n)
	for i := range articles {
		articles[i] = model.Article{ID: i + 1, Title: fmt.Sprintf("Breaking News %d", i+1)}
	}
	return articles
}
