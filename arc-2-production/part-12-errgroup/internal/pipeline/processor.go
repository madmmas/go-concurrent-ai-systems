// Package pipeline implements structured concurrency with errgroup for Part 12.
//
// Parts 10-11 managed goroutines manually with WaitGroup + channels.
// Part 12 shows the errgroup pattern — the idiomatic Go way to:
//   - run N concurrent tasks
//   - collect the first error
//   - cancel all remaining work when one fails
//
// The Group type in errgroup.go implements the same API as
// golang.org/x/sync/errgroup. In your production code, use that package.
package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-12-errgroup/internal/model"
	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-12-errgroup/internal/simulator"
)

// ErrgroupPool uses errgroup for per-article fan-out.
type ErrgroupPool struct {
	llm     *simulator.LLMClient
	Workers int
	Timeout time.Duration
}

// New returns an ErrgroupPool.
func New(llm *simulator.LLMClient, workers int, timeout time.Duration) *ErrgroupPool {
	if workers <= 0 {
		panic("ErrgroupPool: Workers must be > 0")
	}
	return &ErrgroupPool{llm: llm, Workers: workers, Timeout: timeout}
}

// ProcessAll feeds articles through the errgroup-based worker pool.
func (p *ErrgroupPool) ProcessAll(ctx context.Context, articles []model.Article) ([]model.AIResult, time.Duration) {
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
					resultsCh <- p.processWithErrgroup(articleCtx, article)
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

// processWithErrgroup runs three AI tasks using our errgroup implementation.
// Key behaviour: first error cancels the group context, unblocking siblings.
func (p *ErrgroupPool) processWithErrgroup(ctx context.Context, article model.Article) model.AIResult {
	fmt.Printf("[article %d] starting errgroup fan-out\n", article.ID)

	result := model.AIResult{ArticleID: article.ID}
	g, gctx := WithContext(ctx)

	var (
		mu        sync.Mutex
		summary   string
		sentiment string
		keywords  string
	)

	g.Go(func() error {
		if err := p.llm.Call(gctx, "Summarisation", article.ID); err != nil {
			return err
		}
		mu.Lock(); summary = "AI-generated summary"; mu.Unlock()
		return nil
	})

	g.Go(func() error {
		if err := p.llm.Call(gctx, "Sentiment Analysis", article.ID); err != nil {
			return err
		}
		mu.Lock(); sentiment = "Positive"; mu.Unlock()
		return nil
	})

	g.Go(func() error {
		if err := p.llm.Call(gctx, "Keyword Extraction", article.ID); err != nil {
			return err
		}
		mu.Lock(); keywords = "AI,Go,Concurrency"; mu.Unlock()
		return nil
	})

	if err := g.Wait(); err != nil {
		result.Err = err
		fmt.Printf("[article %d] errgroup: task failed: %v\n", article.ID, err)
		return result
	}

	result.Summary   = summary
	result.Sentiment = sentiment
	result.Keywords  = []string{keywords}
	fmt.Printf("[article %d] errgroup: all tasks complete\n", article.ID)
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
