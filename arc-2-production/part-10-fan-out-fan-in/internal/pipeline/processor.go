// Package pipeline implements fan-out/fan-in for Part 10.
//
// Arc 1's worker pool ran three AI tasks sequentially inside each worker:
//
//	Summarization → Sentiment → Keywords  (serial, ~3s per article)
//
// Part 10 fans out: all three tasks launch concurrently per article,
// then fan in to collect all three results before returning.
//
// Architecture per article:
//
//	         ┌─ Summarization goroutine ─┐
//	Article ─┤─ Sentiment goroutine    ─┼─→ AIResult
//	         └─ Keyword goroutine      ─┘
//
// Total per-article time ≈ slowest single task, not the sum.
package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-10-fan-out-fan-in/internal/model"
	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-10-fan-out-fan-in/internal/simulator"
)

// FanOutPool processes articles with a worker pool where each article's
// AI tasks run concurrently (fan-out), results collected (fan-in).
type FanOutPool struct {
	llm     *simulator.LLMClient
	Workers int
	Timeout time.Duration // per-article timeout
}

// New returns a FanOutPool.
func New(llm *simulator.LLMClient, workers int, timeout time.Duration) *FanOutPool {
	if workers <= 0 {
		panic("FanOutPool: Workers must be > 0")
	}
	return &FanOutPool{llm: llm, Workers: workers, Timeout: timeout}
}

// ProcessAll feeds articles through the fan-out worker pool.
func (p *FanOutPool) ProcessAll(ctx context.Context, articles []model.Article) ([]model.AIResult, time.Duration) {
	start := time.Now()

	jobs      := make(chan model.Article, len(articles))
	resultsCh := make(chan model.AIResult, len(articles))

	var wg sync.WaitGroup
	for w := 1; w <= p.Workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for article := range jobs {
				select {
				case <-ctx.Done():
					resultsCh <- model.AIResult{ArticleID: article.ID, Err: ctx.Err()}
				default:
					articleCtx, cancel := context.WithTimeout(ctx, p.Timeout)
					resultsCh <- p.processArticleFanOut(articleCtx, article)
					cancel()
				}
			}
		}(w)
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

// taskResult carries the output of one AI task.
type taskResult struct {
	name  string
	value string
	err   error
}

// processArticleFanOut runs all three AI tasks concurrently for one article.
// This is the fan-out: three goroutines launch simultaneously.
// The fan-in is the WaitGroup + channel collect below.
func (p *FanOutPool) processArticleFanOut(ctx context.Context, article model.Article) model.AIResult {
	fmt.Printf("[article %d] fanning out 3 tasks\n", article.ID)

	taskCh := make(chan taskResult, 3)
	var wg sync.WaitGroup

	// Fan-out: launch all three tasks concurrently
	tasks := []struct {
		name string
		fn   func(context.Context, int) (string, error)
	}{
		{"summarise",  p.summarise},
		{"sentiment",  p.sentiment},
		{"keywords",   p.keywords},
	}

	for _, t := range tasks {
		wg.Add(1)
		go func(name string, fn func(context.Context, int) (string, error)) {
			defer wg.Done()
			val, err := fn(ctx, article.ID)
			taskCh <- taskResult{name: name, value: val, err: err}
		}(t.name, t.fn)
	}

	// Close taskCh after all tasks complete
	go func() {
		wg.Wait()
		close(taskCh)
	}()

	// Fan-in: collect all three results
	result := model.AIResult{ArticleID: article.ID}
	for tr := range taskCh {
		if tr.err != nil {
			result.Err = tr.err
			continue
		}
		switch tr.name {
		case "summarise":
			result.Summary = tr.value
		case "sentiment":
			result.Sentiment = tr.value
		case "keywords":
			result.Keywords = []string{tr.value}
		}
	}

	if result.Err != nil {
		fmt.Printf("[article %d] fan-in complete — failed: %v\n", article.ID, result.Err)
	} else {
		fmt.Printf("[article %d] fan-in complete — all tasks done\n", article.ID)
	}
	return result
}

func (p *FanOutPool) summarise(ctx context.Context, id int) (string, error) {
	if err := p.llm.Call(ctx, "Summarisation", id); err != nil {
		return "", err
	}
	return "AI-generated summary", nil
}

func (p *FanOutPool) sentiment(ctx context.Context, id int) (string, error) {
	if err := p.llm.Call(ctx, "Sentiment Analysis", id); err != nil {
		return "", err
	}
	return "Positive", nil
}

func (p *FanOutPool) keywords(ctx context.Context, id int) (string, error) {
	if err := p.llm.Call(ctx, "Keyword Extraction", id); err != nil {
		return "", err
	}
	return "AI,Go,Concurrency", nil
}

// GenerateArticles produces n dummy articles.
func GenerateArticles(n int) []model.Article {
	articles := make([]model.Article, n)
	for i := range articles {
		articles[i] = model.Article{
			ID:     i + 1,
			Title:  fmt.Sprintf("Breaking News %d", i+1),
			Source: "NewsWire",
		}
	}
	return articles
}
