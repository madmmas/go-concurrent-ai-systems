// Package pipeline implements a multi-stage concurrent pipeline for Part 11.
//
// Part 10 showed fan-out within a single worker.
// Part 11 shows fan-out across pipeline stages — each stage runs concurrently
// with the others, connected by channels.
//
// Architecture:
//
//	Articles
//	   ↓
//	[Scrape Stage]    — fetches content (IO-bound, high concurrency)
//	   ↓
//	[Clean Stage]     — normalises text (CPU-bound, lower concurrency)
//	   ↓
//	[Embed Stage]     — generates embeddings (LLM call, rate-limited)
//	   ↓
//	[Summarise Stage] — generates summary (LLM call, rate-limited)
//	   ↓
//	Results
//
// Each stage has its own worker count, tuned to its bottleneck:
//   - Scrape: many workers (IO-bound, cheap to run many)
//   - Clean:  fewer workers (CPU-bound)
//   - Embed:  few workers (LLM rate limit)
//   - Summarise: few workers (LLM rate limit)
package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-11-pipeline-stages/internal/model"
	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-11-pipeline-stages/internal/simulator"
)

// StageConfig controls the concurrency for one pipeline stage.
type StageConfig struct {
	Name    string
	Workers int
}

// Pipeline is a multi-stage concurrent processing pipeline.
type Pipeline struct {
	llm     *simulator.LLMClient
	timeout time.Duration
	stages  []StageConfig
}

// New returns a Pipeline with the given per-stage worker counts.
func New(llm *simulator.LLMClient, timeout time.Duration, stages []StageConfig) *Pipeline {
	return &Pipeline{llm: llm, timeout: timeout, stages: stages}
}

// DefaultStages returns sensible defaults matching real-world AI pipeline ratios:
// more scraping workers than LLM workers because scraping is cheaper.
func DefaultStages() []StageConfig {
	return []StageConfig{
		{Name: "scrape",    Workers: 10},
		{Name: "clean",     Workers: 5},
		{Name: "embed",     Workers: 3},
		{Name: "summarise", Workers: 3},
	}
}

// ProcessAll runs all articles through the pipeline stages in order.
// Each stage processes results from the previous stage concurrently.
func (p *Pipeline) ProcessAll(ctx context.Context, articles []model.Article) ([]model.AIResult, time.Duration) {
	start := time.Now()

	// Seed the first channel with all articles
	scrapeIn := make(chan model.Article, len(articles))
	for _, a := range articles {
		scrapeIn <- a
	}
	close(scrapeIn)

	// Chain stages — each stage reads from previous output
	scrapeOut  := p.runStage(ctx, "scrape",    scrapeIn,  p.scrape)
	cleanOut   := p.runStageResult(ctx, "clean",     scrapeOut,  p.clean)
	embedOut   := p.runStageResult(ctx, "embed",     cleanOut,   p.embed)
	summaryOut := p.runStageResult(ctx, "summarise", embedOut,   p.summarise)

	// Collect final results
	var results []model.AIResult
	for r := range summaryOut {
		results = append(results, r)
	}

	return results, time.Since(start)
}

// runStage runs the first stage which takes Articles as input.
func (p *Pipeline) runStage(
	ctx context.Context,
	name string,
	in <-chan model.Article,
	fn func(context.Context, model.Article) (model.AIResult, error),
) <-chan model.AIResult {

	out := make(chan model.AIResult, cap(in)+1)
	cfg := p.stageConfig(name)

	var wg sync.WaitGroup
	for w := 0; w < cfg.Workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for article := range in {
				articleCtx, cancel := context.WithTimeout(ctx, p.timeout)
				result, err := fn(articleCtx, article)
				cancel()
				if err != nil {
					result.Err = err
					result.ArticleID = article.ID
				}
				out <- result
			}
		}()
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

// runStageResult runs middle/final stages that take AIResult as input.
func (p *Pipeline) runStageResult(
	ctx context.Context,
	name string,
	in <-chan model.AIResult,
	fn func(context.Context, model.AIResult) (model.AIResult, error),
) <-chan model.AIResult {

	out := make(chan model.AIResult, cap(in)+1)
	cfg := p.stageConfig(name)

	var wg sync.WaitGroup
	for w := 0; w < cfg.Workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for result := range in {
				// Skip failed results — propagate error downstream
				if result.Err != nil {
					out <- result
					continue
				}
				articleCtx, cancel := context.WithTimeout(ctx, p.timeout)
				updated, err := fn(articleCtx, result)
				cancel()
				if err != nil {
					result.Err = err
					out <- result
				} else {
					out <- updated
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

func (p *Pipeline) stageConfig(name string) StageConfig {
	for _, s := range p.stages {
		if s.Name == name {
			return s
		}
	}
	return StageConfig{Name: name, Workers: 1}
}

// Stage implementations

func (p *Pipeline) scrape(ctx context.Context, a model.Article) (model.AIResult, error) {
	fmt.Printf("[scrape] article %d\n", a.ID)
	if err := p.llm.Call(ctx, "Scrape", a.ID); err != nil {
		return model.AIResult{}, err
	}
	return model.AIResult{ArticleID: a.ID}, nil
}

func (p *Pipeline) clean(ctx context.Context, r model.AIResult) (model.AIResult, error) {
	fmt.Printf("[clean] article %d\n", r.ArticleID)
	// Clean is CPU-bound in production — simulate with short sleep
	if err := ctx.Err(); err != nil {
		return r, err
	}
	time.Sleep(10 * time.Millisecond)
	return r, nil
}

func (p *Pipeline) embed(ctx context.Context, r model.AIResult) (model.AIResult, error) {
	fmt.Printf("[embed] article %d\n", r.ArticleID)
	if err := p.llm.Call(ctx, "Embed", r.ArticleID); err != nil {
		return r, err
	}
	r.Embedding = []float32{0.1, 0.2, 0.3} // simulated embedding
	return r, nil
}

func (p *Pipeline) summarise(ctx context.Context, r model.AIResult) (model.AIResult, error) {
	fmt.Printf("[summarise] article %d\n", r.ArticleID)
	if err := p.llm.Call(ctx, "Summarise", r.ArticleID); err != nil {
		return r, err
	}
	r.Summary = "AI-generated summary"
	r.Sentiment = "Positive"
	r.Keywords = []string{"AI", "Go", "Pipeline"}
	return r, nil
}

// GenerateArticles produces n dummy articles.
func GenerateArticles(n int) []model.Article {
	articles := make([]model.Article, n)
	for i := range articles {
		articles[i] = model.Article{
			ID:     i + 1,
			Title:  fmt.Sprintf("Breaking News %d", i+1),
			URL:    fmt.Sprintf("https://news.example.com/article/%d", i+1),
			Source: "NewsWire",
		}
	}
	return articles
}
