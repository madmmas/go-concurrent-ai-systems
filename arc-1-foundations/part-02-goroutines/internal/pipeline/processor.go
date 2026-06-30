// Package pipeline implements the concurrent article processing pipeline
// introduced in Part 2 of the series.
//
// The key change from Part 1: articles are processed in parallel using
// goroutines. Total time is now bounded by the slowest single article,
// not the sum of all articles.
//
// New problems introduced:
//   - goroutines must be waited on (sync.WaitGroup)
//   - loop variable capture must be handled explicitly
//   - shared result collection is not yet safe (fixed in Part 3)
package pipeline

import (
	"fmt"
	"sync"
	"time"

	"github.com/moinuddin/go-concurrent-ai-systems/arc-1-foundations/part-02-goroutines/internal/model"
	"github.com/moinuddin/go-concurrent-ai-systems/arc-1-foundations/part-02-goroutines/internal/simulator"
)

// ConcurrentProcessor processes articles in parallel using goroutines.
// Each article gets its own goroutine; a WaitGroup ensures we collect
// all results before returning.
//
// NOTE: shared slice append is not mutex-protected here — that is the
// deliberate bug introduced in Part 2 and fixed in Part 3. Run with
// -race to observe the data race.
type ConcurrentProcessor struct {
	llm *simulator.LLMClient
}

// New returns a ConcurrentProcessor backed by the given LLM simulator.
func New(llm *simulator.LLMClient) *ConcurrentProcessor {
	return &ConcurrentProcessor{llm: llm}
}

// ProcessAll spawns one goroutine per article and waits for all to finish.
// Returns collected results and total wall-clock duration.
//
// Compare this against Part 1's sequential version:
//   - Part 1: total time ≈ n × per-article time
//   - Part 2: total time ≈ slowest single article
func (p *ConcurrentProcessor) ProcessAll(articles []model.Article) ([]model.AIResult, time.Duration) {
	start := time.Now()

	var wg sync.WaitGroup
	// WARNING: concurrent append to shared slice — data race.
	// This is intentional. Part 3 fixes it with a mutex.
	results := make([]model.AIResult, 0, len(articles))

	for _, article := range articles {
		wg.Add(1)
		// Pass article as argument to avoid loop variable capture bug.
		// The broken version (captured variable) is shown in broken.go.
		go func(a model.Article) {
			defer wg.Done()
			result := p.processArticle(a)
			// DATA RACE: multiple goroutines append to the same slice.
			results = append(results, result)
		}(article)
	}

	wg.Wait()
	return results, time.Since(start)
}

func (p *ConcurrentProcessor) processArticle(article model.Article) model.AIResult {
	fmt.Printf("Processing article %d...\n", article.ID)

	p.llm.Call("Summarization", article.ID)
	p.llm.Call("Sentiment Analysis", article.ID)
	p.llm.Call("Keyword Extraction", article.ID)

	fmt.Printf("Completed  article %d\n", article.ID)

	return model.AIResult{
		ArticleID: article.ID,
		Summary:   "AI-generated summary",
		Sentiment: "Positive",
		Keywords:  []string{"AI", "Go", "Concurrency"},
	}
}

// GenerateArticles produces a slice of n dummy articles for testing and demos.
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
