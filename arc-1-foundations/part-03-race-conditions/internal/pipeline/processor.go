// Package pipeline implements the race-condition-safe concurrent processor
// introduced in Part 3 of the series.
//
// The change from Part 2: a sync.Mutex protects the shared results slice.
// Only one goroutine can append at a time, eliminating the data race.
//
// Key lesson: lock only the critical section — not the entire function.
// Locking around the LLM call would re-serialize the pipeline, defeating
// the purpose of concurrency entirely.
package pipeline

import (
	"fmt"
	"sync"
	"time"

	"github.com/moinuddin/go-concurrent-ai-systems/arc-1-foundations/part-03-race-conditions/internal/model"
	"github.com/moinuddin/go-concurrent-ai-systems/arc-1-foundations/part-03-race-conditions/internal/simulator"
)

// SafeProcessor processes articles concurrently with a mutex protecting
// the shared results slice. The race condition from Part 2 is eliminated.
type SafeProcessor struct {
	llm *simulator.LLMClient
}

// New returns a SafeProcessor backed by the given LLM simulator.
func New(llm *simulator.LLMClient) *SafeProcessor {
	return &SafeProcessor{llm: llm}
}

// ProcessAll spawns one goroutine per article and collects results safely
// using a mutex. Run with -race to confirm no data race is reported.
func (p *SafeProcessor) ProcessAll(articles []model.Article) ([]model.AIResult, time.Duration) {
	start := time.Now()

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		results = make([]model.AIResult, 0, len(articles))
	)

	for _, article := range articles {
		wg.Add(1)
		go func(a model.Article) {
			defer wg.Done()

			// AI work happens OUTSIDE the lock.
			// Holding the lock here would serialize the pipeline — every goroutine
			// would queue up waiting to call the LLM, giving us the worst of both
			// worlds: concurrency complexity with sequential performance.
			result := p.processArticle(a)

			// Only the append — the actual shared-memory write — is protected.
			mu.Lock()
			results = append(results, result)
			mu.Unlock()
		}(article)
	}

	wg.Wait()
	return results, time.Since(start)
}

func (p *SafeProcessor) processArticle(article model.Article) model.AIResult {
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
