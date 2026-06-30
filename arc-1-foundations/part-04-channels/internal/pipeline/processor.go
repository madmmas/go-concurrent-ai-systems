// Package pipeline implements the channel-based article processing pipeline
// introduced in Part 4 of the series.
//
// The change from Part 3: the shared slice and mutex are gone. Instead,
// worker goroutines send results into a channel. A single collector goroutine
// owns the results slice — no mutex needed because only one goroutine ever
// touches it.
//
// Architecture:
//
//	Worker goroutines
//	      ↓  (send AIResult)
//	  results channel
//	      ↓  (receive)
//	 Single collector
//	      ↓
//	 results slice
//
// This maps directly to how real AI streaming systems are designed:
// producers emit results as they complete; consumers process them downstream.
package pipeline

import (
	"fmt"
	"sync"
	"time"

	"github.com/moinuddin/go-concurrent-ai-systems/arc-1-foundations/part-04-channels/internal/model"
	"github.com/moinuddin/go-concurrent-ai-systems/arc-1-foundations/part-04-channels/internal/simulator"
)

// ChannelProcessor processes articles concurrently using a results channel.
// No mutex required — single-owner collection eliminates the race entirely.
type ChannelProcessor struct {
	llm *simulator.LLMClient
}

// New returns a ChannelProcessor backed by the given LLM simulator.
func New(llm *simulator.LLMClient) *ChannelProcessor {
	return &ChannelProcessor{llm: llm}
}

// ProcessAll spawns one goroutine per article. Results flow through a channel
// into a single collector. The channel is closed when all workers finish,
// which causes the range loop in the collector to exit cleanly.
func (p *ChannelProcessor) ProcessAll(articles []model.Article) ([]model.AIResult, time.Duration) {
	start := time.Now()

	resultsCh := make(chan model.AIResult)
	var wg sync.WaitGroup

	// Spawn workers.
	for _, article := range articles {
		wg.Add(1)
		go func(a model.Article) {
			defer wg.Done()
			result := p.processArticle(a)
			resultsCh <- result // send into channel; blocks until collector receives
		}(article)
	}

	// Close the channel once all workers have sent their results.
	// This must run in its own goroutine — if it ran inline it would deadlock
	// because wg.Wait() would block while workers try to send into a full channel
	// with no receiver yet.
	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	// Collect results. The range exits when the channel is closed and drained.
	// Only this goroutine (main) touches the slice — no mutex needed.
	results := make([]model.AIResult, 0, len(articles))
	for result := range resultsCh {
		fmt.Printf("Collected result for article %d\n", result.ArticleID)
		results = append(results, result)
	}

	return results, time.Since(start)
}

func (p *ChannelProcessor) processArticle(article model.Article) model.AIResult {
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
