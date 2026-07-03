// Package pipeline demonstrates deadlock-safe pipeline design for Part 4.
//
// This builds directly on Part 3's mutex-fixed processor and adds
// two concrete deadlock prevention rules that every pipeline must follow:
//
//  1. Always close channels after the last send.
//  2. Never hold a mutex while waiting to send or receive on a channel.
//
// These rules are encoded as patterns in the implementation and
// enforced by the tests.
package pipeline

import (
	"fmt"
	"sync"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-1-foundations/part-04-deadlocks/internal/model"
	"github.com/madmmas/go-concurrent-ai-systems/arc-1-foundations/part-04-deadlocks/internal/simulator"
)

// SafePipeline processes articles concurrently with explicit deadlock prevention.
// It uses the same mutex-protected results slice from Part 3, with two
// additional invariants that prevent deadlock.
type SafePipeline struct {
	llm *simulator.LLMClient
}

// New returns a SafePipeline.
func New(llm *simulator.LLMClient) *SafePipeline {
	return &SafePipeline{llm: llm}
}

// ProcessAll processes all articles concurrently.
//
// Deadlock prevention rules applied here:
//  1. The done channel is closed via defer — guaranteed even on panic.
//  2. The mutex never wraps the LLM call — only the slice append.
//     (Holding a lock while calling a blocking operation is a common
//     deadlock source in more complex systems.)
func (p *SafePipeline) ProcessAll(articles []model.Article) ([]model.AIResult, time.Duration) {
	start := time.Now()

	var (
		mu      sync.Mutex
		wg      sync.WaitGroup
		results = make([]model.AIResult, 0, len(articles))
	)

	for _, article := range articles {
		wg.Add(1)
		go func(a model.Article) {
			defer wg.Done()

			result := p.processArticle(a)

			// Rule: lock ONLY around the shared-memory write.
			// Never hold the lock across a blocking call.
			mu.Lock()
			results = append(results, result)
			mu.Unlock()
		}(article)
	}

	wg.Wait()
	return results, time.Since(start)
}

// ProcessAllWithChannel shows the channel-based alternative.
// The key deadlock prevention rule here: close(resultsCh) is in a
// separate goroutine that waits for all workers. If close() were
// called before wg.Wait(), workers would panic on send-to-closed-channel.
// If close() were never called, the range loop would block forever.
func (p *SafePipeline) ProcessAllWithChannel(articles []model.Article) ([]model.AIResult, time.Duration) {
	start := time.Now()

	resultsCh := make(chan model.AIResult, len(articles)) // buffered: no blocking on send
	var wg sync.WaitGroup

	for _, article := range articles {
		wg.Add(1)
		go func(a model.Article) {
			defer wg.Done()
			resultsCh <- p.processArticle(a)
		}(article)
	}

	// Rule: close AFTER all senders are done — in a separate goroutine
	// so the collector loop can start immediately.
	go func() {
		wg.Wait()
		close(resultsCh) // signals the range below to exit when drained
	}()

	var results []model.AIResult
	for r := range resultsCh {
		results = append(results, r)
	}

	return results, time.Since(start)
}

func (p *SafePipeline) processArticle(article model.Article) model.AIResult {
	fmt.Printf("Processing article %d...\n", article.ID)
	p.llm.Call("Summarization", article.ID)
	p.llm.Call("Sentiment Analysis", article.ID)
	p.llm.Call("Keyword Extraction", article.ID)
	return model.AIResult{
		ArticleID: article.ID,
		Summary:   "AI-generated summary",
		Sentiment: "Positive",
		Keywords:  []string{"AI", "Go", "Concurrency"},
	}
}

// GenerateArticles produces n dummy articles.
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
