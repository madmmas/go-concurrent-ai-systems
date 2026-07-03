// Package pipeline demonstrates buffered vs unbuffered channels
// through the news platform pipeline, introduced in Part 6.
//
// Part 5 (channels) used an unbuffered channel — every send blocks
// until a receiver is ready. This creates tight synchronisation
// between producer and consumer but limits throughput when one
// side is faster than the other.
//
// Part 6 introduces buffered channels, which decouple producers
// from consumers by allowing sends to complete without an immediate
// receiver, up to the buffer capacity.
//
// Two processors are provided for direct comparison:
//   - UnbufferedPipeline: each send waits for the collector
//   - BufferedPipeline: workers can fill the buffer without waiting
package pipeline

import (
	"fmt"
	"sync"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-1-foundations/part-06-buffered-channels/internal/model"
	"github.com/madmmas/go-concurrent-ai-systems/arc-1-foundations/part-06-buffered-channels/internal/simulator"
)

// UnbufferedPipeline processes articles using an unbuffered results channel.
// Each worker blocks on send until the collector receives.
// This creates backpressure naturally: if the collector is slow,
// workers are forced to wait.
type UnbufferedPipeline struct {
	llm *simulator.LLMClient
}

// NewUnbuffered returns an UnbufferedPipeline.
func NewUnbuffered(llm *simulator.LLMClient) *UnbufferedPipeline {
	return &UnbufferedPipeline{llm: llm}
}

// ProcessAll processes articles with an unbuffered channel.
func (p *UnbufferedPipeline) ProcessAll(articles []model.Article) ([]model.AIResult, time.Duration) {
	start := time.Now()

	resultsCh := make(chan model.AIResult) // unbuffered — send blocks until received
	var wg sync.WaitGroup

	for _, article := range articles {
		wg.Add(1)
		go func(a model.Article) {
			defer wg.Done()
			result := p.processArticle(a)
			resultsCh <- result // blocks until collector receives
		}(article)
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

// BufferedPipeline processes articles using a buffered results channel.
// Workers can send up to cap(resultsCh) results without the collector
// keeping pace — they only block when the buffer is full.
//
// In our news pipeline, this matters when:
//   - Articles complete at very different speeds (variable LLM latency)
//   - The collector does non-trivial work (writing to DB, enriching results)
//   - We want workers to keep processing even if the collector falls behind
type BufferedPipeline struct {
	llm        *simulator.LLMClient
	BufferSize int // capacity of the results channel
}

// NewBuffered returns a BufferedPipeline with the given buffer size.
// A buffer of len(articles) means workers never block on send —
// all results can be queued before the collector processes any.
func NewBuffered(llm *simulator.LLMClient, bufferSize int) *BufferedPipeline {
	if bufferSize <= 0 {
		panic("BufferedPipeline: BufferSize must be > 0")
	}
	return &BufferedPipeline{llm: llm, BufferSize: bufferSize}
}

// ProcessAll processes articles with a buffered channel.
func (p *BufferedPipeline) ProcessAll(articles []model.Article) ([]model.AIResult, time.Duration) {
	start := time.Now()

	// Buffer capacity controls how many results can queue up
	// before workers block. cap=1 is almost unbuffered.
	// cap=len(articles) means workers never wait for the collector.
	resultsCh := make(chan model.AIResult, p.BufferSize)
	var wg sync.WaitGroup

	for _, article := range articles {
		wg.Add(1)
		go func(a model.Article) {
			defer wg.Done()
			result := p.processArticle(a)
			resultsCh <- result // only blocks if buffer is full
		}(article)
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

func (p *UnbufferedPipeline) processArticle(article model.Article) model.AIResult {
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

func (p *BufferedPipeline) processArticle(article model.Article) model.AIResult {
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

// AIResultExport is used by cmd/news-processor to avoid importing model directly.
type AIResultExport struct {
	ID int
}
