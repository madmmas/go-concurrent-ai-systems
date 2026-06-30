// Package pipeline implements the bounded worker pool introduced in Part 5.
//
// The problem with Part 4: one goroutine per article.
// At 100,000 articles that's 100,000 goroutines — each consuming memory,
// each competing for the scheduler, each potentially hammering the LLM
// provider until rate limits fire.
//
// Worker pools solve this by decoupling concurrency from input size.
// You control exactly how many goroutines run, regardless of article count.
//
// Architecture:
//
//	articles slice
//	     ↓ (feed)
//	  jobs channel        ← bounded input queue
//	     ↓ (consume)
//	  W worker goroutines ← W is configurable; try 5, 10, 50
//	     ↓ (send result)
//	 results channel
//	     ↓ (collect)
//	  results slice
//
// W workers stay alive for the duration of the job — they don't spawn and
// die per article. This is the pattern behind every production job queue,
// database connection pool, and HTTP server worker.
package pipeline

import (
	"fmt"
	"sync"
	"time"

	"github.com/moinuddin/go-concurrent-ai-systems/arc-1-foundations/part-05-worker-pools/internal/model"
	"github.com/moinuddin/go-concurrent-ai-systems/arc-1-foundations/part-05-worker-pools/internal/simulator"
)

// WorkerPool processes articles using a fixed number of worker goroutines.
// Concurrency is bounded by Workers — increase it to saturate throughput,
// decrease it to respect rate limits.
type WorkerPool struct {
	llm     *simulator.LLMClient
	Workers int // number of concurrent worker goroutines
}

// New returns a WorkerPool with the given worker count and LLM simulator.
// A Workers value of 0 panics — at least one worker is required.
func New(llm *simulator.LLMClient, workers int) *WorkerPool {
	if workers <= 0 {
		panic("WorkerPool: Workers must be > 0")
	}
	return &WorkerPool{llm: llm, Workers: workers}
}

// ProcessAll feeds articles into a jobs channel, processes them with
// Workers goroutines, and collects results through a results channel.
// Returns all results and total wall-clock duration.
func (p *WorkerPool) ProcessAll(articles []model.Article) ([]model.AIResult, time.Duration) {
	start := time.Now()

	jobs := make(chan model.Article, len(articles))
	resultsCh := make(chan model.AIResult, len(articles))

	// Start W workers. Each worker blocks on the jobs channel until work
	// arrives or the channel closes. When the channel closes, the worker exits.
	var wg sync.WaitGroup
	for w := 1; w <= p.Workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			p.worker(workerID, jobs, resultsCh)
		}(w)
	}

	// Feed all articles into the jobs channel, then close it.
	// Workers see the close and drain any remaining jobs before exiting.
	for _, article := range articles {
		jobs <- article
	}
	close(jobs)

	// Wait for all workers to finish, then close the results channel so the
	// collector loop below can exit cleanly.
	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	// Collect results. Single goroutine owns the slice — no mutex needed.
	results := make([]model.AIResult, 0, len(articles))
	for result := range resultsCh {
		results = append(results, result)
	}

	return results, time.Since(start)
}

// worker processes articles from the jobs channel until it is closed.
// Each worker handles many articles sequentially — concurrency comes from
// having multiple workers running simultaneously, not from spawning goroutines
// per article.
func (p *WorkerPool) worker(id int, jobs <-chan model.Article, results chan<- model.AIResult) {
	for article := range jobs {
		fmt.Printf("[worker %d] processing article %d\n", id, article.ID)
		result := p.processArticle(article)
		results <- result
		fmt.Printf("[worker %d] completed  article %d\n", id, article.ID)
	}
	fmt.Printf("[worker %d] jobs channel closed — exiting\n", id)
}

func (p *WorkerPool) processArticle(article model.Article) model.AIResult {
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
