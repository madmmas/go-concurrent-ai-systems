package pipeline

import (
	"fmt"

	"github.com/moinuddin/go-concurrent-ai-systems/arc-1-foundations/part-02-goroutines/internal/model"
	"github.com/moinuddin/go-concurrent-ai-systems/arc-1-foundations/part-02-goroutines/internal/simulator"
)

// BrokenProcessor launches goroutines but does not wait for them.
// main() exits before any goroutine finishes — all work is silently discarded.
//
// This is the first thing most developers try. The compiler won't catch it.
// The output looks almost reasonable at a glance:
//
//	Processing article 1
//	Processing article 2
//	Finished in 120µs
//
// But "Finished in 120µs" for ten articles is the tell — the work never ran.
// See ConcurrentProcessor for the correct version.
type BrokenProcessor struct {
	llm *simulator.LLMClient
}

// NewBroken returns a BrokenProcessor for demonstration purposes.
func NewBroken(llm *simulator.LLMClient) *BrokenProcessor {
	return &BrokenProcessor{llm: llm}
}

// ProcessAll launches one goroutine per article but returns immediately.
// The goroutines are orphaned when the caller exits.
func (p *BrokenProcessor) ProcessAll(articles []model.Article) {
	for _, article := range articles {
		// BUG: no WaitGroup — main() exits before these complete.
		go p.processArticle(article) // nolint:errcheck
	}
	fmt.Println("(returned immediately — goroutines still running in background)")
}

func (p *BrokenProcessor) processArticle(article model.Article) {
	fmt.Printf("Processing article %d\n", article.ID)
	p.llm.Call("Summarization", article.ID)
	p.llm.Call("Sentiment Analysis", article.ID)
	p.llm.Call("Keyword Extraction", article.ID)
	fmt.Printf("Completed article %d\n", article.ID)
}
