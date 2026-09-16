package pipeline

import (
	"fmt"
	"sync"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-1-foundations/part-02-goroutines/internal/model"
	"github.com/madmmas/go-concurrent-ai-systems/arc-1-foundations/part-02-goroutines/internal/simulator"
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

// LoopCaptureProcessor demonstrates the classic Go loop-variable capture bug.
// Before Go 1.22, the loop variable was reused across iterations, so every
// goroutine could see the last article. We recreate that bug by capturing
// the loop variable by reference deliberately.
//
// Run via: go run ./cmd/broken -mode=loop-capture
type LoopCaptureProcessor struct {
	llm *simulator.LLMClient
}

// NewLoopCapture returns a LoopCaptureProcessor for demonstration.
func NewLoopCapture(llm *simulator.LLMClient) *LoopCaptureProcessor {
	return &LoopCaptureProcessor{llm: llm}
}

// ProcessAll spawns goroutines that all close over the same shared variable.
// Most (often all) workers process the last article — the classic capture bug,
// recreated explicitly so it still demos on Go 1.22+ (per-iteration loop vars).
func (p *LoopCaptureProcessor) ProcessAll(articles []model.Article) {
	var wg sync.WaitGroup
	var shared model.Article // deliberate shared capture target
	for _, article := range articles {
		wg.Add(1)
		shared = article
		// BUG: goroutine closes over `shared` instead of taking article by value.
		go func() {
			defer wg.Done()
			fmt.Printf("Processing article %d (loop-capture demo)\n", shared.ID)
			time.Sleep(10 * time.Millisecond)
			p.llm.Call("Summarization", shared.ID)
			fmt.Printf("Completed article %d\n", shared.ID)
		}()
	}
	wg.Wait()
}
