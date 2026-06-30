// Package pipeline implements the sequential article processing pipeline
// introduced in Part 1 of the series.
//
// The design is intentionally simple: one article at a time, three AI tasks
// per article, fully sequential. This is the baseline we measure and then
// replace with concurrent designs in later parts.
package pipeline

import (
	"fmt"
	"time"

	"github.com/moinuddin/go-concurrent-ai-systems/arc-1-foundations/part-01-sequential/internal/model"
	"github.com/moinuddin/go-concurrent-ai-systems/arc-1-foundations/part-01-sequential/internal/simulator"
)

// Processor runs AI tasks against articles one at a time.
type Processor struct {
	llm *simulator.LLMClient
}

// New returns a Processor backed by the given LLM simulator.
func New(llm *simulator.LLMClient) *Processor {
	return &Processor{llm: llm}
}

// ProcessAll processes every article in the slice sequentially and returns
// the collected results alongside the total wall-clock duration.
//
// This is the function the blog post benchmarks. In Part 2 we replace it
// with a concurrent version and compare the numbers directly.
func (p *Processor) ProcessAll(articles []model.Article) ([]model.AIResult, time.Duration) {
	start := time.Now()

	results := make([]model.AIResult, 0, len(articles))
	for _, article := range articles {
		result := p.processArticle(article)
		results = append(results, result)
	}

	return results, time.Since(start)
}

// processArticle runs the full AI pipeline for a single article.
// The three tasks run one after another; the next article never starts
// until all three complete. That sequential wait is the bottleneck
// the blog post measures.
func (p *Processor) processArticle(article model.Article) model.AIResult {
	fmt.Printf("\nProcessing article %d...\n", article.ID)

	summary := p.summarize(article)
	sentiment := p.analyzeSentiment(article)
	keywords := p.extractKeywords(article)

	return model.AIResult{
		ArticleID: article.ID,
		Summary:   summary,
		Sentiment: sentiment,
		Keywords:  keywords,
	}
}

func (p *Processor) summarize(a model.Article) string {
	p.llm.Call("Summarization", a.ID)
	return "AI-generated summary"
}

func (p *Processor) analyzeSentiment(a model.Article) string {
	p.llm.Call("Sentiment Analysis", a.ID)
	return "Positive"
}

func (p *Processor) extractKeywords(a model.Article) []string {
	p.llm.Call("Keyword Extraction", a.ID)
	return []string{"AI", "Go", "Concurrency"}
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
