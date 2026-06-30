package pipeline_test

import (
	"testing"
	"time"

	"github.com/moinuddin/go-concurrent-ai-systems/arc-1-foundations/part-02-goroutines/internal/pipeline"
	"github.com/moinuddin/go-concurrent-ai-systems/arc-1-foundations/part-02-goroutines/internal/simulator"
)

func newFastProcessor() *pipeline.ConcurrentProcessor {
	return pipeline.New(simulator.New(simulator.FastConfig))
}

// TestProcessAll_CorrectResults verifies the concurrent pipeline produces
// complete output for every article despite goroutines running in parallel.
func TestProcessAll_CorrectResults(t *testing.T) {
	proc := newFastProcessor()
	articles := pipeline.GenerateArticles(5)

	results, _ := proc.ProcessAll(articles)

	if len(results) != len(articles) {
		t.Fatalf("expected %d results, got %d", len(articles), len(results))
	}
	for _, r := range results {
		if r.Summary == "" {
			t.Errorf("article %d: Summary is empty", r.ArticleID)
		}
		if r.Sentiment == "" {
			t.Errorf("article %d: Sentiment is empty", r.ArticleID)
		}
		if len(r.Keywords) == 0 {
			t.Errorf("article %d: Keywords slice is empty", r.ArticleID)
		}
	}
}

// TestProcessAll_OrderingNotGuaranteed documents that concurrent processing
// does NOT preserve input order — the fastest article finishes first.
//
// This is the inverse of Part 1's TestProcessAll_SequentialOrdering.
// If you need ordered results from a concurrent pipeline, you must sort
// them after collection or use an index-based approach. That tradeoff is
// discussed in Part 3.
func TestProcessAll_OrderingNotGuaranteed(t *testing.T) {
	proc := newFastProcessor()
	// Use enough articles that at least some will finish out of order.
	articles := pipeline.GenerateArticles(10)

	results, _ := proc.ProcessAll(articles)

	// Build result ID set — we verify all are present, not their order.
	seen := make(map[int]bool, len(results))
	for _, r := range results {
		seen[r.ArticleID] = true
	}
	for _, a := range articles {
		if !seen[a.ID] {
			t.Errorf("article %d missing from results", a.ID)
		}
	}
	// We intentionally do NOT check ordering here.
	// Documenting this in a test is more honest than pretending it doesn't matter.
}

// TestProcessAll_DurationSublinear verifies the central claim of Part 2:
// concurrent processing time does NOT grow linearly with article count.
//
// With sequential processing (Part 1): 10 articles ≈ 10× one article.
// With concurrent processing (Part 2): 10 articles ≈ 1–2× one article.
//
// We assert the 10-article run takes less than 5× the 1-article run.
// In practice the ratio should be close to 1× since all articles run in
// parallel and are bounded by the slowest goroutine.
func TestProcessAll_DurationSublinear(t *testing.T) {
	cfg := simulator.Config{
		MinLatency: 20 * time.Millisecond,
		MaxLatency: 30 * time.Millisecond,
	}
	proc := pipeline.New(simulator.New(cfg))

	_, dur1 := proc.ProcessAll(pipeline.GenerateArticles(1))
	_, dur10 := proc.ProcessAll(pipeline.GenerateArticles(10))

	ratio := float64(dur10) / float64(dur1)
	if ratio >= 5.0 {
		t.Errorf(
			"10-article run should take less than 5× a 1-article run (concurrent). "+
				"Got ratio %.2f — 1 article: %v, 10 articles: %v.\n"+
				"If ratio is near 10×, the sequential processor was used instead.",
			ratio, dur1, dur10,
		)
	}
}
