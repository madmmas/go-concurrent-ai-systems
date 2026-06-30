package pipeline_test

import (
	"testing"
	"time"

	"github.com/moinuddin/go-concurrent-ai-systems/arc-1-foundations/part-01-sequential/internal/pipeline"
	"github.com/moinuddin/go-concurrent-ai-systems/arc-1-foundations/part-01-sequential/internal/simulator"
)

// newFastProcessor returns a Processor backed by the fast simulator so
// the test suite completes in seconds rather than minutes.
func newFastProcessor() *pipeline.Processor {
	return pipeline.New(simulator.New(simulator.FastConfig))
}

// TestProcessAll_CorrectResults verifies the pipeline produces complete,
// correctly-linked output for every article. This is the baseline correctness
// check — if this fails, nothing else matters.
func TestProcessAll_CorrectResults(t *testing.T) {
	proc := newFastProcessor()
	articles := pipeline.GenerateArticles(5)

	results, _ := proc.ProcessAll(articles)

	if len(results) != len(articles) {
		t.Fatalf("expected %d results, got %d", len(articles), len(results))
	}

	for i, r := range results {
		if r.ArticleID != articles[i].ID {
			t.Errorf("results[%d].ArticleID = %d, want %d", i, r.ArticleID, articles[i].ID)
		}
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

// TestProcessAll_SequentialOrdering verifies results come back in the same
// order as the input articles.
//
// Sequential processing guarantees this — article 2 cannot complete before
// article 1 because article 2 never starts until article 1 finishes.
//
// NOTE: This test will FAIL in Part 2 when goroutines are introduced.
// That failure is intentional — it is the first observable contract difference
// between sequential and concurrent designs.
func TestProcessAll_SequentialOrdering(t *testing.T) {
	proc := newFastProcessor()
	articles := pipeline.GenerateArticles(5)

	results, _ := proc.ProcessAll(articles)

	for i, r := range results {
		if r.ArticleID != articles[i].ID {
			t.Errorf("results[%d].ArticleID = %d, want %d — ordering broken",
				i, r.ArticleID, articles[i].ID)
		}
	}
}

// TestProcessAll_DurationScalesLinearly verifies the central claim of Part 1:
// total processing time grows proportionally with article count because each
// article blocks until the previous one fully completes.
//
// We process 1 article and 3 articles and assert the 3-article run takes at
// least 2.5× as long. With truly sequential execution the ratio is ~3×.
//
// NOTE: This test will FAIL in Part 2. With goroutines, all articles run
// concurrently so the ratio collapses toward 1× regardless of article count.
// That collapse — visible right here in the test output — is the entire
// argument for concurrency in IO-bound workloads.
func TestProcessAll_DurationScalesLinearly(t *testing.T) {
	cfg := simulator.Config{
		MinLatency: 20 * time.Millisecond,
		MaxLatency: 30 * time.Millisecond,
	}
	proc := pipeline.New(simulator.New(cfg))

	_, dur1 := proc.ProcessAll(pipeline.GenerateArticles(1))
	_, dur3 := proc.ProcessAll(pipeline.GenerateArticles(3))

	ratio := float64(dur3) / float64(dur1)
	if ratio < 2.5 {
		t.Errorf(
			"3-article run should take at least 2.5× longer than 1-article run "+
				"(sequential scaling). Got ratio %.2f — 1 article: %v, 3 articles: %v.\n"+
				"If this ratio is near 1.0, a concurrent processor was swapped in.",
			ratio, dur1, dur3,
		)
	}
}
