package pipeline_test

import (
	"testing"
	"time"

	"github.com/moinuddin/go-concurrent-ai-systems/arc-1-foundations/part-04-channels/internal/pipeline"
	"github.com/moinuddin/go-concurrent-ai-systems/arc-1-foundations/part-04-channels/internal/simulator"
)

func newFastProcessor() *pipeline.ChannelProcessor {
	return pipeline.New(simulator.New(simulator.FastConfig))
}

// TestProcessAll_AllResultsCollected verifies the channel pipeline delivers
// every result to the collector — no sends lost, no goroutines leaked.
//
// This test also validates the channel close logic: if the channel were never
// closed, the range loop would block forever and the test would time out.
func TestProcessAll_AllResultsCollected(t *testing.T) {
	proc := newFastProcessor()
	articles := pipeline.GenerateArticles(10)

	results, _ := proc.ProcessAll(articles)

	if len(results) != len(articles) {
		t.Fatalf("expected %d results, got %d — possible channel leak or close bug",
			len(articles), len(results))
	}
}

// TestProcessAll_NoRace verifies the channel design eliminates the data race
// from Part 3 without requiring a mutex. Run with -race:
//
//	go test -race ./internal/pipeline/...
//
// The race detector should stay silent. The single-owner collection pattern —
// only one goroutine appends to the slice — is why no synchronization is needed.
func TestProcessAll_NoRace(t *testing.T) {
	proc := newFastProcessor()
	// High concurrency to give the race detector a chance to fire if anything
	// is wrong with the channel ownership model.
	results, _ := proc.ProcessAll(pipeline.GenerateArticles(50))

	if len(results) != 50 {
		t.Errorf("expected 50 results, got %d", len(results))
	}
}

// TestProcessAll_ResultsPopulated verifies each collected result has content —
// the channel is delivering actual AIResult values, not zero values.
func TestProcessAll_ResultsPopulated(t *testing.T) {
	proc := newFastProcessor()
	articles := pipeline.GenerateArticles(5)

	results, _ := proc.ProcessAll(articles)

	for _, r := range results {
		if r.Summary == "" {
			t.Errorf("article %d: Summary empty", r.ArticleID)
		}
		if r.Sentiment == "" {
			t.Errorf("article %d: Sentiment empty", r.ArticleID)
		}
		if len(r.Keywords) == 0 {
			t.Errorf("article %d: Keywords empty", r.ArticleID)
		}
	}
}

// TestProcessAll_DurationSublinear verifies the channel design preserves
// the concurrency gains from Part 2 — adding a channel does not re-serialize.
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
			"channel pipeline appears serialized — ratio %.2f (want < 5×). "+
				"1 article: %v, 10 articles: %v",
			ratio, dur1, dur10,
		)
	}
}
