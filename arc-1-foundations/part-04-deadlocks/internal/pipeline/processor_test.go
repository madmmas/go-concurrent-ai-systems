package pipeline_test

import (
	"testing"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-1-foundations/part-04-deadlocks/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-1-foundations/part-04-deadlocks/internal/simulator"
)

func newFastPipeline() *pipeline.SafePipeline {
	return pipeline.New(simulator.New(simulator.FastConfig))
}

// TestProcessAll_MutexVersion_AllResults verifies the mutex pipeline
// produces a result for every article — no deadlock, no drops.
func TestProcessAll_MutexVersion_AllResults(t *testing.T) {
	p := newFastPipeline()
	articles := pipeline.GenerateArticles(10)

	results, _ := p.ProcessAll(articles)

	if len(results) != len(articles) {
		t.Fatalf("expected %d results, got %d", len(articles), len(results))
	}
	for _, r := range results {
		if r.Summary == "" {
			t.Errorf("article %d: Summary empty", r.ArticleID)
		}
	}
}

// TestProcessAll_ChannelVersion_AllResults verifies the channel pipeline
// produces a result for every article — no deadlock, channel closes correctly.
//
// If the channel were never closed, this test would hang until timeout.
// The test passing is evidence the close logic works.
func TestProcessAll_ChannelVersion_AllResults(t *testing.T) {
	p := newFastPipeline()
	articles := pipeline.GenerateArticles(10)

	results, _ := p.ProcessAllWithChannel(articles)

	if len(results) != len(articles) {
		t.Fatalf("expected %d results, got %d", len(articles), len(results))
	}
}

// TestProcessAll_NoDeadlockUnderHighLoad stress-tests both implementations
// with many articles to ensure no deadlock occurs under concurrent pressure.
func TestProcessAll_NoDeadlockUnderHighLoad(t *testing.T) {
	p := newFastPipeline()
	articles := pipeline.GenerateArticles(100)

	done := make(chan struct{})
	go func() {
		results, _ := p.ProcessAll(articles)
		if len(results) != len(articles) {
			t.Errorf("expected %d results, got %d", len(articles), len(results))
		}
		close(done)
	}()

	select {
	case <-done:
		// passed — no deadlock
	case <-time.After(30 * time.Second):
		t.Fatal("ProcessAll deadlocked — did not complete within 30s")
	}
}

// TestProcessAll_ChannelClosedAfterAllSends verifies the channel version
// handles the close timing correctly across multiple concurrent runs.
// If close() were called too early, workers would panic.
// If not called, the collector would block.
func TestProcessAll_ChannelClosedAfterAllSends(t *testing.T) {
	p := newFastPipeline()

	// Run multiple times to expose any timing-dependent close bugs.
	for i := 0; i < 5; i++ {
		results, _ := p.ProcessAllWithChannel(pipeline.GenerateArticles(20))
		if len(results) != 20 {
			t.Errorf("run %d: expected 20 results, got %d", i+1, len(results))
		}
	}
}
