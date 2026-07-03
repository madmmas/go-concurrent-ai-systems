package pipeline_test

import (
	"testing"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-1-foundations/part-06-buffered-channels/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-1-foundations/part-06-buffered-channels/internal/simulator"
)

// TestUnbuffered_AllResults verifies the unbuffered pipeline
// delivers every result correctly.
func TestUnbuffered_AllResults(t *testing.T) {
	p := pipeline.NewUnbuffered(simulator.New(simulator.FastConfig))
	articles := pipeline.GenerateArticles(10)

	results, _ := p.ProcessAll(articles)

	if len(results) != len(articles) {
		t.Fatalf("expected %d results, got %d", len(articles), len(results))
	}
}

// TestBuffered_AllResults verifies the buffered pipeline
// delivers every result correctly across different buffer sizes.
func TestBuffered_AllResults(t *testing.T) {
	cases := []struct {
		name       string
		bufferSize int
		articles   int
	}{
		{"buffer=1 (near-unbuffered)", 1, 10},
		{"buffer=5 (partial)", 5, 10},
		{"buffer=10 (full capacity)", 10, 10},
		{"buffer=20 (larger than articles)", 20, 10},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := pipeline.NewBuffered(simulator.New(simulator.FastConfig), tc.bufferSize)
			articles := pipeline.GenerateArticles(tc.articles)

			results, _ := p.ProcessAll(articles)

			if len(results) != tc.articles {
				t.Errorf("expected %d results, got %d", tc.articles, len(results))
			}
		})
	}
}

// TestBuffered_vs_Unbuffered_SameResults verifies both pipelines
// produce identical result counts — buffer size affects timing, not correctness.
func TestBuffered_vs_Unbuffered_SameResults(t *testing.T) {
	llm := simulator.New(simulator.FastConfig)
	articles := pipeline.GenerateArticles(20)

	unbuffered := pipeline.NewUnbuffered(llm)
	buffered := pipeline.NewBuffered(simulator.New(simulator.FastConfig), 20)

	r1, _ := unbuffered.ProcessAll(articles)
	r2, _ := buffered.ProcessAll(articles)

	if len(r1) != len(r2) {
		t.Errorf("unbuffered=%d buffered=%d — should be equal", len(r1), len(r2))
	}
}

// TestBuffered_DoesNotDeadlockWhenBufferFull verifies that when the buffer
// fills up, workers block (not deadlock) and the pipeline still completes.
//
// This tests the key behaviour: buffer=1 with 10 articles means workers
// frequently block waiting for space, but the pipeline always finishes.
func TestBuffered_DoesNotDeadlockWhenBufferFull(t *testing.T) {
	p := pipeline.NewBuffered(simulator.New(simulator.FastConfig), 1)
	articles := pipeline.GenerateArticles(10)

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
		// passed — buffer=1 didn't deadlock
	case <-time.After(15 * time.Second):
		t.Fatal("buffered pipeline with buffer=1 appears deadlocked")
	}
}

// TestBuffered_RaceClean verifies no data races under concurrent sends.
// Run with: go test -race ./internal/...
func TestBuffered_RaceClean(t *testing.T) {
	p := pipeline.NewBuffered(simulator.New(simulator.FastConfig), 10)
	results, _ := p.ProcessAll(pipeline.GenerateArticles(50))
	if len(results) != 50 {
		t.Errorf("expected 50 results, got %d", len(results))
	}
}
