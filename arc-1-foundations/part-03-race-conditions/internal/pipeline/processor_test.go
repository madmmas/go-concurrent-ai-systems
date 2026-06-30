package pipeline_test

import (
	"testing"
	"time"

	"github.com/moinuddin/go-concurrent-ai-systems/arc-1-foundations/part-03-race-conditions/internal/pipeline"
	"github.com/moinuddin/go-concurrent-ai-systems/arc-1-foundations/part-03-race-conditions/internal/simulator"
)

func newFastProcessor() *pipeline.SafeProcessor {
	return pipeline.New(simulator.New(simulator.FastConfig))
}

// TestProcessAll_CorrectResultCount verifies that the mutex-protected pipeline
// always returns exactly the right number of results — no dropped writes.
//
// The broken version in Part 2 would occasionally return fewer than expected.
// Run this test 10 times in a row; it must pass every time.
func TestProcessAll_CorrectResultCount(t *testing.T) {
	proc := newFastProcessor()
	articles := pipeline.GenerateArticles(10)

	// Run multiple times to expose any remaining flakiness.
	for i := 0; i < 5; i++ {
		results, _ := proc.ProcessAll(articles)
		if len(results) != len(articles) {
			t.Fatalf("run %d: expected %d results, got %d — possible race condition",
				i+1, len(articles), len(results))
		}
	}
}

// TestProcessAll_NoRaceUnderHighConcurrency stress-tests the mutex by running
// many articles simultaneously. The race detector (-race flag) will catch any
// unprotected concurrent writes if the mutex is removed or incorrectly placed.
//
// Run this test as:
//
//	go test -race ./internal/pipeline/...
func TestProcessAll_NoRaceUnderHighConcurrency(t *testing.T) {
	proc := newFastProcessor()
	// 50 concurrent goroutines all appending to the same slice.
	articles := pipeline.GenerateArticles(50)

	results, _ := proc.ProcessAll(articles)

	if len(results) != len(articles) {
		t.Errorf("expected %d results under high concurrency, got %d",
			len(articles), len(results))
	}
}

// TestProcessAll_MutexDoesNotSerialize verifies that adding a mutex does not
// re-serialize the pipeline. The LLM calls happen outside the critical section,
// so 10 articles should still complete much faster than 10× one article.
//
// If this test fails, it means the lock scope is too wide — the mutex is
// being held during the LLM call, queuing goroutines and killing throughput.
func TestProcessAll_MutexDoesNotSerialize(t *testing.T) {
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
			"mutex appears to be serializing LLM calls — ratio %.2f (want < 5×).\n"+
				"Check that mu.Lock() wraps only the append, not processArticle().",
			ratio,
		)
	}
}
