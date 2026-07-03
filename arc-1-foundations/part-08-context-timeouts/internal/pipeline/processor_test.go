package pipeline_test

import (
	"context"
	"testing"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-1-foundations/part-08-context-timeouts/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-1-foundations/part-08-context-timeouts/internal/simulator"
)

func newFastPool(workers int) *pipeline.WorkerPool {
	return pipeline.New(
		simulator.New(simulator.FastConfig),
		workers,
		500*time.Millisecond,
	)
}

// TestProcessAll_AllSucceedWithGenerousTimeout verifies that a generous
// per-article timeout doesn't affect a healthy pipeline — all articles
// complete successfully, same as Part 5.
func TestProcessAll_AllSucceedWithGenerousTimeout(t *testing.T) {
	pool := newFastPool(5)
	articles := pipeline.GenerateArticles(10)

	results, _ := pool.ProcessAll(context.Background(), articles)

	if len(results) != len(articles) {
		t.Fatalf("expected %d results, got %d", len(articles), len(results))
	}
	for _, r := range results {
		if r.Err != nil {
			t.Errorf("article %d: unexpected error: %v", r.ArticleID, r.Err)
		}
	}
}

// TestProcessAll_TimeoutKillsSlowArticle verifies that a per-article timeout
// fires when the simulated latency exceeds the deadline.
//
// This is the central test of Part 6: a timed-out article produces a result
// with Err set (context.DeadlineExceeded) rather than blocking the worker
// indefinitely.
func TestProcessAll_TimeoutKillsSlowArticle(t *testing.T) {
	// Force every call to time out: MinLatency > our article timeout.
	cfg := simulator.Config{
		MinLatency: 200 * time.Millisecond,
		MaxLatency: 300 * time.Millisecond,
		Failure:    simulator.DefaultProfile,
	}
	pool := pipeline.New(
		simulator.New(cfg),
		3,
		50*time.Millisecond, // timeout shorter than any call
	)
	articles := pipeline.GenerateArticles(5)

	results, _ := pool.ProcessAll(context.Background(), articles)

	if len(results) != len(articles) {
		t.Fatalf("expected %d results, got %d", len(articles), len(results))
	}

	errCount := 0
	for _, r := range results {
		if r.Err != nil {
			errCount++
		}
	}
	if errCount == 0 {
		t.Error("expected at least one timed-out result, got none")
	}
}

// TestProcessAll_PipelineContextCancelsAll verifies that cancelling the
// pipeline-level context stops all in-flight and pending work.
//
// This is the distinction between two levels of context:
//   - per-article timeout: limits one article
//   - pipeline context:    limits everything
func TestProcessAll_PipelineContextCancelsAll(t *testing.T) {
	cfg := simulator.Config{
		MinLatency: 500 * time.Millisecond,
		MaxLatency: 1000 * time.Millisecond,
		Failure:    simulator.DefaultProfile,
	}
	pool := pipeline.New(
		simulator.New(cfg),
		2,
		10*time.Second,
	)
	articles := pipeline.GenerateArticles(10)

	// Cancel the pipeline after 100ms — well before any article can complete.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	results, _ := pool.ProcessAll(ctx, articles)
	elapsed := time.Since(start)

	// Pipeline should have stopped quickly, not run for the full article duration.
	if elapsed > 2*time.Second {
		t.Errorf("pipeline context cancellation took too long: %v (want < 2s)", elapsed)
	}

	// All results should carry a cancellation error.
	for _, r := range results {
		if r.Err == nil {
			t.Errorf("article %d: expected cancellation error, got nil", r.ArticleID)
		}
	}
}

// TestProcessAll_PartialFailures verifies mixed results when some articles
// time out and others complete. The total result count must always equal
// the input count — no articles silently lost.
func TestProcessAll_PartialFailures(t *testing.T) {
	cfg := simulator.Config{
		MinLatency: 10 * time.Millisecond,
		MaxLatency: 200 * time.Millisecond,
		Failure: simulator.FailureProfile{
			TimeoutRate:  0.3,
			TimeoutAfter: 500 * time.Millisecond,
		},
	}
	pool := pipeline.New(
		simulator.New(cfg),
		5,
		80*time.Millisecond,
	)
	articles := pipeline.GenerateArticles(20)

	results, _ := pool.ProcessAll(context.Background(), articles)

	if len(results) != len(articles) {
		t.Fatalf("expected %d results (including failures), got %d",
			len(articles), len(results))
	}
}
