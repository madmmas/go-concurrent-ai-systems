package pipeline_test

import (
	"testing"
	"time"

	"github.com/moinuddin/go-concurrent-ai-systems/arc-1-foundations/part-05-worker-pools/internal/pipeline"
	"github.com/moinuddin/go-concurrent-ai-systems/arc-1-foundations/part-05-worker-pools/internal/simulator"
)

func newPool(workers int) *pipeline.WorkerPool {
	return pipeline.New(simulator.New(simulator.FastConfig), workers)
}

// TestProcessAll_AllResultsDelivered verifies the worker pool delivers every
// result regardless of article count or worker count.
func TestProcessAll_AllResultsDelivered(t *testing.T) {
	cases := []struct {
		articles int
		workers  int
	}{
		{articles: 1, workers: 1},   // edge: single worker, single article
		{articles: 10, workers: 3},  // fewer workers than articles
		{articles: 10, workers: 10}, // workers == articles
		{articles: 10, workers: 20}, // more workers than articles
		{articles: 50, workers: 5},  // typical production ratio
	}

	for _, tc := range cases {
		pool := newPool(tc.workers)
		articles := pipeline.GenerateArticles(tc.articles)

		results, _ := pool.ProcessAll(articles)

		if len(results) != tc.articles {
			t.Errorf("articles=%d workers=%d: expected %d results, got %d",
				tc.articles, tc.workers, tc.articles, len(results))
		}
	}
}

// TestProcessAll_WorkerCountBoundsConcurrency verifies that worker count
// actually controls throughput. With 1 worker, processing is essentially
// sequential — total time ≈ n × per-article time.
// With n workers, total time ≈ per-article time regardless of n.
func TestProcessAll_WorkerCountBoundsConcurrency(t *testing.T) {
	cfg := simulator.Config{
		MinLatency: 20 * time.Millisecond,
		MaxLatency: 30 * time.Millisecond,
	}
	llm := simulator.New(cfg)
	articles := pipeline.GenerateArticles(5)

	// 1 worker — sequential behaviour.
	pool1 := pipeline.New(llm, 1)
	_, dur1Worker := pool1.ProcessAll(articles)

	// 5 workers — all articles run in parallel.
	pool5 := pipeline.New(llm, 5)
	_, dur5Workers := pool5.ProcessAll(articles)

	ratio := float64(dur1Worker) / float64(dur5Workers)
	if ratio < 2.0 {
		t.Errorf(
			"expected 5-worker pool to be at least 2× faster than 1-worker pool.\n"+
				"1 worker: %v, 5 workers: %v, ratio: %.2f\n"+
				"If ratio is near 1.0, worker count is not controlling concurrency.",
			dur1Worker, dur5Workers, ratio,
		)
	}
}

// TestProcessAll_NoRaceUnderLoad confirms the worker pool has no data races.
// Run with -race:
//
//	go test -race ./internal/pipeline/...
func TestProcessAll_NoRaceUnderLoad(t *testing.T) {
	pool := pipeline.New(simulator.New(simulator.FastConfig), 10)
	results, _ := pool.ProcessAll(pipeline.GenerateArticles(100))

	if len(results) != 100 {
		t.Errorf("expected 100 results, got %d", len(results))
	}
}

// TestNewWorkerPool_PanicsOnZeroWorkers documents that creating a pool
// with zero workers panics — it would otherwise deadlock silently.
func TestNewWorkerPool_PanicsOnZeroWorkers(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic for Workers=0, but no panic occurred")
		}
	}()
	pipeline.New(simulator.New(simulator.FastConfig), 0)
}
