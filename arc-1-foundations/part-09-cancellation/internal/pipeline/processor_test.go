package pipeline_test

import (
	"context"
	"testing"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-1-foundations/part-09-cancellation/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-1-foundations/part-09-cancellation/internal/simulator"
)

func newFastPool(workers int) *pipeline.WorkerPool {
	return pipeline.New(
		simulator.New(simulator.FastConfig),
		workers,
		500*time.Millisecond,
	)
}

// TestProcessAll_NormalCompletion verifies the full happy path:
// all articles succeed and the ShutdownReport reflects it.
func TestProcessAll_NormalCompletion(t *testing.T) {
	pool := newFastPool(5)
	articles := pipeline.GenerateArticles(10)

	_, report := pool.ProcessAll(context.Background(), articles)

	if report.Succeeded != len(articles) {
		t.Errorf("expected %d succeeded, got %d", len(articles), report.Succeeded)
	}
	if report.Cancelled+report.Queued+report.Failed != 0 {
		t.Errorf("expected no failures: %s", report)
	}
}

// TestProcessAll_CancellationStopsPipeline verifies that cancelling the
// pipeline context stops work promptly and accounts for every article
// in the ShutdownReport — no articles silently disappear.
func TestProcessAll_CancellationStopsPipeline(t *testing.T) {
	cfg := simulator.Config{
		MinLatency: 300 * time.Millisecond,
		MaxLatency: 500 * time.Millisecond,
		Failure:    simulator.DefaultProfile,
	}
	pool := pipeline.New(simulator.New(cfg), 2, 10*time.Second)
	articles := pipeline.GenerateArticles(20)

	// Cancel after 200ms — workers won't finish a single article.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	results, report := pool.ProcessAll(ctx, articles)
	elapsed := time.Since(start)

	// Stopped quickly.
	if elapsed > 2*time.Second {
		t.Errorf("shutdown took too long: %v", elapsed)
	}

	// Every article accounted for.
	total := report.Succeeded + report.Failed + report.Cancelled + report.Queued
	if total != len(articles) {
		t.Errorf("report accounts for %d articles, want %d: %s", total, len(articles), report)
	}

	// Result slice also has every article.
	if len(results) != len(articles) {
		t.Errorf("results has %d entries, want %d", len(results), len(articles))
	}
}

// TestProcessAll_ShutdownReportIsComplete verifies the invariant that holds
// regardless of what happens: succeeded + failed + cancelled + queued
// always equals the number of input articles.
func TestProcessAll_ShutdownReportIsComplete(t *testing.T) {
	cfg := simulator.Config{
		MinLatency: 20 * time.Millisecond,
		MaxLatency: 80 * time.Millisecond,
		Failure:    simulator.DefaultProfile,
	}

	cases := []struct {
		name       string
		ctxTimeout time.Duration
	}{
		{"full run", 10 * time.Second},
		{"early cancel", 60 * time.Millisecond},
		{"very early cancel", 5 * time.Millisecond},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := pipeline.New(simulator.New(cfg), 3, 200*time.Millisecond)
			articles := pipeline.GenerateArticles(15)

			ctx, cancel := context.WithTimeout(context.Background(), tc.ctxTimeout)
			defer cancel()

			_, report := pool.ProcessAll(ctx, articles)

			total := report.Succeeded + report.Failed + report.Cancelled + report.Queued
			if total != len(articles) {
				t.Errorf("%s: report total %d != article count %d: %s",
					tc.name, total, len(articles), report)
			}
		})
	}
}

// TestProcessAll_InFlightCountsAsCancelled verifies that when the pipeline
// context is cancelled mid-article, in-flight work is Cancelled (via the
// context hierarchy) and remaining jobs are Queued — every article accounted.
func TestProcessAll_InFlightCountsAsCancelled(t *testing.T) {
	cfg := simulator.Config{
		MinLatency: 100 * time.Millisecond,
		MaxLatency: 150 * time.Millisecond,
		Failure:    simulator.DefaultProfile,
	}
	// 1 worker so behaviour is deterministic: processes one article at a time.
	pool := pipeline.New(simulator.New(cfg), 1, 2*time.Second)
	articles := pipeline.GenerateArticles(5)

	// Cancel after 50ms — mid-way through the first article's first task.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, report := pool.ProcessAll(ctx, articles)

	// All articles accounted for — none silently dropped.
	total := report.Succeeded + report.Failed + report.Cancelled + report.Queued
	if total != len(articles) {
		t.Errorf("total %d != article count %d: %s", total, len(articles), report)
	}

	// At least one in-flight cancel and some queued drains expected.
	if report.Cancelled+report.Queued == 0 {
		t.Errorf("expected Cancelled and/or Queued under early cancel: %s", report)
	}
}

// TestProcessAll_QueuedVsCancelledDistinction verifies drain-before-start
// is Queued and mid-flight context.Canceled is Cancelled — not conflated.
func TestProcessAll_QueuedVsCancelledDistinction(t *testing.T) {
	cfg := simulator.Config{
		MinLatency: 200 * time.Millisecond,
		MaxLatency: 300 * time.Millisecond,
		Failure:    simulator.DefaultProfile,
	}
	pool := pipeline.New(simulator.New(cfg), 3, 10*time.Second)
	articles := pipeline.GenerateArticles(10)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, report := pool.ProcessAll(ctx, articles)

	total := report.Succeeded + report.Failed + report.Cancelled + report.Queued
	if total != len(articles) {
		t.Fatalf("total %d != %d: %s", total, len(articles), report)
	}
	if report.Queued == 0 {
		t.Errorf("expected some Queued (never-started drains), got %s", report)
	}
	if report.Cancelled == 0 {
		t.Errorf("expected some Cancelled (in-flight), got %s", report)
	}
}
