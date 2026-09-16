package pipeline_test

import (
	"context"
	"testing"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-12-errgroup/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-12-errgroup/internal/simulator"
)

func newFastPool(workers int) *pipeline.ErrgroupPool {
	return pipeline.New(simulator.New(simulator.FastConfig), workers, 500*time.Millisecond)
}

func TestErrgroup_AllSucceed(t *testing.T) {
	pool := newFastPool(3)
	results, _ := pool.ProcessAll(context.Background(), pipeline.GenerateArticles(9))
	if len(results) != 9 {
		t.Fatalf("expected 9 results, got %d", len(results))
	}
	for _, r := range results {
		if r.Err != nil {
			t.Errorf("article %d: unexpected error: %v", r.ArticleID, r.Err)
		}
	}
}

func TestErrgroup_FailureCancelsGroup(t *testing.T) {
	// All calls fail immediately — errgroup should cancel siblings fast
	cfg := simulator.Config{
		MinLatency: 5 * time.Millisecond,
		MaxLatency: 10 * time.Millisecond,
		Failure: simulator.FailureProfile{
			ServerErrRate: 1.0,
		},
	}
	pool := pipeline.New(simulator.New(cfg), 3, 2*time.Second)
	results, dur := pool.ProcessAll(context.Background(), pipeline.GenerateArticles(6))

	// All fail
	if len(results) != 6 {
		t.Fatalf("expected 6 results, got %d", len(results))
	}
	for _, r := range results {
		if r.Err == nil {
			t.Errorf("article %d: expected error, got nil", r.ArticleID)
		}
	}
	// Should complete quickly — not wait out full timeouts
	if dur > 3*time.Second {
		t.Errorf("took too long on all-fail: %v", dur)
	}
}

func TestErrgroup_NoRace(t *testing.T) {
	pool := newFastPool(5)
	results, _ := pool.ProcessAll(context.Background(), pipeline.GenerateArticles(30))
	if len(results) != 30 {
		t.Errorf("expected 30 results, got %d", len(results))
	}
}
