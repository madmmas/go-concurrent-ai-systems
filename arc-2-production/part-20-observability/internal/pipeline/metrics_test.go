package pipeline_test

import (
	"context"
	"testing"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-20-observability/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-20-observability/internal/simulator"
)

func newPool(workers int) *pipeline.ObservablePool {
	return pipeline.New(simulator.New(simulator.FastConfig), workers, 500*time.Millisecond)
}

func TestObservable_MetricsRecorded(t *testing.T) {
	pool := newPool(3)
	results, _ := pool.ProcessAll(context.Background(), pipeline.GenerateArticles(10))
	if len(results) != 10 {
		t.Fatalf("expected 10, got %d", len(results))
	}
	summary := pool.Metrics.Summary()
	if summary == "" {
		t.Error("metrics summary is empty")
	}
	t.Log("\nMetrics:\n" + summary)
}

func TestObservable_ErrorsTracked(t *testing.T) {
	cfg := simulator.Config{
		MinLatency: 5 * time.Millisecond,
		MaxLatency: 10 * time.Millisecond,
		Failure:    simulator.FailureProfile{ServerErrRate: 0.5},
	}
	pool := pipeline.New(simulator.New(cfg), 3, 200*time.Millisecond)
	pool.ProcessAll(context.Background(), pipeline.GenerateArticles(20))

	summary := pool.Metrics.Summary()
	t.Log("\nMetrics with 50% error rate:\n" + summary)
	// Just verify it doesn't panic and produces output
	if summary == "" {
		t.Error("metrics summary empty after processing with errors")
	}
}

func TestObservable_NoRace(t *testing.T) {
	pool := newPool(5)
	results, _ := pool.ProcessAll(context.Background(), pipeline.GenerateArticles(30))
	if len(results) != 30 {
		t.Errorf("expected 30, got %d", len(results))
	}
}
