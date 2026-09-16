package pipeline_test

import (
	"context"
	"testing"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-16-backpressure/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-16-backpressure/internal/simulator"
)

func newPool(queueDepth int) *pipeline.BackpressurePool {
	return pipeline.New(simulator.New(simulator.FastConfig), 3, queueDepth, 500*time.Millisecond)
}

func TestBackpressure_AllResultsDelivered(t *testing.T) {
	pool := newPool(5)
	results, _ := pool.ProcessWithProducer(context.Background(), pipeline.GenerateArticles(10), 0)
	if len(results) != 10 {
		t.Fatalf("expected 10 results, got %d", len(results))
	}
}

func TestBackpressure_SmallQueueDoesNotDeadlock(t *testing.T) {
	// queue=1 means extreme backpressure — producer blocks after each article
	pool := newPool(1)
	done := make(chan struct{})
	go func() {
		results, _ := pool.ProcessWithProducer(context.Background(), pipeline.GenerateArticles(9), 0)
		if len(results) != 9 {
			t.Errorf("expected 9 results with queue=1, got %d", len(results))
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("deadlock: queue=1 pipeline did not complete in 20s")
	}
}

func TestBackpressure_NoRace(t *testing.T) {
	pool := newPool(10)
	results, _ := pool.ProcessWithProducer(context.Background(), pipeline.GenerateArticles(30), 0)
	if len(results) != 30 {
		t.Errorf("expected 30 results, got %d", len(results))
	}
}
