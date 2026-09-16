package pipeline_test

import (
	"context"
	"testing"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-17-token-streaming/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-17-token-streaming/internal/simulator"
)

func newPool(workers int) *pipeline.StreamingPool {
	return pipeline.New(simulator.New(simulator.FastConfig), workers, 5*time.Second)
}

func TestStreaming_AllResultsDelivered(t *testing.T) {
	pool := newPool(3)
	results, _ := pool.ProcessAll(context.Background(), pipeline.GenerateArticles(6))
	if len(results) != 6 {
		t.Fatalf("expected 6, got %d", len(results))
	}
}

func TestStreaming_TokensAccumulated(t *testing.T) {
	pool := newPool(2)
	results, _ := pool.ProcessAll(context.Background(), pipeline.GenerateArticles(3))
	for _, r := range results {
		if r.Err != nil {
			t.Errorf("article %d: unexpected error: %v", r.ArticleID, r.Err)
			continue
		}
		if r.TokenCount == 0 {
			t.Errorf("article %d: no tokens streamed", r.ArticleID)
		}
		if r.FullText == "" {
			t.Errorf("article %d: FullText is empty", r.ArticleID)
		}
	}
}

func TestStreaming_ContextCancellation(t *testing.T) {
	cfg := simulator.Config{
		MinLatency: 100 * time.Millisecond,
		MaxLatency: 200 * time.Millisecond,
		Failure:    simulator.DefaultProfile,
	}
	pool := pipeline.New(simulator.New(cfg), 2, 5*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	results, _ := pool.ProcessAll(ctx, pipeline.GenerateArticles(10))
	if len(results) != 10 {
		t.Errorf("expected 10 results, got %d", len(results))
	}
}

func TestStreaming_NoRace(t *testing.T) {
	pool := newPool(5)
	results, _ := pool.ProcessAll(context.Background(), pipeline.GenerateArticles(20))
	if len(results) != 20 {
		t.Errorf("expected 20, got %d", len(results))
	}
}
