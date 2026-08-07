package pipeline_test

import (
	"context"
	"testing"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-11-pipeline-stages/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-11-pipeline-stages/internal/simulator"
)

func newFastPipeline() *pipeline.Pipeline {
	return pipeline.New(
		simulator.New(simulator.FastConfig),
		500*time.Millisecond,
		[]pipeline.StageConfig{
			{Name: "scrape",    Workers: 3},
			{Name: "clean",     Workers: 2},
			{Name: "embed",     Workers: 2},
			{Name: "summarise", Workers: 2},
		},
	)
}

func TestPipeline_AllArticlesComplete(t *testing.T) {
	p := newFastPipeline()
	articles := pipeline.GenerateArticles(10)
	results, _ := p.ProcessAll(context.Background(), articles)
	if len(results) != len(articles) {
		t.Fatalf("expected %d results, got %d", len(articles), len(results))
	}
}

func TestPipeline_SuccessfulResultsHaveData(t *testing.T) {
	p := newFastPipeline()
	results, _ := p.ProcessAll(context.Background(), pipeline.GenerateArticles(5))
	for _, r := range results {
		if r.Err != nil {
			continue // failed results skip data checks
		}
		if r.Summary == "" {
			t.Errorf("article %d: Summary empty", r.ArticleID)
		}
		if len(r.Embedding) == 0 {
			t.Errorf("article %d: Embedding empty", r.ArticleID)
		}
	}
}

func TestPipeline_NoRace(t *testing.T) {
	p := newFastPipeline()
	results, _ := p.ProcessAll(context.Background(), pipeline.GenerateArticles(30))
	if len(results) != 30 {
		t.Errorf("expected 30 results, got %d", len(results))
	}
}

func TestPipeline_ErrorPropagatesThrough(t *testing.T) {
	// Use a profile where scrape always fails
	cfg := simulator.Config{
		MinLatency: 5 * time.Millisecond,
		MaxLatency: 10 * time.Millisecond,
		Failure: simulator.FailureProfile{
			ServerErrRate: 1.0,
		},
	}
	p := pipeline.New(
		simulator.New(cfg),
		500*time.Millisecond,
		pipeline.DefaultStages(),
	)
	results, _ := p.ProcessAll(context.Background(), pipeline.GenerateArticles(5))
	if len(results) != 5 {
		t.Fatalf("expected 5 results even on failure, got %d", len(results))
	}
	for _, r := range results {
		if r.Err == nil {
			t.Errorf("article %d: expected error from failed scrape, got nil", r.ArticleID)
		}
	}
}
