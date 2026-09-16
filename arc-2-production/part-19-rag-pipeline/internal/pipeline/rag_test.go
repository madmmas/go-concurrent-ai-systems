package pipeline_test

import (
	"context"
	"testing"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-19-rag-pipeline/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-19-rag-pipeline/internal/simulator"
)

func newRAG() *pipeline.RAGPipeline {
	return pipeline.New(simulator.New(simulator.FastConfig), 3, 3, 2, 3, 500*time.Millisecond)
}

func TestRAG_AllArticlesProduceResult(t *testing.T) {
	rag := newRAG()
	results, _ := rag.ProcessAll(context.Background(), pipeline.GenerateArticles(6))
	if len(results) != 6 {
		t.Fatalf("expected 6 RAG results, got %d", len(results))
	}
}

func TestRAG_ResultsHaveAnswer(t *testing.T) {
	rag := newRAG()
	results, _ := rag.ProcessAll(context.Background(), pipeline.GenerateArticles(3))
	for _, r := range results {
		if r.Err != nil {
			t.Errorf("article %d: unexpected error: %v", r.ArticleID, r.Err)
			continue
		}
		if r.Answer == "" {
			t.Errorf("article %d: Answer is empty", r.ArticleID)
		}
		if r.ChunkCount == 0 {
			t.Errorf("article %d: no chunks", r.ArticleID)
		}
	}
}

func TestRAG_NoRace(t *testing.T) {
	rag := pipeline.New(simulator.New(simulator.FastConfig), 5, 5, 3, 4, 500*time.Millisecond)
	results, _ := rag.ProcessAll(context.Background(), pipeline.GenerateArticles(12))
	if len(results) != 12 {
		t.Errorf("expected 12, got %d", len(results))
	}
}
