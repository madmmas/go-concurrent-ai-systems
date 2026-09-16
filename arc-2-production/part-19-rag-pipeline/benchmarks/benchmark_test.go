// Package benchmarks measures RAG pipeline throughput vs chunk count.
//
// More chunks per document = more embedding calls = higher cost but
// better retrieval precision. This benchmark quantifies the latency cost.
//
// BenchmarkRAG_1Chunk  — baseline: 1 embedding call per article
// BenchmarkRAG_3Chunks — standard: 3 embedding calls per article
// BenchmarkRAG_6Chunks — high-precision: 6 embedding calls per article
package benchmarks

import (
	"context"
	"testing"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-19-rag-pipeline/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-19-rag-pipeline/internal/simulator"
)

func newRAG(chunksPerDoc int) *pipeline.RAGPipeline {
	return pipeline.New(
		simulator.New(simulator.FastConfig),
		5, 3, 3, chunksPerDoc, 500*time.Millisecond,
	)
}

func BenchmarkRAG_1Chunk(b *testing.B) {
	rag := newRAG(1)
	arts := pipeline.GenerateArticles(5)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rag.ProcessAll(context.Background(), arts)
	}
}

func BenchmarkRAG_3Chunks(b *testing.B) {
	rag := newRAG(3)
	arts := pipeline.GenerateArticles(5)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rag.ProcessAll(context.Background(), arts)
	}
}

func BenchmarkRAG_6Chunks(b *testing.B) {
	rag := newRAG(6)
	arts := pipeline.GenerateArticles(5)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rag.ProcessAll(context.Background(), arts)
	}
}
