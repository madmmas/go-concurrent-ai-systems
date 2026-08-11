// Package pipeline implements a concurrent RAG (Retrieval-Augmented Generation)
// pipeline — the flagship Part 19 of Arc 2.
//
// RAG stages (as in the blog):
//   1. Chunker    — split articles into text chunks
//   2. Embedder   — generate vector embeddings for each chunk (LLM call)
//   3. Collector  — group chunks by article; emit as soon as a set is complete
//   4. Generator  — produce a grounded answer from the chunk set (LLM call)
//
// Stages are concurrent pools connected by channels (Part 11). The collector
// streams complete articles to the generator in completion order — it does
// not wait for every embed to finish before generation starts.
package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-19-rag-pipeline/internal/model"
	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-19-rag-pipeline/internal/simulator"
)

// Chunk is a piece of an article for embedding.
type Chunk struct {
	ArticleID int
	ChunkID   int
	Text      string
	Embedding []float32
}

// RAGResult is the output of the full RAG pipeline.
type RAGResult struct {
	ArticleID  int
	Chunks     []Chunk
	Answer     string
	Err        error
	ChunkCount int
}

// articleChunks is a complete set of embedded chunks for one article.
type articleChunks struct {
	ArticleID int
	Chunks    []Chunk
}

// RAGPipeline runs a full concurrent RAG pipeline.
type RAGPipeline struct {
	llm          *simulator.LLMClient
	ChunkWorkers int
	EmbedWorkers int
	GenWorkers   int
	Timeout      time.Duration
	ChunksPerDoc int
}

// New returns a RAGPipeline.
func New(llm *simulator.LLMClient, chunkW, embedW, genW, chunksPerDoc int, timeout time.Duration) *RAGPipeline {
	return &RAGPipeline{
		llm:          llm,
		ChunkWorkers: chunkW,
		EmbedWorkers: embedW,
		GenWorkers:   genW,
		Timeout:      timeout,
		ChunksPerDoc: chunksPerDoc,
	}
}

// ProcessAll runs all articles through the RAG pipeline.
func (p *RAGPipeline) ProcessAll(ctx context.Context, articles []model.Article) ([]RAGResult, time.Duration) {
	start := time.Now()

	artCh := p.seedChannel(articles)
	chunkCh := p.runChunker(ctx, artCh)
	embeddedCh := p.runEmbedder(ctx, chunkCh)
	readyCh := p.runCollector(embeddedCh)
	resultsCh := p.runGenerator(ctx, readyCh, len(articles))

	var results []RAGResult
	for r := range resultsCh {
		results = append(results, r)
	}
	return results, time.Since(start)
}

func (p *RAGPipeline) seedChannel(articles []model.Article) <-chan model.Article {
	ch := make(chan model.Article, len(articles))
	for _, a := range articles {
		ch <- a
	}
	close(ch)
	return ch
}

func (p *RAGPipeline) runChunker(ctx context.Context, in <-chan model.Article) <-chan Chunk {
	out := make(chan Chunk, p.ChunksPerDoc*cap(in)+1)
	var wg sync.WaitGroup

	for w := 0; w < p.ChunkWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for article := range in {
				if ctx.Err() != nil {
					return
				}
				fmt.Printf("[chunk] article %d → %d chunks\n", article.ID, p.ChunksPerDoc)
				for i := 0; i < p.ChunksPerDoc; i++ {
					out <- Chunk{
						ArticleID: article.ID,
						ChunkID:   i,
						Text:      fmt.Sprintf("Chunk %d of article %d", i, article.ID),
					}
				}
			}
		}()
	}

	go func() { wg.Wait(); close(out) }()
	return out
}

func (p *RAGPipeline) runEmbedder(ctx context.Context, in <-chan Chunk) <-chan Chunk {
	out := make(chan Chunk, cap(in)+1)
	var wg sync.WaitGroup

	for w := 0; w < p.EmbedWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for chunk := range in {
				if ctx.Err() != nil {
					out <- chunk
					continue
				}
				articleCtx, cancel := context.WithTimeout(ctx, p.Timeout)
				if err := p.llm.Call(articleCtx, "Embed", chunk.ArticleID); err != nil {
					cancel()
					out <- chunk // pass through without embedding on error
					continue
				}
				cancel()
				chunk.Embedding = []float32{0.1, 0.2, 0.3} // simulated
				fmt.Printf("[embed] article %d chunk %d\n", chunk.ArticleID, chunk.ChunkID)
				out <- chunk
			}
		}()
	}

	go func() { wg.Wait(); close(out) }()
	return out
}

// runCollector groups embedded chunks by article and emits each complete set
// as soon as ChunksPerDoc chunks have arrived — without waiting for all
// articles' embeds to finish.
func (p *RAGPipeline) runCollector(in <-chan Chunk) <-chan articleChunks {
	out := make(chan articleChunks, 8)
	go func() {
		defer close(out)
		chunksByArticle := make(map[int][]Chunk)
		for chunk := range in {
			id := chunk.ArticleID
			chunksByArticle[id] = append(chunksByArticle[id], chunk)
			if len(chunksByArticle[id]) == p.ChunksPerDoc {
				out <- articleChunks{ArticleID: id, Chunks: chunksByArticle[id]}
				delete(chunksByArticle, id)
			}
		}
	}()
	return out
}

func (p *RAGPipeline) runGenerator(ctx context.Context, in <-chan articleChunks, articleCount int) <-chan RAGResult {
	out := make(chan RAGResult, articleCount)
	var wg sync.WaitGroup

	for w := 0; w < p.GenWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ready := range in {
				if ctx.Err() != nil {
					out <- RAGResult{ArticleID: ready.ArticleID, Err: ctx.Err()}
					continue
				}

				articleCtx, cancel := context.WithTimeout(ctx, p.Timeout)
				if err := p.llm.Call(articleCtx, "Generate", ready.ArticleID); err != nil {
					cancel()
					out <- RAGResult{ArticleID: ready.ArticleID, Err: err, ChunkCount: len(ready.Chunks)}
					continue
				}
				cancel()

				fmt.Printf("[generate] article %d answer from %d chunks\n", ready.ArticleID, len(ready.Chunks))
				out <- RAGResult{
					ArticleID:  ready.ArticleID,
					Chunks:     ready.Chunks,
					Answer:     fmt.Sprintf("RAG answer for article %d using %d chunks", ready.ArticleID, len(ready.Chunks)),
					ChunkCount: len(ready.Chunks),
				}
			}
		}()
	}

	go func() { wg.Wait(); close(out) }()
	return out
}

// GenerateArticles produces n dummy articles.
func GenerateArticles(n int) []model.Article {
	articles := make([]model.Article, n)
	for i := range articles {
		articles[i] = model.Article{
			ID:     i + 1,
			Title:  fmt.Sprintf("Breaking News %d", i+1),
			Source: "NewsWire",
		}
	}
	return articles
}
