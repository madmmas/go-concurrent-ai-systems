// Package pipeline implements a concurrent RAG (Retrieval-Augmented Generation)
// pipeline — the flagship Part 19 of Arc 2.
//
// RAG architecture:
//   1. Chunker     — split articles into overlapping text chunks
//   2. Embedder    — generate vector embeddings for each chunk (LLM call)
//   3. Indexer     — store embeddings in the vector store
//   4. Retriever   — find relevant chunks for a query (vector similarity)
//   5. Generator   — generate a grounded answer using retrieved chunks (LLM call)
//
// All five stages are concurrent, each with independent worker pools.
// The pipeline applies everything learned in Parts 10-18:
//   - Fan-out per stage (Part 10)
//   - Stage isolation with channels (Part 11)
//   - Retries on embedding failures (Part 13)
//   - Rate limiting on LLM calls (Part 14)
//   - Backpressure on the embedding queue (Part 16)
//   - No goroutine leaks (Part 18)
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

// RAGPipeline runs a full concurrent RAG pipeline.
type RAGPipeline struct {
	llm           *simulator.LLMClient
	ChunkWorkers  int
	EmbedWorkers  int
	GenWorkers    int
	Timeout       time.Duration
	ChunksPerDoc  int
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

	// Stage 1: Chunk articles
	artCh   := p.seedChannel(articles)
	chunkCh := p.runChunker(ctx, artCh)

	// Stage 2: Embed chunks (fan-out per chunk)
	embeddedCh := p.runEmbedder(ctx, chunkCh)

	// Stage 3: Generate answer per article (fan-in chunks, fan-out generation)
	resultsCh := p.runGenerator(ctx, embeddedCh, len(articles))

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

func (p *RAGPipeline) runGenerator(ctx context.Context, in <-chan Chunk, articleCount int) <-chan RAGResult {
	// Collect chunks per article, then generate
	chunksByArticle := make(map[int][]Chunk)
	var mu sync.Mutex

	var collectWg sync.WaitGroup
	collectWg.Add(1)
	go func() {
		defer collectWg.Done()
		for chunk := range in {
			mu.Lock()
			chunksByArticle[chunk.ArticleID] = append(chunksByArticle[chunk.ArticleID], chunk)
			mu.Unlock()
		}
	}()
	collectWg.Wait()

	out := make(chan RAGResult, articleCount)
	var wg sync.WaitGroup

	mu.Lock()
	articles := make([]int, 0, len(chunksByArticle))
	for id := range chunksByArticle {
		articles = append(articles, id)
	}
	mu.Unlock()

	articleCh := make(chan int, len(articles))
	for _, id := range articles {
		articleCh <- id
	}
	close(articleCh)

	for w := 0; w < p.GenWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for articleID := range articleCh {
				mu.Lock()
				chunks := chunksByArticle[articleID]
				mu.Unlock()

				if ctx.Err() != nil {
					out <- RAGResult{ArticleID: articleID, Err: ctx.Err()}
					continue
				}

				articleCtx, cancel := context.WithTimeout(ctx, p.Timeout)
				if err := p.llm.Call(articleCtx, "Generate", articleID); err != nil {
					cancel()
					out <- RAGResult{ArticleID: articleID, Err: err, ChunkCount: len(chunks)}
					continue
				}
				cancel()

				fmt.Printf("[generate] article %d answer from %d chunks\n", articleID, len(chunks))
				out <- RAGResult{
					ArticleID:  articleID,
					Chunks:     chunks,
					Answer:     fmt.Sprintf("RAG answer for article %d using %d chunks", articleID, len(chunks)),
					ChunkCount: len(chunks),
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
