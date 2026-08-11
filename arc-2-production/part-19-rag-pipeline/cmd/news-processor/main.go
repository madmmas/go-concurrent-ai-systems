// Command news-processor — Part 19: Concurrent RAG Pipeline (flagship).
//
//	go run ./cmd/news-processor -articles=4 -chunks=3
package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-19-rag-pipeline/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-19-rag-pipeline/internal/simulator"
)

func main() {
	n      := flag.Int("articles", 4, "number of articles")
	chunks := flag.Int("chunks", 3, "chunks per article")
	flag.Parse()

	rag := pipeline.New(
		simulator.New(simulator.DefaultConfig),
		5, // chunk workers
		3, // embed workers
		3, // generation workers
		*chunks,
		5*time.Second,
	)

	fmt.Printf("RAG Pipeline: %d articles × %d chunks each\n", *n, *chunks)
	fmt.Printf("Stages: chunk(5) → embed(3) → generate(3)\n")
	fmt.Println("─────────────────────────────────────────────────────────")

	results, dur := rag.ProcessAll(context.Background(), pipeline.GenerateArticles(*n))

	fmt.Println("\n═════════════════════════════════════════════════════════")
	for _, r := range results {
		if r.Err != nil {
			fmt.Printf("Article %d: ERROR %v\n", r.ArticleID, r.Err)
		} else {
			fmt.Printf("Article %d: %d chunks → %q\n", r.ArticleID, r.ChunkCount, r.Answer)
		}
	}
	fmt.Printf("Total: %v\n", dur.Round(time.Millisecond))
}
