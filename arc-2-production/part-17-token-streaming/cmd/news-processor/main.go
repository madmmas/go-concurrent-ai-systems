// Command news-processor — Part 17: Token Streaming.
//
//	go run ./cmd/news-processor -articles=3 -workers=2
package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-17-token-streaming/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-17-token-streaming/internal/simulator"
)

func main() {
	n := flag.Int("articles", 2, "number of articles")
	w := flag.Int("workers", 2, "workers")
	flag.Parse()

	pool := pipeline.New(simulator.New(simulator.DefaultConfig), *w, 10*time.Second)
	fmt.Printf("Token streaming: %d articles, %d workers\n", *n, *w)
	fmt.Println("Watch tokens arrive incrementally per article.")
	fmt.Println("─────────────────────────────────────────────────────────")

	results, dur := pool.ProcessAll(context.Background(), pipeline.GenerateArticles(*n))

	fmt.Println("\n═════════════════════════════════════════════════════════")
	for _, r := range results {
		if r.Err != nil {
			fmt.Printf("Article %d: ERROR %v\n", r.ArticleID, r.Err)
		} else {
			fmt.Printf("Article %d: %d tokens in %v\n  %s\n",
				r.ArticleID, r.TokenCount, r.Duration.Round(time.Millisecond), r.FullText)
		}
	}
	fmt.Printf("Total: %v\n", dur.Round(time.Millisecond))
}
