// Command news-processor — Part 24: Singleflight.
//
//	go run ./cmd/news-processor -articles=20 -workers=20 -unique-urls=1
//	go run ./cmd/news-processor -articles=20 -workers=10 -unique-urls=5
package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-24-singleflight/internal/model"
	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-24-singleflight/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-24-singleflight/internal/simulator"
)

func main() {
	n          := flag.Int("articles", 10, "number of articles")
	w          := flag.Int("workers", 5, "workers")
	uniqueURLs := flag.Int("unique-urls", 0, "unique URLs (0=all unique)")
	flag.Parse()

	pool := pipeline.New(simulator.New(simulator.DefaultConfig), *w, 5*time.Second)

	var arts []model.Article
	if *uniqueURLs > 0 {
		arts = pipeline.GenerateArticlesWithDuplicates(*n, *uniqueURLs)
		fmt.Printf("Singleflight: %d articles, %d unique URLs\n", *n, *uniqueURLs)
	} else {
		arts = pipeline.GenerateArticles(*n)
		fmt.Printf("Singleflight: %d articles, all unique URLs\n", *n)
	}
	fmt.Println("─────────────────────────────────────────────────────────")

	results, dur := pool.ProcessAll(context.Background(), arts)

	ok := 0
	for _, r := range results { if r.Err == nil { ok++ } }
	hits, misses := pool.CacheStats()

	fmt.Printf("\nProcessed :%d | Succeeded:%d\n", len(results), ok)
	fmt.Printf("Cache     : hits=%d misses=%d size=%d\n", hits, misses, pool.CacheSize())
	fmt.Printf("Duration  : %v\n", dur.Round(time.Millisecond))
}
