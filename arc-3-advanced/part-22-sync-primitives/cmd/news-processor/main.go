// Command news-processor — Part 22: sync.Once, sync.Map, sync.Pool, atomic.
//
//	# All unique URLs — each article gets its own LLM call
//	go run ./cmd/news-processor -articles=9 -workers=3
//
//	# 3 unique URLs repeated — sync.Map cache reduces LLM calls
//	go run ./cmd/news-processor -articles=9 -workers=3 -unique-urls=3
package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-22-sync-primitives/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-22-sync-primitives/internal/simulator"
)

func main() {
	n          := flag.Int("articles", 9, "number of articles")
	w          := flag.Int("workers", 3, "workers")
	uniqueURLs := flag.Int("unique-urls", 0, "unique URLs (0=all unique)")
	flag.Parse()

	factory := pipeline.NewFactory(simulator.DefaultConfig)
	pool    := pipeline.NewPrimitivesPool(factory, *w)

	var arts []pipeline.ArticleItem
	if *uniqueURLs > 0 {
		arts = pipeline.MakeArticlesWithDuplicates(*n, *uniqueURLs)
		fmt.Printf("Part 22 — %d articles, %d unique URLs\n", *n, *uniqueURLs)
	} else {
		arts = pipeline.MakeArticles(*n)
		fmt.Printf("Part 22 — %d articles, all unique URLs\n", *n)
	}

	fmt.Printf("Primitives: sync.Once + sync.Map + sync.Pool + atomic\n")
	fmt.Println("─────────────────────────────────────────────────────────")

	results, dur := pool.ProcessAll(context.Background(), arts)

	calls, success, failed := pool.Stats()
	fmt.Printf("\nResults   : %d articles processed\n", len(results))
	fmt.Printf("Cache size: %d unique URLs\n", pool.CacheSize())
	fmt.Printf("LLM calls : %d (success=%d failed=%d)\n", calls, success, failed)
	fmt.Printf("Duration  : %v\n", dur.Round(time.Millisecond))
}
