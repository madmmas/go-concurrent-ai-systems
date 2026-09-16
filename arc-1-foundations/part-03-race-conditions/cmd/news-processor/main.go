// Command news-processor is the runnable entry point for Part 3.
//
//	go run -race ./broken
//	go run ./cmd/news-processor -mode=good
//	go run ./cmd/news-processor -mode=bad
//	go run ./cmd/news-processor -mode=cache -articles=50
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-1-foundations/part-03-race-conditions/internal/model"
	"github.com/madmmas/go-concurrent-ai-systems/arc-1-foundations/part-03-race-conditions/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-1-foundations/part-03-race-conditions/internal/simulator"
)

func main() {
	n    := flag.Int("articles", 10, "number of articles to process")
	mode := flag.String("mode", "good", "good | bad | cache")
	flag.Parse()

	if *n <= 0 {
		fmt.Fprintln(os.Stderr, "error: -articles must be a positive integer")
		os.Exit(1)
	}

	llm := simulator.New(simulator.DefaultConfig)

	var (
		results  []model.AIResult
		duration time.Duration
		cacheN   int
	)

	switch *mode {
	case "good":
		fmt.Printf("Good lock — mutex around append only — %d articles\n", *n)
		fmt.Println("LLM calls run concurrently; expect ~3–4s for 10 articles.")
		fmt.Println("─────────────────────────────────────────────────────────")
		results, duration = pipeline.New(llm).ProcessAll(pipeline.GenerateArticles(*n))

	case "bad":
		fmt.Printf("Bad lock — mutex around entire article (incl. LLM) — %d articles\n", *n)
		fmt.Println("LLM calls serialize; expect ~30s for 10 articles.")
		fmt.Println("─────────────────────────────────────────────────────────")
		results, duration = pipeline.NewBadLock(llm).ProcessAll(pipeline.GenerateArticles(*n))

	case "cache":
		// Many articles, few unique URLs → most requests are concurrent cache hits.
		unique := 5
		if *n < unique {
			unique = *n
		}
		fmt.Printf("RWMutex cache — %d articles, %d unique URLs\n", *n, unique)
		fmt.Println("Many concurrent readers (RLock); rare writers (Lock).")
		fmt.Println("─────────────────────────────────────────────────────────")
		proc := pipeline.NewCached(llm)
		results, duration = proc.ProcessAll(pipeline.GenerateArticlesWithDuplicates(*n, unique))
		cacheN = proc.CacheSize()

	default:
		fmt.Fprintf(os.Stderr, "unknown mode: %s (want good, bad, or cache)\n", *mode)
		os.Exit(1)
	}

	fmt.Println("\n═════════════════════════════════════════════════════════")
	fmt.Printf("Mode      : %s\n", *mode)
	fmt.Printf("Processed : %d articles (expected %d)\n", len(results), *n)
	if *mode == "cache" {
		fmt.Printf("Cache size: %d unique URLs\n", cacheN)
	}
	fmt.Printf("Total time: %v\n", duration.Round(time.Millisecond))
	fmt.Println("═════════════════════════════════════════════════════════")
}
