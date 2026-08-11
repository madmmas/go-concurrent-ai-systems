// Command news-processor is the runnable entry point for Part 3.
//
// First, see the race condition:
//
//	go run -race ./broken
//
// Then see the good lock (~3.4s) vs bad lock (~32s):
//
//	go run ./cmd/news-processor -mode=good
//	go run ./cmd/news-processor -mode=bad
//
// The -race flag should report nothing for either version.
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
	mode := flag.String("mode", "good", "good (lock around append) or bad (lock around LLM)")
	flag.Parse()

	if *n <= 0 {
		fmt.Fprintln(os.Stderr, "error: -articles must be a positive integer")
		os.Exit(1)
	}

	llm := simulator.New(simulator.DefaultConfig)
	articles := pipeline.GenerateArticles(*n)

	var (
		results  []model.AIResult
		duration time.Duration
	)

	switch *mode {
	case "good":
		fmt.Printf("Good lock — mutex around append only — %d articles\n", *n)
		fmt.Println("LLM calls run concurrently; expect ~3–4s for 10 articles.")
		fmt.Println("─────────────────────────────────────────────────────────")
		results, duration = pipeline.New(llm).ProcessAll(articles)

	case "bad":
		fmt.Printf("Bad lock — mutex around entire article (incl. LLM) — %d articles\n", *n)
		fmt.Println("LLM calls serialize; expect ~30s for 10 articles.")
		fmt.Println("─────────────────────────────────────────────────────────")
		results, duration = pipeline.NewBadLock(llm).ProcessAll(articles)

	default:
		fmt.Fprintf(os.Stderr, "unknown mode: %s\n", *mode)
		os.Exit(1)
	}

	fmt.Println("\n═════════════════════════════════════════════════════════")
	fmt.Printf("Mode      : %s\n", *mode)
	fmt.Printf("Processed : %d articles (expected %d)\n", len(results), *n)
	fmt.Printf("Total time: %v\n", duration.Round(time.Millisecond))
	fmt.Println("═════════════════════════════════════════════════════════")
}
