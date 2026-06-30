// Command news-processor is the runnable entry point for Part 3.
//
// First, see the race condition:
//
//	go run -race ./broken
//
// Then see it fixed:
//
//	go run -race ./cmd/news-processor
//
// The -race flag should report nothing for this version.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/moinuddin/go-concurrent-ai-systems/arc-1-foundations/part-03-race-conditions/internal/pipeline"
	"github.com/moinuddin/go-concurrent-ai-systems/arc-1-foundations/part-03-race-conditions/internal/simulator"
)

func main() {
	n := flag.Int("articles", 10, "number of articles to process")
	flag.Parse()

	if *n <= 0 {
		fmt.Fprintln(os.Stderr, "error: -articles must be a positive integer")
		os.Exit(1)
	}

	llm := simulator.New(simulator.DefaultConfig)
	proc := pipeline.New(llm)
	articles := pipeline.GenerateArticles(*n)

	fmt.Printf("Starting mutex-safe concurrent pipeline — %d articles\n", *n)
	fmt.Println("Run with -race: the race detector should stay silent.")
	fmt.Println("─────────────────────────────────────────────────────────")

	results, duration := proc.ProcessAll(articles)

	fmt.Println("\n═════════════════════════════════════════════════════════")
	fmt.Printf("Processed : %d articles (expected %d)\n", len(results), *n)
	fmt.Printf("Total time: %v\n", duration.Round(1_000_000))
	fmt.Println("═════════════════════════════════════════════════════════")
}
