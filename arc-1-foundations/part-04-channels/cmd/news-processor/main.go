// Command news-processor is the runnable entry point for Part 4.
//
// Run with -race — the race detector should stay completely silent:
//
//	go run -race ./cmd/news-processor
//
// Compare against Part 3 — same performance, no mutex required.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/moinuddin/go-concurrent-ai-systems/arc-1-foundations/part-04-channels/internal/pipeline"
	"github.com/moinuddin/go-concurrent-ai-systems/arc-1-foundations/part-04-channels/internal/simulator"
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

	fmt.Printf("Starting channel-based pipeline — %d articles\n", *n)
	fmt.Println("No mutex. No shared state. Run with -race — it stays silent.")
	fmt.Println("─────────────────────────────────────────────────────────")

	results, duration := proc.ProcessAll(articles)

	fmt.Println("\n═════════════════════════════════════════════════════════")
	fmt.Printf("Processed : %d articles (expected %d)\n", len(results), *n)
	fmt.Printf("Total time: %v\n", duration.Round(1_000_000))
	fmt.Println("═════════════════════════════════════════════════════════")
	fmt.Println("\nNext problem: what happens with 100,000 articles?")
	fmt.Println("One goroutine per article does not scale. See Part 5.")
}
