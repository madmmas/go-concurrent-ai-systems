// Command news-processor is the runnable entry point for Part 2.
//
// Run it and compare total time against Part 1:
//
//	cd arc-1-foundations/part-01-sequential && go run ./cmd/news-processor
//	cd arc-1-foundations/part-02-goroutines  && go run ./cmd/news-processor
//
// The time should collapse from ~30s to ~1-2s for 10 articles.
// Also run with -race to observe the data race introduced in this part:
//
//	go run -race ./cmd/news-processor
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/moinuddin/go-concurrent-ai-systems/arc-1-foundations/part-02-goroutines/internal/pipeline"
	"github.com/moinuddin/go-concurrent-ai-systems/arc-1-foundations/part-02-goroutines/internal/simulator"
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

	fmt.Printf("Starting concurrent pipeline — %d articles, 3 AI tasks each\n", *n)
	fmt.Println("Note: run with -race to see the data race introduced in this part")
	fmt.Println("─────────────────────────────────────────────────────────")

	results, duration := proc.ProcessAll(articles)

	fmt.Println("\n═════════════════════════════════════════════════════════")
	fmt.Printf("Processed : %d articles\n", len(results))
	fmt.Printf("Total time: %v\n", duration.Round(1_000_000))
	fmt.Println("═════════════════════════════════════════════════════════")
	fmt.Println("\nCompare this against Part 1 — same work, fraction of the time.")
	fmt.Println("But run with -race. There is a bug hiding in this code.")
}
