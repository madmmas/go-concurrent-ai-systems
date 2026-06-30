// Command news-processor is the runnable entry point for Part 5.
//
// Experiment with worker count to see the effect on throughput:
//
//	go run ./cmd/news-processor -articles=20 -workers=1
//	go run ./cmd/news-processor -articles=20 -workers=5
//	go run ./cmd/news-processor -articles=20 -workers=20
//
// With 1 worker, time grows linearly (like Part 1).
// With 20 workers, time collapses (like Parts 2-4).
// The sweet spot is somewhere in between — bounded by your LLM provider's
// rate limits and your process's memory budget.
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/moinuddin/go-concurrent-ai-systems/arc-1-foundations/part-05-worker-pools/internal/pipeline"
	"github.com/moinuddin/go-concurrent-ai-systems/arc-1-foundations/part-05-worker-pools/internal/simulator"
)

func main() {
	n := flag.Int("articles", 20, "number of articles to process")
	w := flag.Int("workers", 5, "number of concurrent worker goroutines")
	flag.Parse()

	if *n <= 0 {
		fmt.Fprintln(os.Stderr, "error: -articles must be a positive integer")
		os.Exit(1)
	}
	if *w <= 0 {
		fmt.Fprintln(os.Stderr, "error: -workers must be a positive integer")
		os.Exit(1)
	}

	llm := simulator.New(simulator.DefaultConfig)
	pool := pipeline.New(llm, *w)
	articles := pipeline.GenerateArticles(*n)

	fmt.Printf("Worker pool pipeline — %d articles, %d workers\n", *n, *w)
	fmt.Printf("Max concurrent goroutines: %d (regardless of article count)\n", *w)
	fmt.Println("─────────────────────────────────────────────────────────")

	results, duration := pool.ProcessAll(articles)

	fmt.Println("\n═════════════════════════════════════════════════════════")
	fmt.Printf("Workers   : %d\n", *w)
	fmt.Printf("Processed : %d articles\n", len(results))
	fmt.Printf("Total time: %v\n", duration.Round(time.Millisecond))
	fmt.Println("═════════════════════════════════════════════════════════")
	fmt.Printf("\nTry: -workers=1  to see sequential behaviour\n")
	fmt.Printf("Try: -workers=%d to see maximum parallelism\n", *n)
}
