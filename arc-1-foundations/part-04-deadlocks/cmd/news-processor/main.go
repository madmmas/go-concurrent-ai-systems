// Command news-processor is the runnable entry point for Part 4.
//
// This part does NOT run the deadlock demos — those crash intentionally.
// Run them individually:
//
//	go run ./deadlocks/send-no-receive   # fatal: all goroutines asleep
//	go run ./deadlocks/circular-wait     # fatal: all goroutines asleep
//	go run ./deadlocks/forgotten-close   # fatal: all goroutines asleep
//	go run ./deadlocks/correct           # runs cleanly
//
// This binary runs the deadlock-safe pipeline to show correct behaviour:
//
//	go run ./cmd/news-processor
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-1-foundations/part-04-deadlocks/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-1-foundations/part-04-deadlocks/internal/simulator"
)

func main() {
	n := flag.Int("articles", 10, "number of articles")
	flag.Parse()

	if *n <= 0 {
		fmt.Fprintln(os.Stderr, "articles must be > 0")
		os.Exit(1)
	}

	llm := simulator.New(simulator.DefaultConfig)
	p := pipeline.New(llm)
	articles := pipeline.GenerateArticles(*n)

	fmt.Printf("Deadlock-safe pipeline — %d articles\n", *n)
	fmt.Println("─────────────────────────────────────────────────────────")

	results, duration := p.ProcessAll(articles)

	fmt.Println("\n═════════════════════════════════════════════════════════")
	fmt.Printf("Processed : %d articles\n", len(results))
	fmt.Printf("Duration  : %v\n", duration.Round(time.Millisecond))
	fmt.Println("═════════════════════════════════════════════════════════")
	fmt.Println("\nRun the deadlock demos to see what we prevented:")
	fmt.Println("  go run ./deadlocks/send-no-receive")
	fmt.Println("  go run ./deadlocks/circular-wait")
	fmt.Println("  go run ./deadlocks/forgotten-close")
}
