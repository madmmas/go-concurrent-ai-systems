// Command news-processor demonstrates per-article timeouts using context.
//
// Run with a healthy pipeline (no timeouts):
//
//	go run ./cmd/news-processor -articles=10 -workers=5 -timeout=5s
//
// Run with an unreliable provider (20% timeout rate):
//
//	go run ./cmd/news-processor -articles=10 -workers=5 -timeout=2s -unreliable
//
// Watch: articles that exceed their deadline produce "failed" results rather
// than blocking the worker indefinitely. The pipeline always finishes.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-1-foundations/part-08-context-timeouts/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-1-foundations/part-08-context-timeouts/internal/simulator"
)

func main() {
	n          := flag.Int("articles", 10, "number of articles")
	w          := flag.Int("workers", 5, "number of workers")
	timeout    := flag.Duration("timeout", 4*time.Second, "per-article timeout")
	unreliable := flag.Bool("unreliable", false, "simulate 20% timeout rate")
	flag.Parse()

	if *n <= 0 || *w <= 0 {
		fmt.Fprintln(os.Stderr, "articles and workers must be > 0")
		os.Exit(1)
	}

	cfg := simulator.DefaultConfig
	if *unreliable {
		cfg = simulator.UnreliableConfig
		fmt.Println("⚠️  Unreliable mode: 20% of calls will time out")
	}

	llm := simulator.New(cfg)
	pool := pipeline.New(llm, *w, *timeout)
	articles := pipeline.GenerateArticles(*n)

	fmt.Printf("Pipeline: %d articles, %d workers, %v per-article timeout\n",
		*n, *w, *timeout)
	fmt.Println("─────────────────────────────────────────────────────────")

	results, duration := pool.ProcessAll(context.Background(), articles)

	succeeded := 0
	failed := 0
	for _, r := range results {
		if r.Err != nil {
			failed++
		} else {
			succeeded++
		}
	}

	fmt.Println("\n═════════════════════════════════════════════════════════")
	fmt.Printf("Total     : %d articles\n", len(results))
	fmt.Printf("Succeeded : %d\n", succeeded)
	fmt.Printf("Failed    : %d\n", failed)
	fmt.Printf("Duration  : %v\n", duration.Round(time.Millisecond))
	fmt.Println("═════════════════════════════════════════════════════════")

	if failed > 0 {
		fmt.Println("\nFailed articles had their context deadline exceeded.")
		fmt.Println("In Arc 2 (Part 13), retries with exponential backoff handle these failures.")
	}
}
