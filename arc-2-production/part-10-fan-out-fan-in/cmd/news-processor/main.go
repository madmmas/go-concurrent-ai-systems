// Command news-processor — Part 10: Fan-Out / Fan-In.
//
// Compare against Part 9's sequential-tasks worker pool:
//
//	cd arc-1-foundations/part-09-cancellation
//	go run ./cmd/news-processor -articles=5 -workers=3
//
//	cd arc-2-production/part-10-fan-out-fan-in
//	go run ./cmd/news-processor -articles=5 -workers=3
//
// Part 9: ~3s per article (tasks serial inside each worker)
// Part 10: ~1s per article (tasks concurrent inside each worker)
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-10-fan-out-fan-in/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-10-fan-out-fan-in/internal/simulator"
)

func main() {
	n       := flag.Int("articles", 3, "number of articles")
	w       := flag.Int("workers", 2, "number of worker goroutines")
	timeout := flag.Duration("timeout", 5*time.Second, "per-article timeout")
	flag.Parse()

	if *n <= 0 || *w <= 0 {
		fmt.Fprintln(os.Stderr, "articles and workers must be > 0")
		os.Exit(1)
	}

	llm  := simulator.New(simulator.DefaultConfig)
	pool := pipeline.New(llm, *w, *timeout)
	arts := pipeline.GenerateArticles(*n)

	fmt.Printf("Fan-Out pipeline: %d articles, %d workers\n", *n, *w)
	fmt.Println("Each article fans out 3 AI tasks concurrently.")
	fmt.Println("─────────────────────────────────────────────────────────")

	results, dur := pool.ProcessAll(context.Background(), arts)

	ok, fail := 0, 0
	for _, r := range results {
		if r.Err != nil { fail++ } else { ok++ }
	}

	fmt.Println("\n═════════════════════════════════════════════════════════")
	fmt.Printf("Processed : %d articles\n", len(results))
	fmt.Printf("Succeeded : %d\n", ok)
	fmt.Printf("Failed    : %d\n", fail)
	fmt.Printf("Duration  : %v\n", dur.Round(time.Millisecond))
	fmt.Println("═════════════════════════════════════════════════════════")
}
