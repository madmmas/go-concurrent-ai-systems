// Command news-processor — Part 12: errgroup.
//
//	go run ./cmd/news-processor -articles=3 -workers=2
//
//	# Failure demo — first 503 cancels siblings (~1ms)
//	go run ./cmd/news-processor -articles=3 -workers=2 -error-rate=1.0
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-12-errgroup/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-12-errgroup/internal/simulator"
)

func main() {
	n       := flag.Int("articles", 3, "number of articles")
	w       := flag.Int("workers", 2, "number of workers")
	errRate := flag.Float64("error-rate", 0.0, "server error rate (0.0-1.0); 1.0 demos fast cancel")
	flag.Parse()

	if *n <= 0 || *w <= 0 {
		fmt.Fprintln(os.Stderr, "articles and workers must be > 0")
		os.Exit(1)
	}

	cfg := simulator.DefaultConfig
	cfg.Failure = simulator.FailureProfile{ServerErrRate: *errRate}

	pool := pipeline.New(simulator.New(cfg), *w, 5*time.Second)
	arts := pipeline.GenerateArticles(*n)

	fmt.Printf("errgroup pipeline: %d articles, %d workers, error-rate=%.0f%%\n",
		*n, *w, *errRate*100)
	fmt.Println("─────────────────────────────────────────────────────────")

	results, dur := pool.ProcessAll(context.Background(), arts)

	ok, fail := 0, 0
	for _, r := range results {
		if r.Err != nil {
			fail++
		} else {
			ok++
		}
	}
	fmt.Printf("Succeeded: %d | Failed: %d | Duration: %v\n", ok, fail, dur.Round(time.Millisecond))
}
