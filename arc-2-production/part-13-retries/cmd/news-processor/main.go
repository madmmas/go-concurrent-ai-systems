// Command news-processor — Part 13: Retries with exponential backoff.
//
//	go run ./cmd/news-processor -articles=10 -workers=3
//	go run ./cmd/news-processor -articles=10 -workers=3 -rate-limit=0.3
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-13-retries/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-13-retries/internal/simulator"
)

func main() {
	n         := flag.Int("articles", 10, "number of articles")
	w         := flag.Int("workers", 3, "workers")
	rateLimit := flag.Float64("rate-limit", 0.2, "probability of rate limit error (0.0-1.0)")
	flag.Parse()

	if *n <= 0 || *w <= 0 {
		fmt.Fprintln(os.Stderr, "articles and workers must be > 0")
		os.Exit(1)
	}

	cfg := simulator.Config{
		MinLatency: 200 * time.Millisecond,
		MaxLatency: 600 * time.Millisecond,
		Failure: simulator.FailureProfile{
			RateLimitRate: *rateLimit,
			ServerErrRate: 0.05,
		},
	}
	pool := pipeline.New(simulator.New(cfg), *w, 3*time.Second, pipeline.DefaultRetryConfig)
	results, dur := pool.ProcessAll(context.Background(), pipeline.GenerateArticles(*n))

	ok, fail, dead := 0, 0, 0
	totalRetries := 0
	for _, r := range results {
		totalRetries += r.Result.Retries
		if r.DeadLetter { dead++; fail++ } else if r.Result.Err != nil { fail++ } else { ok++ }
	}

	fmt.Printf("\nRetry pipeline: %d articles, rate-limit=%.0f%%\n", *n, *rateLimit*100)
	fmt.Printf("Succeeded: %d | Failed: %d | Dead letter: %d\n", ok, fail, dead)
	fmt.Printf("Total retries: %d | Duration: %v\n", totalRetries, dur.Round(time.Millisecond))
}
