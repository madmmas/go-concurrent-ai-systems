// Command news-processor — Part 14: Rate Limiting.
//
//	# No rate limit — baseline
//	go run ./cmd/news-processor -articles=10 -workers=5 -rate=1000
//
//	# Strict rate limit: 3 calls/second
//	go run ./cmd/news-processor -articles=10 -workers=5 -rate=3
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-14-rate-limiting/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-14-rate-limiting/internal/simulator"
)

func main() {
	n     := flag.Int("articles", 5, "number of articles")
	w     := flag.Int("workers", 5, "workers")
	rate  := flag.Float64("rate", 3.0, "max LLM calls per second")
	burst := flag.Int("burst", 3, "burst size")
	flag.Parse()

	if *n <= 0 || *w <= 0 {
		fmt.Fprintln(os.Stderr, "articles and workers must be > 0")
		os.Exit(1)
	}

	pool    := pipeline.New(simulator.New(simulator.DefaultConfig), *w, 5*time.Second, *rate, *burst)
	results, dur := pool.ProcessAll(context.Background(), pipeline.GenerateArticles(*n))

	ok, fail := 0, 0
	for _, r := range results {
		if r.Err != nil { fail++ } else { ok++ }
	}
	fmt.Printf("\nRate limit=%.0f/s burst=%d | %d articles | Succeeded:%d Failed:%d | %v\n",
		*rate, *burst, *n, ok, fail, dur.Round(time.Millisecond))
}
