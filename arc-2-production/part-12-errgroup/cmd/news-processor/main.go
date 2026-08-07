// Command news-processor — Part 12: errgroup.
//
//	go run ./cmd/news-processor -articles=6 -workers=3
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
	n := flag.Int("articles", 6, "number of articles")
	w := flag.Int("workers", 3, "number of workers")
	flag.Parse()

	if *n <= 0 || *w <= 0 {
		fmt.Fprintln(os.Stderr, "articles and workers must be > 0")
		os.Exit(1)
	}

	pool    := pipeline.New(simulator.New(simulator.DefaultConfig), *w, 5*time.Second)
	arts    := pipeline.GenerateArticles(*n)
	results, dur := pool.ProcessAll(context.Background(), arts)

	ok, fail := 0, 0
	for _, r := range results {
		if r.Err != nil { fail++ } else { ok++ }
	}
	fmt.Printf("errgroup pipeline: %d articles, %d workers\n", *n, *w)
	fmt.Printf("Succeeded: %d | Failed: %d | Duration: %v\n", ok, fail, dur.Round(time.Millisecond))
}
