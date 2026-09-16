// Command news-processor — Part 23: Semaphore Pattern.
//
//	# 10 workers, only 2 can embed at once
//	go run ./cmd/news-processor -articles=8 -workers=10 -embed-slots=2
//
//	# Compare: no concurrency limit on embed
//	go run ./cmd/news-processor -articles=8 -workers=10 -embed-slots=10
package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-23-semaphore/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-23-semaphore/internal/simulator"
)

func main() {
	n         := flag.Int("articles", 8, "number of articles")
	w         := flag.Int("workers", 10, "total worker goroutines")
	embedSlots := flag.Int("embed-slots", 2, "max concurrent embed calls")
	flag.Parse()

	pool := pipeline.New(simulator.New(simulator.DefaultConfig), *w, *embedSlots, 5*time.Second)
	arts := pipeline.GenerateArticles(*n)

	fmt.Printf("Semaphore pool: %d workers, %d embed slots\n", *w, *embedSlots)
	fmt.Println("Workers run freely; embed calls limited by semaphore.")
	fmt.Println("─────────────────────────────────────────────────────────")

	results, dur := pool.ProcessAll(context.Background(), arts)

	ok, fail := 0, 0
	for _, r := range results {
		if r.Err != nil { fail++ } else { ok++ }
	}
	fmt.Printf("\nProcessed:%d Succeeded:%d Failed:%d Duration:%v\n",
		len(results), ok, fail, dur.Round(time.Millisecond))
}
