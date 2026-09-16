// Command news-processor — Part 16: Backpressure.
//
//	# Observe backpressure with slow workers and fast producer
//	go run ./cmd/news-processor -articles=15 -workers=2 -queue=3
package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-16-backpressure/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-16-backpressure/internal/simulator"
)

func main() {
	n     := flag.Int("articles", 8, "number of articles")
	w     := flag.Int("workers", 2, "workers (consumers)")
	queue := flag.Int("queue", 2, "bounded queue depth")
	flag.Parse()

	pool := pipeline.New(simulator.New(simulator.DefaultConfig), *w, *queue, 5*time.Second)
	fmt.Printf("Backpressure pool: %d articles, %d workers, queue=%d\n", *n, *w, *queue)
	fmt.Println("Producer blocks when queue is full — that IS backpressure.")
	fmt.Println("─────────────────────────────────────────────────────────")

	results, dur := pool.ProcessWithProducer(context.Background(), pipeline.GenerateArticles(*n), 0)

	ok, fail := 0, 0
	for _, r := range results {
		if r.Err != nil { fail++ } else { ok++ }
	}
	fmt.Printf("\nProcessed:%d Succeeded:%d Failed:%d Duration:%v\n",
		len(results), ok, fail, dur.Round(time.Millisecond))
}
