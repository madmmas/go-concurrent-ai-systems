// Command news-processor — Part 15: Circuit Breaker.
//
//	# Healthy provider
//	go run ./cmd/news-processor -articles=10 -workers=3
//
//	# Unhealthy provider — watch the circuit open
//	go run ./cmd/news-processor -articles=20 -workers=3 -error-rate=0.6
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-15-circuit-breaker/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-15-circuit-breaker/internal/simulator"
)

func main() {
	n         := flag.Int("articles", 20, "number of articles")
	w         := flag.Int("workers", 3, "workers")
	errRate   := flag.Float64("error-rate", 0.0, "server error rate (0.0-1.0)")
	threshold := flag.Int("threshold", 3, "failures before circuit opens")
	cooldown  := flag.Duration("cooldown", 2*time.Second, "cooldown before half-open")
	flag.Parse()

	cfg := simulator.Config{
		MinLatency: 100 * time.Millisecond,
		MaxLatency: 300 * time.Millisecond,
		Failure:    simulator.FailureProfile{ServerErrRate: *errRate},
	}
	cb   := pipeline.NewCircuitBreaker(*threshold, *cooldown)
	pool := pipeline.New(simulator.New(cfg), *w, 2*time.Second, cb)

	fmt.Printf("Circuit breaker: threshold=%d cooldown=%v error-rate=%.0f%%\n",
		*threshold, *cooldown, *errRate*100)

	results, dur := pool.ProcessAll(context.Background(), pipeline.GenerateArticles(*n))

	ok, fail, open := 0, 0, 0
	for _, r := range results {
		switch {
		case r.Err == nil:                        ok++
		case errors.Is(r.Err, pipeline.ErrCircuitOpen): open++; fail++
		default:                                   fail++
		}
	}
	fmt.Printf("\nSucceeded:%d | Failed:%d (circuit-open:%d) | %v\n", ok, fail, open, dur.Round(time.Millisecond))
}
