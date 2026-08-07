// Command news-processor — Part 18: Goroutine Leak Detection.
//
//	go run ./cmd/news-processor -articles=10 -workers=3
//
// Watch the goroutine count before and after — it should return to baseline.
// In production, use pprof: go tool pprof http://localhost:6060/debug/pprof/goroutine
package main

import (
	"context"
	"flag"
	"fmt"
	"runtime"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-18-goroutine-leaks/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-18-goroutine-leaks/internal/simulator"
)

func main() {
	n := flag.Int("articles", 10, "number of articles")
	w := flag.Int("workers", 3, "workers")
	flag.Parse()

	pool := pipeline.New(simulator.New(simulator.DefaultConfig), *w, 5*time.Second)
	arts := pipeline.GenerateArticles(*n)

	before := runtime.NumGoroutine()
	fmt.Printf("Goroutines before: %d\n", before)

	results, dur := pool.ProcessAll(context.Background(), arts)

	time.Sleep(100 * time.Millisecond) // let goroutines exit
	after := runtime.NumGoroutine()

	fmt.Printf("Goroutines after:  %d (delta: %+d)\n", after, after-before)
	fmt.Printf("Processed: %d articles in %v\n", len(results), dur.Round(time.Millisecond))
	if after <= before+2 {
		fmt.Println("✓ No goroutine leak detected")
	} else {
		fmt.Printf("✗ Possible leak: %d goroutines remain\n", after-before)
	}
}
