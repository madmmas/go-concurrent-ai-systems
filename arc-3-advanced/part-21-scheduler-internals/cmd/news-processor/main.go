// Command news-processor — Part 21: Go Scheduler Internals.
//
// Compares IO-bound vs CPU-bound workloads across different worker counts.
// Run with -mode=io or -mode=cpu, vary -workers and observe the difference.
//
//	# IO-bound: adding workers beyond NumCPU still helps
//	go run ./cmd/news-processor -mode=io -workers=1 -articles=8
//	go run ./cmd/news-processor -mode=io -workers=8 -articles=8
//	go run ./cmd/news-processor -mode=io -workers=16 -articles=8
//
//	# CPU-bound: workers beyond GOMAXPROCS add scheduling overhead only
//	go run ./cmd/news-processor -mode=cpu -workers=1 -articles=8
//	go run ./cmd/news-processor -mode=cpu -workers=4 -articles=8
//	go run ./cmd/news-processor -mode=cpu -workers=16 -articles=8
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-21-scheduler-internals/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-21-scheduler-internals/internal/simulator"
)

func main() {
	mode    := flag.String("mode", "io", "io or cpu")
	n       := flag.Int("articles", 8, "number of articles")
	w       := flag.Int("workers", runtime.NumCPU(), "worker goroutines")
	flag.Parse()

	info := pipeline.CaptureSchedulerInfo()
	fmt.Printf("Go Scheduler: GOMAXPROCS=%d NumCPU=%d\n",
		info.GOMAXPROCS, info.NumCPU)

	arts := pipeline.GenerateArticles(*n)

	fmt.Printf("Mode: %s | Articles: %d | Workers: %d\n", *mode, *n, *w)
	fmt.Println("─────────────────────────────────────────────────────────")

	var dur time.Duration

	switch *mode {
	case "io":
		pool := pipeline.NewIOBound(simulator.New(simulator.DefaultConfig), *w)
		results, d := pool.ProcessAll(context.Background(), arts)
		dur = d
		ok := 0
		for _, r := range results {
			if r.Err == nil { ok++ }
		}
		fmt.Printf("\nSucceeded: %d / %d\n", ok, len(results))

	case "cpu":
		pool := pipeline.NewCPUBound(*w, 50000) // 50k hash iterations per article
		results, d := pool.ProcessAll(arts)
		dur = d
		fmt.Printf("\nProcessed: %d articles (CPU hashing)\n", len(results))

	default:
		fmt.Fprintf(os.Stderr, "unknown mode %q — use io or cpu\n", *mode)
		os.Exit(1)
	}

	fmt.Printf("Duration : %v\n", dur.Round(time.Millisecond))
	fmt.Printf("Throughput: %.1f articles/sec\n",
		float64(*n)/dur.Seconds())
}
