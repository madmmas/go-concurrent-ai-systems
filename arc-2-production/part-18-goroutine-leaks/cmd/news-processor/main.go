// Command news-processor — Part 18: Goroutine Leak Detection.
//
//	go run ./cmd/news-processor -articles=8 -workers=3
//	go run ./cmd/news-processor -mode=leaky -articles=8 -workers=3
//
// pprof (optional): -pprof=:6060 then
//
//	go tool pprof http://localhost:6060/debug/pprof/goroutine
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-18-goroutine-leaks/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-18-goroutine-leaks/internal/simulator"
)

func main() {
	n     := flag.Int("articles", 8, "number of articles")
	w     := flag.Int("workers", 3, "workers")
	mode  := flag.String("mode", "fixed", "fixed (no leak) or leaky")
	pprof := flag.String("pprof", "", "optional pprof listen addr, e.g. :6060")
	flag.Parse()

	if *pprof != "" {
		go func() {
			fmt.Printf("pprof listening on http://localhost%s/debug/pprof/\n", *pprof)
			_ = http.ListenAndServe(*pprof, nil)
		}()
	}

	llm := simulator.New(simulator.DefaultConfig)
	arts := pipeline.GenerateArticles(*n)

	before := runtime.NumGoroutine()
	fmt.Printf("Mode: %s | Goroutines before: %d\n", *mode, before)

	switch *mode {
	case "fixed":
		pool := pipeline.New(llm, *w, 5*time.Second)
		results, dur := pool.ProcessAll(context.Background(), arts)
		time.Sleep(100 * time.Millisecond)
		after := runtime.NumGoroutine()
		fmt.Printf("Goroutines after:  %d (delta: %+d)\n", after, after-before)
		fmt.Printf("Processed: %d articles in %v\n", len(results), dur.Round(time.Millisecond))
		if after <= before+2 {
			fmt.Println("✓ No goroutine leak detected")
		} else {
			fmt.Printf("✗ Possible leak: %d goroutines remain\n", after-before)
		}

	case "leaky":
		fmt.Println("Running LeakyPool — early cancel leaves senders blocked on unbuffered channel.")
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()
		// Drain only until cancel; abandon the rest → leak.
		_ = pipeline.NewLeaky(llm, *w).ProcessAllLeaky(ctx, arts)
		time.Sleep(200 * time.Millisecond)
		after := runtime.NumGoroutine()
		fmt.Printf("Goroutines after:  %d (delta: %+d)\n", after, after-before)
		if after > before+2 {
			fmt.Printf("✗ Leak demonstrated: %d extra goroutines\n", after-before)
		} else {
			fmt.Println("(leak may be timing-sensitive; try more articles or shorter cancel)")
		}

	default:
		fmt.Printf("unknown mode: %s\n", *mode)
	}

	if *pprof != "" {
		fmt.Println("pprof still listening — Ctrl+C to exit")
		select {}
	}
}
