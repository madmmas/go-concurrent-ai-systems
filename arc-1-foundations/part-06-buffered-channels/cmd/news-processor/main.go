// Command news-processor demonstrates buffered vs unbuffered channels.
//
//	go run ./cmd/news-processor -mode=unbuffered -articles=10
//	go run ./cmd/news-processor -mode=buffered   -articles=10 -buffer=1
//	go run ./cmd/news-processor -mode=buffered   -articles=10 -buffer=10
//	go run ./cmd/news-processor -mode=drop       -articles=20 -buffer=2
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-1-foundations/part-06-buffered-channels/internal/model"
	"github.com/madmmas/go-concurrent-ai-systems/arc-1-foundations/part-06-buffered-channels/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-1-foundations/part-06-buffered-channels/internal/simulator"
)

func main() {
	n      := flag.Int("articles", 10, "number of articles")
	mode   := flag.String("mode", "buffered", "unbuffered, buffered, or drop")
	buffer := flag.Int("buffer", 10, "buffer size (buffered/drop mode only)")
	flag.Parse()

	if *n <= 0 {
		fmt.Fprintln(os.Stderr, "articles must be > 0")
		os.Exit(1)
	}

	llm := simulator.New(simulator.DefaultConfig)
	articles := pipeline.GenerateArticles(*n)

	var (
		results  []model.AIResult
		dropped  int
		duration time.Duration
	)

	switch *mode {
	case "unbuffered":
		fmt.Printf("Unbuffered pipeline — %d articles\n", *n)
		fmt.Println("Every worker send blocks until the collector receives.")
		fmt.Println("─────────────────────────────────────────────────────────")
		p := pipeline.NewUnbuffered(llm)
		results, duration = p.ProcessAll(articles)

	case "buffered":
		fmt.Printf("Buffered pipeline — %d articles, buffer=%d\n", *n, *buffer)
		fmt.Printf("Workers can queue up to %d results before blocking.\n", *buffer)
		fmt.Println("Collector uses select with a 30s deadline.")
		fmt.Println("─────────────────────────────────────────────────────────")
		p := pipeline.NewBuffered(llm, *buffer)
		results, duration = p.ProcessAll(articles)

	case "drop":
		fmt.Printf("Drop-on-full pipeline — %d articles, buffer=%d\n", *n, *buffer)
		fmt.Println("Workers drop results when the buffer is full (non-blocking select).")
		fmt.Println("─────────────────────────────────────────────────────────")
		p := pipeline.NewBuffered(llm, *buffer)
		results, dropped, duration = p.ProcessAllDropOnFull(articles)

	default:
		fmt.Fprintf(os.Stderr, "unknown mode: %s\n", *mode)
		os.Exit(1)
	}

	fmt.Println("\n═════════════════════════════════════════════════════════")
	fmt.Printf("Mode      : %s\n", *mode)
	fmt.Printf("Processed : %d articles\n", len(results))
	if *mode == "drop" {
		fmt.Printf("Dropped   : %d\n", dropped)
		fmt.Printf("Accounted : %d / %d\n", len(results)+dropped, *n)
	}
	fmt.Printf("Duration  : %v\n", duration.Round(time.Millisecond))
	fmt.Println("═════════════════════════════════════════════════════════")
}
