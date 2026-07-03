// Command news-processor demonstrates buffered vs unbuffered channels.
//
//	go run ./cmd/news-processor -mode=unbuffered -articles=10
//	go run ./cmd/news-processor -mode=buffered   -articles=10 -buffer=1
//	go run ./cmd/news-processor -mode=buffered   -articles=10 -buffer=10
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-1-foundations/part-06-buffered-channels/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-1-foundations/part-06-buffered-channels/internal/simulator"
)

func main() {
	n      := flag.Int("articles", 10, "number of articles")
	mode   := flag.String("mode", "buffered", "unbuffered or buffered")
	buffer := flag.Int("buffer", 10, "buffer size (buffered mode only)")
	flag.Parse()

	if *n <= 0 {
		fmt.Fprintln(os.Stderr, "articles must be > 0")
		os.Exit(1)
	}

	llm := simulator.New(simulator.DefaultConfig)
	articles := pipeline.GenerateArticles(*n)

	var (
		results  []pipeline.AIResultExport
		duration time.Duration
	)

	switch *mode {
	case "unbuffered":
		fmt.Printf("Unbuffered pipeline — %d articles\n", *n)
		fmt.Println("Every worker send blocks until the collector receives.")
		fmt.Println("─────────────────────────────────────────────────────────")
		p := pipeline.NewUnbuffered(llm)
		r, d := p.ProcessAll(articles)
		for _, res := range r {
			results = append(results, pipeline.AIResultExport{ID: res.ArticleID})
		}
		duration = d

	case "buffered":
		fmt.Printf("Buffered pipeline — %d articles, buffer=%d\n", *n, *buffer)
		fmt.Printf("Workers can queue up to %d results before blocking.\n", *buffer)
		fmt.Println("─────────────────────────────────────────────────────────")
		p := pipeline.NewBuffered(llm, *buffer)
		r, d := p.ProcessAll(articles)
		for _, res := range r {
			results = append(results, pipeline.AIResultExport{ID: res.ArticleID})
		}
		duration = d

	default:
		fmt.Fprintf(os.Stderr, "unknown mode: %s\n", *mode)
		os.Exit(1)
	}

	fmt.Println("\n═════════════════════════════════════════════════════════")
	fmt.Printf("Mode      : %s\n", *mode)
	fmt.Printf("Processed : %d articles\n", len(results))
	fmt.Printf("Duration  : %v\n", duration.Round(time.Millisecond))
	fmt.Println("═════════════════════════════════════════════════════════")
}
