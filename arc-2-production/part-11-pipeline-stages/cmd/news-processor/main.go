// Command news-processor — Part 11: Multi-Stage Pipeline.
//
//	go run ./cmd/news-processor -articles=10
//	go run ./cmd/news-processor -articles=10 -scrape-workers=20 -llm-workers=3
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-11-pipeline-stages/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-11-pipeline-stages/internal/simulator"
)

func main() {
	n            := flag.Int("articles", 6, "number of articles")
	scrapeW      := flag.Int("scrape-workers", 10, "scrape stage workers")
	llmW         := flag.Int("llm-workers", 3, "embed and summarise stage workers")
	timeout      := flag.Duration("timeout", 5*time.Second, "per-stage timeout")
	flag.Parse()

	if *n <= 0 {
		fmt.Fprintln(os.Stderr, "articles must be > 0")
		os.Exit(1)
	}

	stages := []pipeline.StageConfig{
		{Name: "scrape",    Workers: *scrapeW},
		{Name: "clean",     Workers: *scrapeW / 2},
		{Name: "embed",     Workers: *llmW},
		{Name: "summarise", Workers: *llmW},
	}

	p    := pipeline.New(simulator.New(simulator.DefaultConfig), *timeout, stages)
	arts := pipeline.GenerateArticles(*n)

	fmt.Printf("Multi-stage pipeline: %d articles\n", *n)
	fmt.Printf("Stages: scrape(%d) → clean(%d) → embed(%d) → summarise(%d)\n",
		stages[0].Workers, stages[1].Workers, stages[2].Workers, stages[3].Workers)
	fmt.Println("─────────────────────────────────────────────────────────")

	results, dur := p.ProcessAll(context.Background(), arts)

	ok, fail := 0, 0
	for _, r := range results {
		if r.Err != nil { fail++ } else { ok++ }
	}

	fmt.Println("\n═════════════════════════════════════════════════════════")
	fmt.Printf("Processed : %d\n", len(results))
	fmt.Printf("Succeeded : %d | Failed: %d\n", ok, fail)
	fmt.Printf("Duration  : %v\n", dur.Round(time.Millisecond))
	fmt.Println("═════════════════════════════════════════════════════════")
}
