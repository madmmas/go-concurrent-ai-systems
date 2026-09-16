// Command news-processor — Part 26: OpenTelemetry-style traces.
//
// Records spans in-process and prints a summary — no backend required.
// To send traces to Jaeger with the real OTEL SDK:
//
//	docker run --rm -p 16686:16686 -p 4317:4317 jaegertracing/all-in-one
//	(then swap the in-process recorder for otlptracegrpc in production)
//
//	go run ./cmd/news-processor -articles=5 -workers=3
package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-26-opentelemetry/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-26-opentelemetry/internal/simulator"
)

func main() {
	n := flag.Int("articles", 5, "number of articles")
	w := flag.Int("workers", 3, "workers")
	flag.Parse()

	pool := pipeline.New(simulator.New(simulator.DefaultConfig), *w, 5*time.Second)
	arts := pipeline.GenerateArticles(*n)

	fmt.Printf("OpenTelemetry tracing: %d articles, %d workers\n", *n, *w)
	fmt.Println("Each article produces 3 spans: process-article → summarise + embed")
	fmt.Println("─────────────────────────────────────────────────────────")

	results, dur := pool.ProcessAll(context.Background(), arts)

	ok := 0
	for _, r := range results { if r.Err == nil { ok++ } }
	spans := pool.Recorder().Spans()

	fmt.Printf("\nResults : %d processed, %d succeeded\n", len(results), ok)
	fmt.Printf("Spans   : %d total (%d per article + 1 batch)\n",
		len(spans), (len(spans)-1)/len(arts))
	fmt.Printf("Duration: %v\n", dur.Round(time.Millisecond))
	fmt.Println("\nSpan summary:")
	for _, s := range spans {
		fmt.Printf("  %-20s %v\n", s.Name(), s.Duration().Round(time.Millisecond))
	}
}
