// Command news-processor — Part 26: OpenTelemetry-style traces.
//
// Records spans in-process — no backend required.
//
//	# Waterfall view of the batch trace (default)
//	go run ./cmd/news-processor -articles=3 -workers=2
//
//	# Stream every finished span as JSON (OTEL stdouttrace-style)
//	go run ./cmd/news-processor -articles=1 -export=json
//
//	# 20% LLM failures — failed stages show status=Error in the tree
//	go run ./cmd/news-processor -articles=6 -workers=3 -fail=0.2
//
// To send traces to Jaeger, swap in the real OTEL SDK with an OTLP exporter
// (see the package doc in internal/pipeline) and run:
//
//	docker run --rm -p 16686:16686 -p 4317:4317 jaegertracing/all-in-one
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-26-opentelemetry/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-26-opentelemetry/internal/simulator"
)

func main() {
	n := flag.Int("articles", 3, "number of articles")
	w := flag.Int("workers", 2, "workers")
	export := flag.String("export", "tree", "tree (waterfall at end) or json (one span per line as it ends)")
	fail := flag.Float64("fail", 0, "fraction of LLM calls that fail with 503")
	flag.Parse()

	cfg := simulator.DefaultConfig
	cfg.Failure = simulator.FailureProfile{ServerErrRate: *fail}
	cfg.Silent = true // the trace replaces per-call prints

	var extra []pipeline.Exporter
	if *export == "json" {
		extra = append(extra, pipeline.NewStdoutExporter(os.Stdout))
	}
	pool := pipeline.New(simulator.New(cfg), *w, 5*time.Second, extra...)

	results, dur := pool.ProcessAll(context.Background(), pipeline.GenerateArticles(*n))

	ok := 0
	for _, r := range results {
		if r.Err == nil {
			ok++
		}
	}
	spans := pool.Recorder().Spans()
	fmt.Printf("\nResults : %d processed, %d succeeded\n", len(results), ok)
	fmt.Printf("Spans   : %d  Duration: %v\n\n", len(spans), dur.Round(time.Millisecond))

	if *export == "tree" {
		pipeline.RenderTree(os.Stdout, spans, "article.id", "worker.id")
	}
}
