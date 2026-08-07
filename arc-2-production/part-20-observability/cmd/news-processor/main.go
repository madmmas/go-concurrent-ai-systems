// Command news-processor — Part 20: Observability.
//
//	go run ./cmd/news-processor -articles=20 -workers=5
//	go run ./cmd/news-processor -articles=20 -workers=5 -error-rate=0.3
package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-20-observability/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-20-observability/internal/simulator"
)

func main() {
	n         := flag.Int("articles", 20, "number of articles")
	w         := flag.Int("workers", 5, "workers")
	errRate   := flag.Float64("error-rate", 0.0, "server error rate (0.0-1.0)")
	flag.Parse()

	cfg := simulator.Config{
		MinLatency: 100 * time.Millisecond,
		MaxLatency: 400 * time.Millisecond,
		Failure:    simulator.FailureProfile{ServerErrRate: *errRate},
	}

	pool := pipeline.New(simulator.New(cfg), *w, 3*time.Second)
	fmt.Printf("Observable pipeline: %d articles, %d workers, error-rate=%.0f%%\n",
		*n, *w, *errRate*100)

	pool.ProcessAll(context.Background(), pipeline.GenerateArticles(*n))

	fmt.Println("\n═════════════════ METRICS ════════════════════")
	fmt.Println(pool.Metrics.Summary())
	fmt.Println("══════════════════════════════════════════════")
}
