// Command news-processor — Part 25: slog + pprof.
//
//	# JSON logging (production format)
//	go run ./cmd/news-processor -articles=10 -log=json
//
//	# Text logging (development format, default)
//	go run ./cmd/news-processor -articles=10
//
//	# With pprof server — profile while running
//	go run ./cmd/news-processor -articles=50 -pprof=:6060
//	# Then in another terminal:
//	go tool pprof http://localhost:6060/debug/pprof/goroutine
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-25-slog-pprof/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-25-slog-pprof/internal/simulator"
)

func main() {
	n         := flag.Int("articles", 10, "number of articles")
	w         := flag.Int("workers", 3, "workers")
	logFmt    := flag.String("log", "text", "log format: text or json")
	pprofAddr := flag.String("pprof", "", "pprof listen address (e.g. :6060, empty=disabled)")
	flag.Parse()

	logger := pipeline.NewLogger(os.Stdout, *logFmt)
	slog.SetDefault(logger)

	if *pprofAddr != "" {
		pipeline.StartPprofServer(*pprofAddr)
	}

	pool := pipeline.New(simulator.New(simulator.DefaultConfig), *w, 5*time.Second, logger)
	calls, errors := pool.CounterRefs()
	flusher := pipeline.NewPeriodicFlusher(2*time.Second, calls, errors, logger)
	flusher.Start()
	defer flusher.Stop()

	arts := pipeline.GenerateArticles(*n)
	results, dur := pool.ProcessAll(context.Background(), arts)

	total, errs := pool.Counters()
	fmt.Printf("\nTotal calls: %d | Errors: %d | Duration: %v\n",
		total, errs, dur.Round(time.Millisecond))
	_ = results
}
