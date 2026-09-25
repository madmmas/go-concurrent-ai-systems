// Command news-processor — Part 25: slog + Ticker + pprof.
//
//	# Text logging (development format, default)
//	go run ./cmd/news-processor -articles=10
//
//	# JSON logging (production format), with per-LLM-call debug lines
//	go run ./cmd/news-processor -articles=10 -log=json -level=debug
//
//	# With pprof server — profile while running
//	go run ./cmd/news-processor -articles=200 -workers=20 -pprof=localhost:6060
//	# Then in another terminal:
//	go tool pprof -top http://localhost:6060/debug/pprof/goroutine
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-25-slog-pprof/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-25-slog-pprof/internal/simulator"
)

func main() {
	n := flag.Int("articles", 10, "number of articles")
	w := flag.Int("workers", 3, "workers")
	logFmt := flag.String("log", "text", "log format: text or json")
	levelName := flag.String("level", "info", "log level: debug, info, warn, error")
	flushEvery := flag.Duration("flush", time.Second, "metric flush interval")
	pprofAddr := flag.String("pprof", "", "pprof listen address (e.g. localhost:6060, empty=disabled)")
	flag.Parse()

	var level slog.Level
	if err := level.UnmarshalText([]byte(*levelName)); err != nil {
		slog.Error("bad -level", "error", err)
		os.Exit(1)
	}
	logger := pipeline.NewLogger(os.Stdout, *logFmt, level)
	slog.SetDefault(logger)

	if *pprofAddr != "" {
		srv, err := pipeline.StartPprofServer(*pprofAddr, logger)
		if err != nil {
			logger.Error("pprof disabled", "error", err)
			os.Exit(1)
		}
		defer srv.Close()
	}

	pool := pipeline.New(simulator.New(simulator.DefaultConfig), *w, 5*time.Second, logger)
	processed, failed := pool.CounterRefs()
	flusher := pipeline.NewPeriodicFlusher(*flushEvery, processed, failed, logger)
	flusher.Start()

	pool.ProcessAll(context.Background(), pipeline.GenerateArticles(*n))
	flusher.Stop() // emits the final flush and waits for it
}
