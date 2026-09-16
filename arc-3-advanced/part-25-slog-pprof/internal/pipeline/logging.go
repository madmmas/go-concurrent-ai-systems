// Package pipeline implements structured logging and diagnostics for Part 25.
//
// Arc 2 Part 20 added metrics. Part 25 adds:
//
//  1. slog (Go 1.21) — structured logging replacing fmt.Printf.
//     Every log line carries article_id, stage, latency, and worker_id
//     as key-value pairs that log aggregators can query and alert on.
//
//  2. time.Ticker — periodic metric flush every N seconds.
//     The same counter from Part 20, now emitted on a schedule
//     rather than only at pipeline end.
//
//  3. pprof HTTP server — live profiling endpoint.
//     go tool pprof http://localhost:6060/debug/pprof/goroutine
//     Shows every goroutine's stack trace — the production leak detector.
package pipeline

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	_ "net/http/pprof" // registers /debug/pprof handlers
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-25-slog-pprof/internal/model"
	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-25-slog-pprof/internal/simulator"
)

// NewLogger returns an slog.Logger writing to w (or os.Stdout if w is nil).
// format="json" for production (log aggregators parse JSON).
// format="text" for development (human-readable).
func NewLogger(w io.Writer, format string) *slog.Logger {
	if w == nil {
		w = os.Stdout
	}
	var handler slog.Handler
	switch format {
	case "json":
		handler = slog.NewJSONHandler(w, &slog.HandlerOptions{Level: slog.LevelDebug})
	default:
		handler = slog.NewTextHandler(w, &slog.HandlerOptions{Level: slog.LevelDebug})
	}
	return slog.New(handler)
}

// StartPprofServer starts the pprof HTTP server on addr in a background goroutine.
// Access profiles at:
//
//	http://localhost:6060/debug/pprof/goroutine   — goroutine stacks
//	http://localhost:6060/debug/pprof/heap        — heap allocations
//	http://localhost:6060/debug/pprof/profile     — 30s CPU profile
//
// In production, protect this endpoint — it exposes internal state.
func StartPprofServer(addr string) {
	go func() {
		if err := http.ListenAndServe(addr, nil); err != nil {
			slog.Error("pprof server failed", "error", err)
		}
	}()
	slog.Info("pprof server started", "addr", addr,
		"goroutine_profile", "http://"+addr+"/debug/pprof/goroutine")
}

// ─── Periodic metric flush using time.Ticker ──────────────────────────────────

// PeriodicFlusher emits pipeline metrics on a regular schedule.
// In production: replace slog.Info calls with Prometheus metrics emission
// or OpenTelemetry meter records.
type PeriodicFlusher struct {
	interval time.Duration
	calls    *int64
	errors   *int64
	stop     chan struct{}
	logger   *slog.Logger
}

// NewPeriodicFlusher returns a flusher that logs metrics every interval.
func NewPeriodicFlusher(interval time.Duration, calls, errors *int64, logger *slog.Logger) *PeriodicFlusher {
	return &PeriodicFlusher{
		interval: interval,
		calls:    calls,
		errors:   errors,
		stop:     make(chan struct{}),
		logger:   logger,
	}
}

// Start begins periodic metric emission. Call Stop() to halt.
func (f *PeriodicFlusher) Start() {
	ticker := time.NewTicker(f.interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				calls  := atomic.LoadInt64(f.calls)
				errors := atomic.LoadInt64(f.errors)
				f.logger.Info("pipeline metrics",
					"llm_calls",  calls,
					"llm_errors", errors,
					"error_rate", fmt.Sprintf("%.1f%%",
						errorRate(calls, errors)),
				)
			case <-f.stop:
				return
			}
		}
	}()
}

// Stop halts the flusher.
func (f *PeriodicFlusher) Stop() { close(f.stop) }

func errorRate(calls, errors int64) float64 {
	if calls == 0 { return 0 }
	return float64(errors) / float64(calls) * 100
}

// ─── Observable pipeline with slog ────────────────────────────────────────────

// ObservablePool is the Arc 2 Part 20 pipeline upgraded with slog.
type ObservablePool struct {
	llm     *simulator.LLMClient
	Workers int
	Timeout time.Duration
	logger  *slog.Logger
	// Atomic counters — shared across workers
	calls  int64
	errors int64
}

// New returns an ObservablePool with structured logging.
func New(llm *simulator.LLMClient, workers int, timeout time.Duration, logger *slog.Logger) *ObservablePool {
	return &ObservablePool{
		llm:     llm,
		Workers: workers,
		Timeout: timeout,
		logger:  logger,
	}
}

// ProcessAll processes articles with structured logging per article.
func (p *ObservablePool) ProcessAll(ctx context.Context, articles []model.Article) ([]model.AIResult, time.Duration) {
	start := time.Now()

	p.logger.Info("pipeline started",
		"articles", len(articles),
		"workers",  p.Workers,
	)

	jobs    := make(chan model.Article, len(articles))
	results := make(chan model.AIResult, len(articles))

	var wg sync.WaitGroup
	for w := 0; w < p.Workers; w++ {
		workerID := w + 1
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for article := range jobs {
				articleCtx, cancel := context.WithTimeout(ctx, p.Timeout)
				articleStart := time.Now()
				r := p.processArticle(articleCtx, article, id)
				cancel()

				dur := time.Since(articleStart)
				if r.Err != nil {
					atomic.AddInt64(&p.errors, 1)
					p.logger.Warn("article failed",
						"article_id", article.ID,
						"worker_id",  id,
						"latency_ms", dur.Milliseconds(),
						"error",      r.Err,
					)
				} else {
					p.logger.Debug("article processed",
						"article_id", article.ID,
						"worker_id",  id,
						"latency_ms", dur.Milliseconds(),
					)
				}
				atomic.AddInt64(&p.calls, 1)
				results <- r
			}
		}(workerID)
	}

	for _, a := range articles { jobs <- a }
	close(jobs)
	go func() { wg.Wait(); close(results) }()

	var out []model.AIResult
	for r := range results { out = append(out, r) }

	dur := time.Since(start)
	p.logger.Info("pipeline complete",
		"duration_ms", dur.Milliseconds(),
		"articles",    len(out),
		"errors",      atomic.LoadInt64(&p.errors),
	)
	return out, dur
}

func (p *ObservablePool) processArticle(ctx context.Context, a model.Article, workerID int) model.AIResult {
	r := model.AIResult{ArticleID: a.ID}
	if err := p.llm.Call(ctx, "Summarise", a.ID); err != nil {
		r.Err = err
		return r
	}
	r.Summary = "AI summary"
	r.Sentiment = "Positive"
	return r
}

// Counters returns current call and error counts.
func (p *ObservablePool) Counters() (calls, errors int64) {
	return atomic.LoadInt64(&p.calls), atomic.LoadInt64(&p.errors)
}

// CounterRefs returns pointers to the live counters for PeriodicFlusher.
func (p *ObservablePool) CounterRefs() (calls, errors *int64) {
	return &p.calls, &p.errors
}

// GenerateArticles produces n dummy articles.
func GenerateArticles(n int) []model.Article {
	articles := make([]model.Article, n)
	for i := range articles {
		articles[i] = model.Article{
			ID:    i + 1,
			Title: fmt.Sprintf("Breaking News %d", i+1),
			URL:   fmt.Sprintf("https://news.example.com/%d", i+1),
		}
	}
	return articles
}
