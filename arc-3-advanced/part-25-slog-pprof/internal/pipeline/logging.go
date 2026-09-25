// Package pipeline implements structured logging and diagnostics for Part 25.
//
// Arc 2 Part 20 added metrics. Part 25 adds:
//
//  1. slog (Go 1.21) — structured logging replacing fmt.Printf.
//     Every log line carries article_id, stage, latency_ms and worker_id
//     as key-value pairs that log aggregators can query and alert on.
//
//  2. time.Ticker — periodic metric flush every N seconds.
//     The same counters from Part 20, now emitted on a schedule
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
	"math"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-25-slog-pprof/internal/model"
	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-25-slog-pprof/internal/simulator"
)

// NewLogger returns an slog.Logger writing to w (or os.Stdout if w is nil).
// format="json" for production (log aggregators parse JSON).
// format="text" for development (human-readable logfmt).
// level controls the minimum level emitted; Debug lines cost almost
// nothing when the level is Info (see BenchmarkLog_DisabledDebug).
func NewLogger(w io.Writer, format string, level slog.Level) *slog.Logger {
	if w == nil {
		w = os.Stdout
	}
	opts := &slog.HandlerOptions{Level: level}
	var handler slog.Handler
	switch format {
	case "json":
		handler = slog.NewJSONHandler(w, opts)
	default:
		handler = slog.NewTextHandler(w, opts)
	}
	return slog.New(handler)
}

// StartPprofServer serves the pprof endpoints on addr and returns once the
// port is bound, so a bad address fails immediately rather than in a
// background goroutine after we have already logged "listening".
//
// Profiles:
//
//	http://localhost:6060/debug/pprof/goroutine   — goroutine stacks
//	http://localhost:6060/debug/pprof/heap        — heap allocations
//	http://localhost:6060/debug/pprof/profile     — 30s CPU profile
//
// The handlers are mounted on a dedicated mux, never http.DefaultServeMux.
// Note that importing net/http/pprof ALSO registers these handlers on
// DefaultServeMux as a side effect — so any other server in the process
// that serves DefaultServeMux exposes them too. Keep pprof on its own mux,
// bound to localhost (or an internal-only interface) in production.
func StartPprofServer(addr string, logger *slog.Logger) (*http.Server, error) {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index) // also serves /goroutine, /heap, ...
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("pprof listen %s: %w", addr, err)
	}
	// Addr is informational here (Serve uses ln); it reports the bound port.
	srv := &http.Server{Addr: ln.Addr().String(), Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			logger.Error("pprof server stopped", "error", err)
		}
	}()
	logger.Info("pprof server listening",
		"addr", ln.Addr().String(),
		"goroutine_profile", "http://"+ln.Addr().String()+"/debug/pprof/goroutine")
	return srv, nil
}

// ─── Periodic metric flush using time.Ticker ──────────────────────────────────

// PeriodicFlusher emits pipeline metrics on a regular schedule.
// In production: replace the slog call with Prometheus metrics emission
// or OpenTelemetry meter records.
type PeriodicFlusher struct {
	interval  time.Duration
	processed *int64
	failed    *int64
	logger    *slog.Logger

	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once
}

// NewPeriodicFlusher returns a flusher that logs metrics every interval.
func NewPeriodicFlusher(interval time.Duration, processed, failed *int64, logger *slog.Logger) *PeriodicFlusher {
	return &PeriodicFlusher{
		interval:  interval,
		processed: processed,
		failed:    failed,
		logger:    logger,
		stop:      make(chan struct{}),
		done:      make(chan struct{}),
	}
}

// Start begins periodic metric emission. Call Stop() to halt.
func (f *PeriodicFlusher) Start() {
	ticker := time.NewTicker(f.interval)
	go func() {
		defer close(f.done)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				f.flush(false)
			case <-f.stop:
				// Final flush: without it, everything since the last tick
				// is lost — for a short run, that can be everything.
				f.flush(true)
				return
			}
		}
	}()
}

// Stop halts the flusher, emits a final flush, and waits for the flusher
// goroutine to exit. Safe to call more than once.
func (f *PeriodicFlusher) Stop() {
	f.stopOnce.Do(func() { close(f.stop) })
	<-f.done
}

func (f *PeriodicFlusher) flush(final bool) {
	processed := atomic.LoadInt64(f.processed)
	failed := atomic.LoadInt64(f.failed)
	// LogAttrs with typed attrs: numbers stay numbers in the JSON output,
	// so the aggregator can run error_rate_pct > 5 without parsing strings.
	f.logger.LogAttrs(context.Background(), slog.LevelInfo, "pipeline metrics",
		slog.Int64("articles_processed", processed),
		slog.Int64("articles_failed", failed),
		slog.Float64("error_rate_pct", errorRate(processed, failed)),
		slog.Int("goroutines", runtime.NumGoroutine()),
		slog.Bool("final", final),
	)
}

func errorRate(processed, failed int64) float64 {
	if processed == 0 {
		return 0
	}
	return math.Round(float64(failed)/float64(processed)*1000) / 10 // one decimal
}

// ─── Observable pipeline with slog ────────────────────────────────────────────

// ObservablePool is the Arc 2 Part 20 pipeline upgraded with slog.
type ObservablePool struct {
	llm     *simulator.LLMClient
	Workers int
	Timeout time.Duration
	logger  *slog.Logger
	// Atomic counters — shared across workers
	processed int64 // articles finished (success or failure)
	failed    int64 // articles that failed
}

// New returns an ObservablePool with structured logging.
// The LLM client's own call logging is routed through the same logger.
func New(llm *simulator.LLMClient, workers int, timeout time.Duration, logger *slog.Logger) *ObservablePool {
	llm.WithLogger(logger)
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
		"workers", p.Workers,
	)

	jobs := make(chan model.Article, len(articles))
	results := make(chan model.AIResult, len(articles))

	var wg sync.WaitGroup
	for w := 0; w < p.Workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			// Child logger: worker_id is attached once, not repeated at
			// every call site. The handler pre-formats it.
			wlog := p.logger.With("worker_id", id)
			for article := range jobs {
				articleCtx, cancel := context.WithTimeout(ctx, p.Timeout)
				articleStart := time.Now()
				r := p.processArticle(articleCtx, article)
				cancel()

				dur := time.Since(articleStart)
				if r.Err != nil {
					atomic.AddInt64(&p.failed, 1)
					wlog.Warn("article failed",
						"article_id", article.ID,
						"latency_ms", dur.Milliseconds(),
						"error", r.Err,
					)
				} else {
					wlog.Info("article processed",
						"article_id", article.ID,
						"latency_ms", dur.Milliseconds(),
					)
				}
				atomic.AddInt64(&p.processed, 1)
				results <- r
			}
		}(w + 1)
	}

	for _, a := range articles {
		jobs <- a
	}
	close(jobs)
	go func() { wg.Wait(); close(results) }()

	var out []model.AIResult
	for r := range results {
		out = append(out, r)
	}

	dur := time.Since(start)
	p.logger.Info("pipeline complete",
		"duration_ms", dur.Milliseconds(),
		"articles", len(out),
		"failed", atomic.LoadInt64(&p.failed),
		"llm_calls", p.llm.TotalCalls(),
	)
	return out, dur
}

func (p *ObservablePool) processArticle(ctx context.Context, a model.Article) model.AIResult {
	r := model.AIResult{ArticleID: a.ID}
	if err := p.llm.Call(ctx, "summarise", a.ID); err != nil {
		r.Err = err
		return r
	}
	r.Summary = "AI summary"
	r.Sentiment = "Positive"
	return r
}

// Counters returns current processed and failed article counts.
func (p *ObservablePool) Counters() (processed, failed int64) {
	return atomic.LoadInt64(&p.processed), atomic.LoadInt64(&p.failed)
}

// CounterRefs returns pointers to the live counters for PeriodicFlusher.
func (p *ObservablePool) CounterRefs() (processed, failed *int64) {
	return &p.processed, &p.failed
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
