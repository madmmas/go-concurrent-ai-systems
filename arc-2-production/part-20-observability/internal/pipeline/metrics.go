// Package pipeline implements structured observability for Part 20.
//
// Production pipelines need visibility into:
//   - Throughput: articles/sec processed
//   - Latency: p50/p95/p99 per stage
//   - Error rate: failures per stage
//   - Goroutine count: leak detection
//   - Queue depth: backpressure signal
//
// This implementation uses simple in-process counters and histograms.
// In production, emit these to Prometheus via promhttp or OpenTelemetry.
package pipeline

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-20-observability/internal/model"
	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-20-observability/internal/simulator"
)

// Metrics tracks pipeline statistics.
type Metrics struct {
	mu           sync.Mutex
	processed    int64
	failed       int64
	latencies    []float64 // milliseconds
	errorsByType map[string]int64
	startTime    time.Time
}

// NewMetrics creates a fresh Metrics collector.
func NewMetrics() *Metrics {
	return &Metrics{
		errorsByType: make(map[string]int64),
		startTime:    time.Now(),
	}
}

// Record records the outcome of one article.
func (m *Metrics) Record(dur time.Duration, err error) {
	if err != nil {
		atomic.AddInt64(&m.failed, 1)
		m.mu.Lock()
		m.errorsByType[err.Error()]++
		m.mu.Unlock()
	} else {
		atomic.AddInt64(&m.processed, 1)
	}
	m.mu.Lock()
	m.latencies = append(m.latencies, float64(dur.Milliseconds()))
	m.mu.Unlock()
}

// Summary returns a formatted metrics summary.
func (m *Metrics) Summary() string {
	m.mu.Lock()
	lats := make([]float64, len(m.latencies))
	copy(lats, m.latencies)
	errTypes := make(map[string]int64)
	for k, v := range m.errorsByType {
		errTypes[k] = v
	}
	m.mu.Unlock()

	total    := atomic.LoadInt64(&m.processed) + atomic.LoadInt64(&m.failed)
	elapsed  := time.Since(m.startTime).Seconds()
	throughput := float64(total) / elapsed

	sort.Float64s(lats)
	p50, p95, p99 := percentile(lats, 0.50), percentile(lats, 0.95), percentile(lats, 0.99)

	errRate := 0.0
	if total > 0 {
		errRate = float64(atomic.LoadInt64(&m.failed)) / float64(total) * 100
	}

	return fmt.Sprintf(
		"Processed: %d | Failed: %d (%.1f%%) | Throughput: %.1f/s\n"+
			"Latency p50: %.0fms | p95: %.0fms | p99: %.0fms\n"+
			"Errors: %v",
		atomic.LoadInt64(&m.processed),
		atomic.LoadInt64(&m.failed), errRate,
		throughput,
		p50, p95, p99,
		errTypes,
	)
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	if idx < 0 { idx = 0 }
	if idx >= len(sorted) { idx = len(sorted) - 1 }
	return sorted[idx]
}

// ObservablePool wraps a worker pool with Metrics collection.
type ObservablePool struct {
	llm     *simulator.LLMClient
	Workers int
	Timeout time.Duration
	Metrics *Metrics
}

// New returns an ObservablePool.
func New(llm *simulator.LLMClient, workers int, timeout time.Duration) *ObservablePool {
	if workers <= 0 {
		panic("ObservablePool: Workers must be > 0")
	}
	return &ObservablePool{
		llm: llm, Workers: workers, Timeout: timeout,
		Metrics: NewMetrics(),
	}
}

// ProcessAll processes articles and records metrics for each.
func (p *ObservablePool) ProcessAll(ctx context.Context, articles []model.Article) ([]model.AIResult, time.Duration) {
	start := time.Now()

	jobs      := make(chan model.Article, len(articles))
	resultsCh := make(chan model.AIResult, len(articles))

	var wg sync.WaitGroup
	for w := 0; w < p.Workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for article := range jobs {
				select {
				case <-ctx.Done():
					resultsCh <- model.AIResult{ArticleID: article.ID, Err: ctx.Err()}
				default:
					articleStart := time.Now()
					articleCtx, cancel := context.WithTimeout(ctx, p.Timeout)
					result := p.processArticle(articleCtx, article)
					cancel()
					// Record metrics
					p.Metrics.Record(time.Since(articleStart), result.Err)
					resultsCh <- result
				}
			}
		}()
	}

	for _, a := range articles {
		jobs <- a
	}
	close(jobs)

	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	var results []model.AIResult
	for r := range resultsCh {
		results = append(results, r)
	}
	return results, time.Since(start)
}

func (p *ObservablePool) processArticle(ctx context.Context, article model.Article) model.AIResult {
	result := model.AIResult{ArticleID: article.ID}
	if err := p.llm.Call(ctx, "Summarisation", article.ID); err != nil {
		result.Err = err
		return result
	}
	result.Summary = "AI-generated summary"
	result.Sentiment = "Positive"
	result.Keywords = []string{"AI", "Go", "Observability"}
	return result
}

// GenerateArticles produces n dummy articles.
func GenerateArticles(n int) []model.Article {
	articles := make([]model.Article, n)
	for i := range articles {
		articles[i] = model.Article{ID: i + 1, Title: fmt.Sprintf("Breaking News %d", i+1)}
	}
	return articles
}
