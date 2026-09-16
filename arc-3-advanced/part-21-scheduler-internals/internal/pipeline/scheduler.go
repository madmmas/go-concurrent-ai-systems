// Package pipeline demonstrates Go scheduler behaviour for Part 21.
//
// The Go scheduler uses an M:P:G model:
//   M = OS thread (machine)
//   P = logical processor (controls M:G assignment)
//   G = goroutine
//
// GOMAXPROCS controls how many Ps run simultaneously.
// Default: runtime.NumCPU() — one P per CPU core.
//
// Key insight:
//   IO-bound workloads (LLM calls, HTTP, disk): goroutines block and yield
//   their P, so you benefit from more goroutines than CPUs.
//   CPU-bound workloads (hashing, parsing, embeddings): goroutines hold
//   their P until preempted, so throughput caps at GOMAXPROCS.
//
// This package benchmarks both to make the difference measurable.
package pipeline

import (
	"context"
	"crypto/sha256"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-21-scheduler-internals/internal/model"
	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-21-scheduler-internals/internal/simulator"
)

// SchedulerInfo captures runtime scheduler state at a point in time.
type SchedulerInfo struct {
	GOMAXPROCS   int
	NumCPU       int
	NumGoroutine int
}

// CaptureSchedulerInfo returns the current scheduler state.
func CaptureSchedulerInfo() SchedulerInfo {
	return SchedulerInfo{
		GOMAXPROCS:   runtime.GOMAXPROCS(0), // 0 = query without changing
		NumCPU:       runtime.NumCPU(),
		NumGoroutine: runtime.NumGoroutine(),
	}
}

// IOBoundPool processes articles with simulated LLM calls (IO-bound).
// Goroutines block on network calls and yield their P — many goroutines
// can run concurrently even with a low GOMAXPROCS.
type IOBoundPool struct {
	llm     *simulator.LLMClient
	Workers int
	Timeout time.Duration
}

// NewIOBound returns an IOBoundPool.
func NewIOBound(llm *simulator.LLMClient, workers int) *IOBoundPool {
	return &IOBoundPool{llm: llm, Workers: workers, Timeout: 5 * time.Second}
}

// ProcessAll runs IO-bound work (LLM calls) across the worker pool.
func (p *IOBoundPool) ProcessAll(ctx context.Context, articles []model.Article) ([]model.AIResult, time.Duration) {
	start := time.Now()
	jobs := make(chan model.Article, len(articles))
	results := make(chan model.AIResult, len(articles))

	var wg sync.WaitGroup
	for w := 0; w < p.Workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for article := range jobs {
				articleCtx, cancel := context.WithTimeout(ctx, p.Timeout)
				r := p.processIO(articleCtx, article)
				cancel()
				results <- r
			}
		}()
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
	return out, time.Since(start)
}

func (p *IOBoundPool) processIO(ctx context.Context, a model.Article) model.AIResult {
	r := model.AIResult{ArticleID: a.ID}
	if err := p.llm.Call(ctx, "Summarise", a.ID); err != nil {
		r.Err = err
		return r
	}
	r.Summary = "AI summary"
	return r
}

// CPUBoundPool processes articles with CPU-intensive work (hashing).
// Goroutines do not yield their P during computation — throughput is
// bounded by GOMAXPROCS, not goroutine count.
type CPUBoundPool struct {
	Workers    int
	Iterations int // hashing rounds per article
}

// NewCPUBound returns a CPUBoundPool.
func NewCPUBound(workers, iterations int) *CPUBoundPool {
	return &CPUBoundPool{Workers: workers, Iterations: iterations}
}

// ProcessAll runs CPU-bound work (repeated SHA-256 hashing) across the pool.
func (p *CPUBoundPool) ProcessAll(articles []model.Article) ([]model.AIResult, time.Duration) {
	start := time.Now()
	jobs := make(chan model.Article, len(articles))
	results := make(chan model.AIResult, len(articles))

	var wg sync.WaitGroup
	for w := 0; w < p.Workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for article := range jobs {
				results <- p.processCPU(article)
			}
		}()
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
	return out, time.Since(start)
}

// processCPU simulates CPU-intensive work: repeated SHA-256 hashing.
// This is a realistic proxy for embedding computation or tokenisation.
func (p *CPUBoundPool) processCPU(a model.Article) model.AIResult {
	data := []byte(a.Content + a.Title)
	hash := sha256.Sum256(data)
	for i := 1; i < p.Iterations; i++ {
		hash = sha256.Sum256(hash[:])
	}
	return model.AIResult{
		ArticleID: a.ID,
		Summary:   fmt.Sprintf("hash:%x", hash[:4]),
	}
}

// GenerateArticles produces n dummy articles.
func GenerateArticles(n int) []model.Article {
	articles := make([]model.Article, n)
	for i := range articles {
		articles[i] = model.Article{
			ID:      i + 1,
			Title:   fmt.Sprintf("Breaking News %d", i+1),
			Content: fmt.Sprintf("Article content for story number %d about AI and Go.", i+1),
			URL:     fmt.Sprintf("https://news.example.com/%d", i+1),
		}
	}
	return articles
}
