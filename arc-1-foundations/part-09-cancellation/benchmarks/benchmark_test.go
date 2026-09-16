// Package benchmarks measures graceful shutdown cost and throughput.
//
// Two scenarios:
//
//  1. Clean completion — all articles finish normally.
//     Baseline for Part 9: cancellation overhead on the happy path.
//
//  2. Mid-flight cancellation — context cancelled after half the articles.
//     Measures the cost of the shutdown path: draining queued articles,
//     propagating cancellation, and building the ShutdownReport.
//
// Run:
//
//	go test ./benchmarks/... -bench=. -benchmem -benchtime=3s -run='^$'
package benchmarks

import (
	"context"
	"testing"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-1-foundations/part-09-cancellation/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-1-foundations/part-09-cancellation/internal/simulator"
)

func newPool() *pipeline.WorkerPool {
	cfg := simulator.Config{
		MinLatency: 5 * time.Millisecond,
		MaxLatency: 15 * time.Millisecond,
		Failure:    simulator.DefaultProfile,
	}
	return pipeline.New(simulator.New(cfg), 5, 2*time.Second)
}

// BenchmarkCleanCompletion — all articles finish, no cancellation.
// Measures the overhead of ShutdownReport tracking on the happy path.
func BenchmarkCleanCompletion(b *testing.B) {
	pool := newPool()
	arts := pipeline.GenerateArticles(10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, report := pool.ProcessAll(context.Background(), arts)
		if report.Succeeded != len(arts) {
			b.Fatalf("expected %d successes, got %d", len(arts), report.Succeeded)
		}
	}
}

// BenchmarkEarlyCancellation — context cancelled after 50ms.
// Measures the cost of the shutdown drain path and report assembly.
func BenchmarkEarlyCancellation(b *testing.B) {
	pool := newPool()
	arts := pipeline.GenerateArticles(20)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		_, report := pool.ProcessAll(ctx, arts)
		cancel()
		total := report.Succeeded + report.Failed + report.Cancelled + report.Queued
		if total != len(arts) {
			b.Fatalf("report total %d != article count %d", total, len(arts))
		}
	}
}
