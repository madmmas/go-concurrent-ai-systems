// Package benchmarks measures the overhead of metrics collection.
//
// Core claim: observability should add negligible overhead to the pipeline.
// BenchmarkObservable_WithMetrics  — full ObservablePool (metrics on)
// BenchmarkObservable_MetricsRecord — Record() call in isolation (per-article cost)
//
// Record() uses atomic.AddInt64 for counters and sync.Mutex only for the
// latency slice append. Under concurrent load it should be cheap.
package benchmarks

import (
	"context"
	"testing"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-20-observability/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-20-observability/internal/simulator"
)

func BenchmarkObservable_Pipeline(b *testing.B) {
	pool := pipeline.New(simulator.New(simulator.FastConfig), 5, 500*time.Millisecond)
	arts := pipeline.GenerateArticles(10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.ProcessAll(context.Background(), arts)
	}
}

// BenchmarkMetrics_RecordConcurrent measures Record() throughput under
// concurrent access — the hot path during pipeline execution.
func BenchmarkMetrics_RecordConcurrent(b *testing.B) {
	m := pipeline.NewMetrics()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Record(5*time.Millisecond, nil)
		}
	})
}

func BenchmarkMetrics_Summary(b *testing.B) {
	m := pipeline.NewMetrics()
	// Pre-populate with 1000 data points
	for i := 0; i < 1000; i++ {
		m.Record(time.Duration(i)*time.Millisecond, nil)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Summary()
	}
}
