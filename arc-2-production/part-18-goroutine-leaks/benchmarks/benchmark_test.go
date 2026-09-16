// Package benchmarks verifies goroutine count stability over repeated runs.
//
// A leaky pool would show increasing b.N with rising goroutine count.
// FixedPool goroutine count must return to baseline after each iteration.
//
// BenchmarkFixed_GoroutineStability — run ProcessAll repeatedly and confirm
// goroutine count at the END equals goroutine count at the START.
package benchmarks

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-18-goroutine-leaks/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-18-goroutine-leaks/internal/simulator"
)

func BenchmarkFixed_Throughput(b *testing.B) {
	pool := pipeline.New(simulator.New(simulator.FastConfig), 5, 500*time.Millisecond)
	arts := pipeline.GenerateArticles(10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.ProcessAll(context.Background(), arts)
	}
}

// BenchmarkFixed_GoroutineStability is unusual: it reports goroutine
// delta as a custom metric rather than timing.
// A delta > 2 after b.N iterations means goroutines are leaking.
func BenchmarkFixed_GoroutineStability(b *testing.B) {
	pool := pipeline.New(simulator.New(simulator.FastConfig), 5, 500*time.Millisecond)
	arts := pipeline.GenerateArticles(10)

	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	before := runtime.NumGoroutine()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.ProcessAll(context.Background(), arts)
	}
	b.StopTimer()

	time.Sleep(50 * time.Millisecond)
	runtime.GC()
	after := runtime.NumGoroutine()

	delta := after - before
	b.ReportMetric(float64(delta), "goroutine_delta")
	if delta > 2 {
		b.Errorf("goroutine leak: started with %d, ended with %d (delta %d)",
			before, after, delta)
	}
}
