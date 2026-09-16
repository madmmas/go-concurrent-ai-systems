// Package benchmarks measures the Go scheduler's M:P:G model in practice.
//
// Two workload types with varying GOMAXPROCS and worker counts:
//
// IO-bound (LLM calls with simulated network latency):
//   Goroutines block and yield P → many goroutines per CPU is fine.
//   Expected: throughput increases beyond GOMAXPROCS workers.
//
// CPU-bound (SHA-256 hashing):
//   Goroutines hold P until preempted → throughput caps at GOMAXPROCS.
//   Expected: adding workers beyond GOMAXPROCS gives no benefit.
//
// Run and observe:
//
//	go test ./benchmarks/... -bench=. -benchmem -benchtime=2s -run='^$' -v
package benchmarks

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-21-scheduler-internals/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-21-scheduler-internals/internal/simulator"
)

var fastIO = simulator.Config{
	MinLatency: 5 * time.Millisecond,
	MaxLatency: 15 * time.Millisecond,
	Failure:    simulator.DefaultProfile,
}

// ─── IO-bound: worker count sweep ────────────────────────────────────────────
// As workers increase beyond NumCPU, throughput should keep rising
// because goroutines yield P during the simulated network wait.

func BenchmarkIOBound_W1(b *testing.B) {
	pool := pipeline.NewIOBound(simulator.New(fastIO), 1)
	arts := pipeline.GenerateArticles(8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.ProcessAll(context.Background(), arts)
	}
}

func BenchmarkIOBound_W4(b *testing.B) {
	pool := pipeline.NewIOBound(simulator.New(fastIO), 4)
	arts := pipeline.GenerateArticles(8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.ProcessAll(context.Background(), arts)
	}
}

func BenchmarkIOBound_WCPU(b *testing.B) {
	pool := pipeline.NewIOBound(simulator.New(fastIO), runtime.NumCPU())
	arts := pipeline.GenerateArticles(8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.ProcessAll(context.Background(), arts)
	}
}

func BenchmarkIOBound_W2xCPU(b *testing.B) {
	pool := pipeline.NewIOBound(simulator.New(fastIO), runtime.NumCPU()*2)
	arts := pipeline.GenerateArticles(8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.ProcessAll(context.Background(), arts)
	}
}

// ─── CPU-bound: GOMAXPROCS sweep ─────────────────────────────────────────────
// Throughput should scale linearly with GOMAXPROCS up to NumCPU,
// then plateau — adding more Ps does not give more CPU time.

func BenchmarkCPUBound_P1(b *testing.B) {
	prev := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(prev)
	pool := pipeline.NewCPUBound(4, 5000)
	arts := pipeline.GenerateArticles(8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.ProcessAll(arts)
	}
}

func BenchmarkCPUBound_P2(b *testing.B) {
	prev := runtime.GOMAXPROCS(2)
	defer runtime.GOMAXPROCS(prev)
	pool := pipeline.NewCPUBound(4, 5000)
	arts := pipeline.GenerateArticles(8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.ProcessAll(arts)
	}
}

func BenchmarkCPUBound_PCPU(b *testing.B) {
	ncpu := runtime.NumCPU()
	prev := runtime.GOMAXPROCS(ncpu)
	defer runtime.GOMAXPROCS(prev)
	pool := pipeline.NewCPUBound(ncpu, 5000)
	arts := pipeline.GenerateArticles(8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.ProcessAll(arts)
	}
}
