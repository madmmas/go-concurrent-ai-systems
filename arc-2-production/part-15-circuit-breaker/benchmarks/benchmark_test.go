// Package benchmarks shows the fail-fast benefit of the circuit breaker.
//
// BenchmarkCB_Closed  — all calls succeed, circuit stays closed
// BenchmarkCB_Open    — circuit opens after threshold; remaining calls
//                       rejected in microseconds (no network round trip)
//
// The open-circuit benchmark should be dramatically faster than closed
// because ErrCircuitOpen is returned without touching the simulator.
package benchmarks

import (
	"context"
	"testing"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-15-circuit-breaker/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-15-circuit-breaker/internal/simulator"
)

func BenchmarkCB_ClosedHealthy(b *testing.B) {
	cb := pipeline.NewCircuitBreaker(5, 10*time.Second)
	pool := pipeline.New(simulator.New(simulator.FastConfig), 5, 500*time.Millisecond, cb)
	arts := pipeline.GenerateArticles(10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.ProcessAll(context.Background(), arts)
	}
}

func BenchmarkCB_OpensAndFailsFast(b *testing.B) {
	// 100% error rate — circuit opens after threshold=2, then all
	// subsequent calls return ErrCircuitOpen in nanoseconds.
	cfg := simulator.Config{
		MinLatency: 5 * time.Millisecond,
		MaxLatency: 10 * time.Millisecond,
		Failure:    simulator.FailureProfile{ServerErrRate: 1.0},
	}
	arts := pipeline.GenerateArticles(20)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Fresh breaker each iteration so it trips at the same point
		cb := pipeline.NewCircuitBreaker(2, 10*time.Second)
		pool := pipeline.New(simulator.New(cfg), 3, 500*time.Millisecond, cb)
		pool.ProcessAll(context.Background(), arts)
	}
}
