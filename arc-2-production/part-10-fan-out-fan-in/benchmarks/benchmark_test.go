package benchmarks

import (
	"context"
	"testing"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-10-fan-out-fan-in/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-10-fan-out-fan-in/internal/simulator"
)

func newBench() *pipeline.FanOutPool {
	cfg := simulator.Config{
		MinLatency: 5 * time.Millisecond,
		MaxLatency: 15 * time.Millisecond,
		Failure:    simulator.DefaultProfile,
	}
	return pipeline.New(simulator.New(cfg), 5, 2*time.Second)
}

func BenchmarkFanOut_10(b *testing.B) {
	pool := newBench()
	arts := pipeline.GenerateArticles(10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.ProcessAll(context.Background(), arts)
	}
}
