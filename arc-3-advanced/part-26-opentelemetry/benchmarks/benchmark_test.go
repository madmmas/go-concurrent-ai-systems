// Package benchmarks measures OpenTelemetry tracing overhead.
//
// Tracing should add minimal overhead to the pipeline.
// Compare: BenchmarkTraced vs the equivalent untraced pipeline from Part 20.
package benchmarks

import (
	"context"
	"testing"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-26-opentelemetry/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-26-opentelemetry/internal/simulator"
)

func BenchmarkTraced_Pipeline(b *testing.B) {
	pool := pipeline.New(simulator.New(simulator.FastConfig), 5, 2*time.Second)
	arts := pipeline.GenerateArticles(10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.Recorder().Clear()
		pool.ProcessAll(context.Background(), arts)
	}
}

func BenchmarkTraced_SpanCreation(b *testing.B) {
	recorder := &pipeline.SpanRecorder{}
	tracer := pipeline.NewTracer("bench", recorder)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, span := tracer.Start(ctx, "bench-span", "key", i)
		span.End()
	}
}
