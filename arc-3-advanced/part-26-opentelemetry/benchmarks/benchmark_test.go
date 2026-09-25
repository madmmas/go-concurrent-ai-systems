// Package benchmarks measures the cost of tracing.
//
// A span per stage is only worth it if it is cheap next to the work it
// measures. The LLM calls it wraps take hundreds of milliseconds.
package benchmarks

import (
	"context"
	"io"
	"testing"

	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-26-opentelemetry/internal/pipeline"
)

// Span start + end, kept in memory.
func BenchmarkSpan_Recorder(b *testing.B) {
	recorder := &pipeline.SpanRecorder{}
	tracer := pipeline.NewTracer("bench", recorder)
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, span := tracer.Start(ctx, "embed", "article.id", i)
		span.End()
		if i%10000 == 0 {
			recorder.Clear() // keep memory flat
		}
	}
}

// Span start + end, serialised as JSON (stdout exporter to io.Discard).
func BenchmarkSpan_StdoutJSON(b *testing.B) {
	tracer := pipeline.NewTracer("bench", pipeline.NewStdoutExporter(io.Discard))
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, span := tracer.Start(ctx, "embed", "article.id", i)
		span.End()
	}
}

// Child span: parent lookup from ctx on top of the above.
func BenchmarkSpan_Child(b *testing.B) {
	recorder := &pipeline.SpanRecorder{}
	tracer := pipeline.NewTracer("bench", recorder)
	ctx, root := tracer.Start(context.Background(), "process-article")
	defer root.End()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, span := tracer.Start(ctx, "summarise", "article.id", i)
		span.End()
		if i%10000 == 0 {
			recorder.Clear()
		}
	}
}
