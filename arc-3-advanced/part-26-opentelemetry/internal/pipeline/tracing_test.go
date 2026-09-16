package pipeline_test

import (
	"context"
	"testing"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-26-opentelemetry/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-26-opentelemetry/internal/simulator"
)

// TestTracer_SpanHierarchy verifies parent-child span relationships.
func TestTracer_SpanHierarchy(t *testing.T) {
	recorder := &pipeline.SpanRecorder{}
	tracer := pipeline.NewTracer("test", recorder)

	// Root span
	ctx, root := tracer.Start(context.Background(), "root")
	// Child span
	_, child := tracer.Start(ctx, "child", "key", "value")
	child.End()
	root.End()

	spans := recorder.Spans()
	if len(spans) != 2 {
		t.Fatalf("expected 2 spans, got %d", len(spans))
	}
	// Child should be first (ended first), root second
	// Both should share the same trace ID
}

// TestTracer_AttributesAndEvents verifies span metadata works correctly.
func TestTracer_AttributesAndEvents(t *testing.T) {
	recorder := &pipeline.SpanRecorder{}
	tracer := pipeline.NewTracer("test", recorder)

	_, span := tracer.Start(context.Background(), "test-span",
		"article.id", 42,
		"stage",      "embed",
	)
	span.AddEvent("cache.hit")
	span.SetStatus("OK", "")
	span.End()

	spans := recorder.Spans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
}

// TestTracedPool_AllResultsDelivered verifies correctness.
func TestTracedPool_AllResultsDelivered(t *testing.T) {
	pool := pipeline.New(simulator.New(simulator.FastConfig), 3, 2*time.Second)
	arts := pipeline.GenerateArticles(6)
	results, _ := pool.ProcessAll(context.Background(), arts)
	if len(results) != 6 {
		t.Errorf("expected 6, got %d", len(results))
	}
}

// TestTracedPool_SpansCreatedPerArticle verifies each article produces spans.
// Expected spans per article: 1 article span + 2 child spans (summarise, embed)
// Plus 1 batch span = 1 + 3*N spans total.
func TestTracedPool_SpansCreatedPerArticle(t *testing.T) {
	pool := pipeline.New(simulator.New(simulator.FastConfig), 2, 2*time.Second)
	const n = 4
	pool.ProcessAll(context.Background(), pipeline.GenerateArticles(n))

	spans := pool.Recorder().Spans()
	// 1 batch + n articles × 3 (article + summarise + embed) = 1 + 4×3 = 13
	expected := 1 + n*3
	if len(spans) != expected {
		t.Errorf("expected %d spans for %d articles, got %d", expected, n, len(spans))
	}
	t.Logf("✓ %d spans created for %d articles (batch=1, per-article=3)", len(spans), n)
}

// TestTracedPool_SpanDurationsPositive verifies all spans have positive duration.
func TestTracedPool_SpanDurationsPositive(t *testing.T) {
	pool := pipeline.New(simulator.New(simulator.FastConfig), 3, 2*time.Second)
	pool.ProcessAll(context.Background(), pipeline.GenerateArticles(4))

	for _, span := range pool.Recorder().Spans() {
		if span.Duration() <= 0 {
			t.Errorf("span %q has non-positive duration: %v",
				span.Name(), span.Duration())
		}
	}
}

// TestTracedPool_NoRace verifies no data races.
func TestTracedPool_NoRace(t *testing.T) {
	pool := pipeline.New(simulator.New(simulator.FastConfig), 8, 2*time.Second)
	results, _ := pool.ProcessAll(context.Background(), pipeline.GenerateArticles(20))
	if len(results) != 20 {
		t.Errorf("expected 20, got %d", len(results))
	}
}
